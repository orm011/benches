#include <tuple>
#include <ostream>
#include "common.h"
#include "immintrin.h"
#include <error.h>
#include <stdio.h>

#ifdef CILKPUB
#include <cilkpub/dotmix.h>
#include <cilkpub/sort.h>
#endif

using std::ostream;

bool g_verbose = false;
static const int k_flags = 3;
static const int k_status = 2;

/*
 *
select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price. //l_discount is per row?
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, /l_tax is per row?
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
lineitem
where
l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3) -- delta in (60, 120)
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus;
*/
#define OUT(x) #x << ": " << x << " "

template <typename T> struct q1group_template {
	T count {};
	T sum_qt {};
	T sum_base_price {};
	T sum_disc_price {};
	T sum_charge {};

	friend ostream & operator<<(ostream &o, const q1group_template &q) {
		o << "{ "\
				<< OUT(q.count)\
				<< OUT(q.sum_qt) \
				<< OUT(q.sum_base_price) \
				<< OUT(q.sum_disc_price) \
				<< OUT(q.sum_charge)
		<< "}";
		return o;
	}

	q1group_template &operator=(const q1group_template &rhs){
		count = rhs.count;
		sum_qt = rhs.sum_qt;
		sum_base_price = rhs.sum_base_price;
		sum_disc_price = rhs.sum_disc_price;
		sum_charge = rhs.sum_charge;

		return *this;
	}

	friend bool operator==(const q1group_template &l, const q1group_template &r) {
			return l.count == r.count &&
					l.sum_qt == r.sum_qt &&
					l.sum_base_price == r.sum_base_price &&
					l.sum_disc_price == r.sum_disc_price &&
					l.sum_charge == r.sum_charge;
	}

	friend bool operator!=(const q1group_template &l, const q1group_template &r) {
		return ! (l == r);
	}
};

typedef q1group_template<int64_t> q1group;

struct group {
	int f;
	int s;
};

vector<group> k_groups = { {0,0}, {0,1}, {1,0}, {1,1}, {2,0}, {2,1} };
typedef q1group q1result[k_flags][k_status];

void adjust_sums(q1result r) {
	for (auto  &g : k_groups) {
		r[g.f][g.s].sum_disc_price /= 100;
		r[g.f][g.s].sum_charge /= 10000;
	}
}


/**
baseline: read the data once
 */
/** dimensions to vary:
 * -arrays already sorted by l_returnflag, l_linestatus or not.
 * -struct of arrays vs array of structs.
 * -intermediate hash table.
 */
short date_of(int yr, int month, int day){
	return (yr << 9) + (month << 5) + day ; // todo: fix. vary.;
	/* first 3 bits yr, then 4 bits month, then 5 bits day */
	/* 1/128 is 2^12/2^7 = 2^5 = 32. so pct. x% -> ((x * 32) / 100) << 7*/
}

void copy_groups(q1result dst, const q1result src) {
	for (int f = 0; f < k_flags; ++f) {
		for (int s = 0; s < k_status; ++s) {
			dst[f][s] = src[f][s];
		}
	}
}

struct LineitemColumnar {
	LineitemColumnar() = default;

	LineitemColumnar(size_t len) : len(len) {
		l_shipdate = allocate<int32_t>(len);
		l_quantity = allocate<int32_t>(len);
		l_extendedprice = allocate<int32_t>(len);
		l_discount = allocate<int32_t>(len); /*0.00 to 100.00*/
		l_tax = allocate<int32_t>(len); /*0.00 to 100.00*/
		l_returnflag = allocate<int32_t>(len); /* 2 values*/
		l_linestatus = allocate<int32_t>(len); /* 3 values*/
	}

	void printitem(size_t i) {
		printf("%d %d %d %d %d %d %d\n",
				l_quantity[i], l_extendedprice[i], l_discount[i], l_tax[i], l_returnflag[i], l_linestatus[i], l_shipdate[i]);
	}

	// 28 bytes for qualifying. 4 for non qualifying.

	word w1 {};
	size_t len {};
	int32_t *l_shipdate {}; //where
	int32_t *l_quantity {};
	int32_t *l_extendedprice {};
	int32_t *l_discount {};
	int32_t *l_tax {};
	int32_t *l_returnflag {};
	int32_t *l_linestatus {};
	word w2 {};
};


struct LineitemColumnarX {
	LineitemColumnarX() = default;

	LineitemColumnarX(size_t len) : len(len) {
		l_shipdate = allocate<int16_t>(len);
		l_quantity = allocate<int32_t>(len);
		l_extendedprice = allocate<int32_t>(len);
		l_discount = allocate<int16_t>(len); /*0.00 to 100.00*/
		l_tax = allocate<int16_t>(len); /*0.00 to 100.00*/
		l_returnflag = allocate<int8_t>(len); /* 2 values*/
		l_linestatus = allocate<int8_t>(len); /* 3 values*/
	}

	void printitem(size_t i) {
		printf("%d %d %d %d %d %d %d\n",
				l_quantity[i], l_extendedprice[i], l_discount[i], l_tax[i], l_returnflag[i], l_linestatus[i], l_shipdate[i]);
	}

	// 16 bytes for qualifying
	// 2 bytes for non qualifying.

	word w1 {};
	size_t len {};
	int8_t *l_returnflag {};
	int8_t *l_linestatus {};
	int16_t *l_shipdate {}; //where
	int16_t *l_discount {};
	int16_t *l_tax {};
	int32_t *l_quantity {};
	int32_t *l_extendedprice {};
	word w2 {};
};


const size_t k_unroll = 8;
void tpch_q1_baseline(const word *l, size_t len,  int64_t *out) {
	char sum[k_unroll] {};

	for (size_t i = 0; i < len; i += k_unroll ) {
		sum[0] ^= l[i+0]._pad[0];
		sum[1] ^= l[i+1]._pad[0];
		sum[2] ^= l[i+2]._pad[0];
		sum[3] ^= l[i+3]._pad[0];
		sum[4] ^= l[i+4]._pad[0];
		sum[5] ^= l[i+5]._pad[0];
		sum[6] ^= l[i+6]._pad[0];
		sum[7] ^= l[i+7]._pad[0];
	}

	/**
	 * op intensity:
	 * one comparison, one increment, 8 xor -> 10 intop / 8*64 bytes = 1 op/ 60 bytes
	 */
	for (size_t i = 0; i < k_unroll; ++i ) {
		*out ^= sum[i];
	}
}

__m256i _100s = _mm256_set1_epi32(100);

void merge_lanes(q1result dst, const q1group_template<__m256i> src[k_flags][k_status]) {
	for (int flag = 0; flag < k_flags; flag++) {
	  for (int status = 0; status < k_status; status++) {
	    auto & op = dst[flag][status];
    	op.sum_base_price = sum_lanes_8(src[flag][status].sum_base_price);
    	op.sum_qt = sum_lanes_8(src[flag][status].sum_qt);
    	op.sum_disc_price= sum_lanes_8(src[flag][status].sum_disc_price);
    	op.sum_charge = sum_lanes_8(src[flag][status].sum_charge);
    	op.count = sum_lanes_8(src[flag][status].count);
	  }
	}
}

void tpch_q1_columnar_double_masked_avx256(const LineitemColumnar *l, q1result out, int cutoff)
{
	q1group_template<__m256i> accs[k_flags][k_status] {};
	__m256i cutoffv = _mm256_set1_epi32(cutoff);

	for ( size_t i = 0; i < l->len; i+=k_vecsize ) {
		auto datev = _mm256_load_si256((__m256i*)&l->l_shipdate[i]);

		auto compgt = _mm256_cmpgt_epi32(datev, cutoffv);
		auto mask = _mm256_xor_si256(compgt, _minus1);

		auto flagv = _mm256_load_si256((__m256i*)&l->l_returnflag[i]);
		auto statusv = _mm256_load_si256((__m256i*)&l->l_linestatus[i]);
		auto quantityv = _mm256_load_si256((__m256i*)&l->l_quantity[i]);
		auto pricev = _mm256_load_si256((__m256i*)&l->l_extendedprice[i]);
		auto discountv = _mm256_load_si256((__m256i*)&l->l_discount[i]);
		auto taxv = _mm256_load_si256((__m256i*)&l->l_tax[i]);

		for (int f = 0; f < k_flags; ++f) {
			auto fv = _mm256_set1_epi32(f);
			auto eqf = _mm256_cmpeq_epi32(flagv, fv);

			for (int s = 0; s < k_status; ++s) {
				auto & op = accs[f][s];
				auto sv = _mm256_set1_epi32(s);
				auto eqs = _mm256_cmpeq_epi32(statusv, sv);

				auto botheqv = _mm256_and_si256(eqs, eqf);
				auto maskfsv = _mm256_and_si256(botheqv, mask);
				op.count =  _mm256_add_epi32(_mm256_and_si256(maskfsv, _ones), op.count);
				op.sum_qt = _mm256_add_epi32(_mm256_and_si256(maskfsv, quantityv), op.sum_qt);
				op.sum_base_price = _mm256_add_epi32(_mm256_and_si256(maskfsv, pricev), op.sum_base_price);

				auto actual_pcntX100 = _mm256_sub_epi32(_100s, discountv);
				auto discounted_pricesX100 = _mm256_mullo_epi32(pricev, actual_pcntX100);
				op.sum_disc_price = _mm256_add_epi32(_mm256_and_si256(maskfsv, discounted_pricesX100), op.sum_disc_price);

				auto tax_pcntX100 = _mm256_add_epi32(_100s, taxv);
				auto taxed_priceX10k = _mm256_mullo_epi32(discounted_pricesX100, tax_pcntX100);
				op.sum_charge = _mm256_add_epi32(_mm256_and_si256(maskfsv, taxed_priceX10k), op.sum_charge);
			}
		}

	}
	merge_lanes(out, accs);
	adjust_sums(out);
}

//_mm256_i32gather_epi32((const int*)$prev, $offsets, 4);
#define gather_incr($prev, $offsets, $mask, $delta, $val_out, $i)  \
					do {\
						auto currval = _mm256_load_si256(($prev) + ($i)*8);\
						auto gather_plcholder = _mm256_xor_si256(currval, _ones); \
						auto masked_delta = _mm256_and_si256(($delta), ($mask));\
						($val_out) = _mm256_add_epi32(gather_plcholder, masked_delta);\
					 } while (0)

void tpch_q1_columnar_cond_avx256(const LineitemColumnar *l, q1result out, int cutoff)
{
	__m256i count[k_flags*k_status]  {}; // (indexed by f * k_status + s) (f=0, s=0) then (f=0, s= 1);
	__m256i sum_qt[k_flags*k_status]  {};
	__m256i sum_base_price[k_flags*k_status]  {};
	__m256i sum_disc_price[k_flags*k_status]  {};
	__m256i sum_charge[k_flags*k_status]  {};

	__m256i cutoffv = _mm256_set1_epi32(cutoff);
	__m256i posv = _mm256_set_epi32(0,1,2,3,4,5,6,7);

	for ( size_t i = 0; i < l->len; i+= k_vecsize) {
		auto datev = _mm256_load_si256((__m256i*)&l->l_shipdate[i]);
		auto compgt = _mm256_cmpgt_epi32(datev, cutoffv);
		auto mask = _mm256_xor_si256(compgt, _minus1);

			auto flagv = _mm256_load_si256((__m256i*)&(l->l_returnflag[i]));
			auto statusv = _mm256_load_si256((__m256i*)&l->l_linestatus[i]);

			auto quantityv = _mm256_load_si256((__m256i*)&l->l_quantity[i]);
			auto pricev = _mm256_load_si256((__m256i*)&l->l_extendedprice[i]);
			auto discountv = _mm256_load_si256((__m256i*)&l->l_discount[i]);
			auto taxv = _mm256_load_si256((__m256i*)&l->l_tax[i]);

			auto actual_pcntX100 = _mm256_sub_epi32(_100s, discountv);
			auto discounted_pricesX100 = _mm256_mullo_epi32(pricev, actual_pcntX100);

			auto tax_pcntX100 = _mm256_add_epi32(_100s, taxv);
			auto taxed_priceX10k = _mm256_mullo_epi32(discounted_pricesX100, tax_pcntX100);

			// gather and add totals.
			// position in the totals array: base + (flags*k_status) + status + posv
			auto tmp1 = _mm256_slli_epi32(flagv, 1);
			auto tmp2 = _mm256_add_epi32(statusv, tmp1);
			auto tmp3 = _mm256_slli_epi32(tmp2, 3); // mult by 8.
			auto offsets = _mm256_add_epi32(tmp3, posv);

			__m256i new_count {};
			gather_incr(count, offsets, mask, _ones, new_count, (i>>3));

			__m256i new_sum_qnt {};
			gather_incr(sum_qt, offsets, mask, quantityv, new_sum_qnt, (i>>3));

			__m256i new_sum_base_price {};
			gather_incr(sum_base_price, offsets, mask, pricev, new_sum_base_price, (i>>3));

			__m256i new_sum_disc_price {};
			gather_incr(sum_disc_price, offsets, mask, discounted_pricesX100, new_sum_disc_price, (i>>3));

			__m256i new_sum_charge {};
			gather_incr(sum_charge, offsets, mask, taxed_priceX10k, new_sum_charge, (i>>3));

			// manual scatter.
			for (size_t elt = 0; elt < k_vecsize; elt+=1) {
				auto offset0 = as_array(offsets)[elt];
//				auto offset1 = as_array(offsets)[elt+1];
//				auto offset2 = as_array(offsets)[elt+2];
//				auto offset3 = as_array(offsets)[elt+3];

				as_array(count)[offset0] = as_array(new_count)[elt]; //scatter.
				as_array(sum_qt)[offset0] = as_array(new_sum_qnt)[elt]; //scatter.
				as_array(sum_base_price)[offset0] = as_array(new_sum_base_price)[elt]; //scatter.
				as_array(sum_disc_price)[offset0] = as_array(new_sum_disc_price)[elt]; //scatter.
				as_array(sum_charge)[offset0] = as_array(new_sum_charge)[elt]; //scatter.

//				as_array(count)[offset1] = as_array(new_count)[elt+1]; //scatter.
//				as_array(sum_qt)[offset1] = as_array(new_sum_qnt)[elt+1]; //scatter.
//				as_array(sum_base_price)[offset1] = as_array(new_sum_base_price)[elt+1]; //scatter.
//				as_array(sum_disc_price)[offset1] = as_array(new_sum_disc_price)[elt+1]; //scatter.
//				as_array(sum_charge)[offset1] = as_array(new_sum_charge)[elt+1]; //scatter.
//
//				as_array(count)[offset2] = as_array(new_count)[elt+2]; //scatter.
//				as_array(sum_qt)[offset2] = as_array(new_sum_qnt)[elt+2]; //scatter.
//				as_array(sum_base_price)[offset2] = as_array(new_sum_base_price)[elt+2]; //scatter.
//				as_array(sum_disc_price)[offset2] = as_array(new_sum_disc_price)[elt+2]; //scatter.
//				as_array(sum_charge)[offset2] = as_array(new_sum_charge)[elt+2]; //scatter.
//
//				as_array(count)[offset3] = as_array(new_count)[elt+3]; //scatter.
//				as_array(sum_qt)[offset3] = as_array(new_sum_qnt)[elt+3]; //scatter.
//				as_array(sum_base_price)[offset3] = as_array(new_sum_base_price)[elt+3]; //scatter.
//				as_array(sum_disc_price)[offset3] = as_array(new_sum_disc_price)[elt+3]; //scatter.
//				as_array(sum_charge)[offset3] = as_array(new_sum_charge)[elt+3]; //scatter.
			}
	}

	for (int f = 0; f < k_flags; ++f){
		for (int s = 0; s < k_status; ++s) {
			out[f][s].count = sum_lanes_8(count[f*k_status + s]);
			out[f][s].sum_qt = sum_lanes_8(sum_qt[f*k_status + s]);
			out[f][s].sum_base_price= sum_lanes_8(sum_base_price[f*k_status + s]);
			out[f][s].sum_disc_price = sum_lanes_8(sum_disc_price[f*k_status + s]);
			out[f][s].sum_charge = sum_lanes_8(sum_charge[f*k_status + s]);
		}
	}

	adjust_sums(out);
}

void tpch_q1_columnar_clustered_avx256(const LineitemColumnar *l, q1result out, int cutoff) {
	q1group_template<__m256i> accs[k_flags][k_status] {};
	__m256i cutoffv = _mm256_set1_epi32(cutoff);
	int32_t currentflag = 0;
	int32_t currentstatus = 0;

	for ( size_t i = 0; i < l->len; i += k_vecsize) {
	auto datev = _mm256_load_si256((__m256i*)&l->l_shipdate[i]);
	auto compgt = _mm256_cmpgt_epi32(datev, cutoffv);
	auto mask = _mm256_xor_si256(compgt, _minus1);

	if (!_mm256_testz_si256(mask, mask)) { // may want to remove for high selectivity
		currentflag = l->l_returnflag[i];
		currentstatus = l->l_linestatus[i];

		auto quantityv = _mm256_load_si256((__m256i*)&l->l_quantity[i]);
		auto pricev = _mm256_load_si256((__m256i*)&l->l_extendedprice[i]);
		auto discountv = _mm256_load_si256((__m256i*)&l->l_discount[i]);
		auto taxv = _mm256_load_si256((__m256i*)&l->l_tax[i]);

		auto actual_pcntX100 = _mm256_sub_epi32(_100s, discountv);
		auto discounted_pricesX100 = _mm256_mullo_epi32(pricev, actual_pcntX100);
		auto tax_pcntX100 = _mm256_add_epi32(_100s, taxv);
		auto taxed_priceX10k = _mm256_mullo_epi32(discounted_pricesX100, tax_pcntX100);

		auto &op = accs[currentflag][currentstatus];

		op.count = _mm256_add_epi32(op.count, _ones);
		op.sum_qt = _mm256_add_epi32(op.sum_qt, quantityv);
		op.sum_base_price = _mm256_add_epi32(op.sum_base_price, pricev);
		op.sum_disc_price = _mm256_add_epi32(op.sum_disc_price, discounted_pricesX100);
		op.sum_charge = _mm256_add_epi32(op.sum_charge, taxed_priceX10k);
	}
	}

	merge_lanes(out, accs);
	adjust_sums(out);
}

void tpch_q1_columnar_double_masked(const LineitemColumnar *l, q1result out, int cutoff)
{
	for ( size_t i = 0; i < l->len; i+=1 ) {
		auto mask = (l->l_shipdate[i] <= cutoff) ? 0xffffffff : 0;
		auto &flag = l->l_returnflag[i];
		auto &status = l->l_linestatus[i];

		for (int f = 0; f < k_flags; ++f) {
			for (int s = 0; s < k_status; ++s) {
				auto & op = out[f][s];
				int32_t maskfs = (mask && (flag == f) && (status == s)) ? 0xffffffff : 0;

				op.count += (maskfs & 1);
				op.sum_qt += (maskfs & l->l_quantity[i]);
				op.sum_base_price += (maskfs & l->l_extendedprice[i]);
				auto discounted = (maskfs & (l->l_extendedprice[i] * (100 - l->l_discount[i])));
				op.sum_disc_price += discounted;
				op.sum_charge  +=  discounted * (100 + l->l_tax[i]);
			}
		}
	}

	adjust_sums(out);
}


void tpch_q1_columnar_condstore_direct(const LineitemColumnar *l, q1result out, int cutoff)
{
	for ( size_t i = 0; i < l->len; i+=1 ) {
		auto &flag = l->l_returnflag[i];
		auto &status = l->l_linestatus[i];
	    auto & op = out[flag][status];

		auto mask = (l->l_shipdate[i] <= cutoff);

		int32_t count = mask ? 1 : 0;
		op.count  += count;

		int32_t quant = mask ? l->l_quantity[i] : 0;
		op.sum_qt  += quant;

		int32_t basepr = mask ? l->l_extendedprice[i] : 0;
		op.sum_base_price  += basepr;

		int discounted = mask ? (l->l_extendedprice[i] * (100 - l->l_discount[i])) : 0;
		op.sum_disc_price += discounted;

		int taxed = mask ? (discounted * (100 + l->l_tax[i])) : 0;
		op.sum_charge  += taxed;
	}

	adjust_sums(out);
}


void tpch_q1_columnar_masked_direct(const LineitemColumnar *l, q1result out, int cutoff)
{

	for ( size_t i = 0; i < l->len; i+=1 ) {
		auto &flag = l->l_returnflag[i];
		auto &status = l->l_linestatus[i];
	    auto & op = out[flag][status];

		auto mask = (l->l_shipdate[i] <= cutoff) ? 0xffffffff : 0;
		op.count += mask & 1;
		op.sum_qt += mask & l->l_quantity[i];
		op.sum_base_price += mask & l->l_extendedprice[i];
		int discounted = (l->l_extendedprice[i] * (100 - l->l_discount[i]));
		op.sum_disc_price += mask & discounted;
		int taxed = (discounted * (100 + l->l_tax[i]));
		op.sum_charge += mask & taxed;
	}

	adjust_sums(out);
}

void tpch_q1_columnar_plain(const LineitemColumnar *l, q1result out, int cutoff)
{
	for ( size_t i = 0; i < l->len; i+=1 ) {
		auto &flag = l->l_returnflag[i];
		auto &status = l->l_linestatus[i];

		if (l->l_shipdate[i] <= cutoff) {
		    auto & op = out[flag][status];
		    op.count += 1;
		    op.sum_qt += l->l_quantity[i];
		    op.sum_base_price += l->l_extendedprice[i];

		    long int discounted = (l->l_extendedprice[i] * (100 - l->l_discount[i]));;
		    op.sum_disc_price += discounted;
		    long int taxed = (discounted * (100 + l->l_tax[i]));
		    op.sum_charge += taxed;
		}
	}

	adjust_sums(out);
}


struct TaskData {
	LineitemColumnar data {};
	q1group ans[k_flags][k_status] {};
	thread t {};
	char pad[64] {};
};

/**
 * run hyper on same environment.
 * use provided work generator
 * check on sse flags (avx has no int stuff, so try avx2 to see if compiler is ok).
 * expand fields. see effect on bandwidth.
 * open ec2 account.
 * use icc.
 * check on clang + llvm (re vectorization).
 * get reliable lanka baseline.
 *
 */

/**
 * not for NUMA.
 */
void generateData (const vector<TaskData>  &l, bool sorted) {
#ifdef CILKPUB
	cilkpub::pedigree_scope scope = cilkpub::pedigree_scope::current();
	cilkpub::DotMix c(0xabc);
	c.init_scope(scope);

	int len = l[0].data.len;

	_Cilk_for (size_t vec = 0; vec < l.size(); ++vec) {
		_Cilk_for (int item = 0; item < len; ++item) {
			int s = c.get();
			l[vec].data.l_quantity[item] = MarsagliaXOR(&s) % 100;
			l[vec].data.l_extendedprice[item] = MarsagliaXOR(&s) % 100 + 100;
			l[vec].data.l_discount[item] = MarsagliaXOR(&s) % 40 + 1;
			l[vec].data.l_tax[item] = MarsagliaXOR(&s) % 10 + 1;
			l[vec].data.l_returnflag[item] = MarsagliaXOR(&s) % 3;
			l[vec].data.l_linestatus[item] = MarsagliaXOR(&s) % 2;
			l[vec].data.l_shipdate[item] =  MarsagliaXOR(&s) % (1 << 12);
		}

	  if (sorted) {
		  cilkpub::cilk_sort_in_place(l[vec].data.l_shipdate, l[vec].data.l_shipdate + l[vec].data.len);
	  }
	}
#else
	assert(false);
#endif
}

typedef void (*variant_t)(const LineitemColumnar *, q1group (*)[2], int);
#define impl(s) {#s, tpch_q1_columnar_##s}
map<string, variant_t> g_variants
{
	impl(plain),
	impl(masked_direct),
	impl(double_masked),
	impl(condstore_direct),
	impl(double_masked_avx256),
	impl(cond_avx256),
	impl(clustered_avx256)
};

variant_t dispatch_function(string variant) {
	if (g_variants.count(variant) > 0){
		return g_variants.at(variant);
	} else {
		printf("variant %s not recognized. valid variants:\n", variant.c_str());
		for (auto &p : g_variants){
			printf("%s, ", p.first.c_str());
		}
		printf("\n");
		exit(1);
	}
}

LineitemColumnarX from_fileX(string filename, size_t lines) {
	auto *f = fopen(filename.c_str(), "r");
	if (!f) {
		error(1, errno, "failed at fopen");
	}

	LineitemColumnarX ans(lines);
	size_t pos = 0;

	size_t buf_size = 100;
	auto buf = (char*)malloc(buf_size);
	long int readline_result = 0;

	while ((readline_result = getline(&buf, &buf_size, f)) > 0 && pos < lines) {

		buf[readline_result] = '\0'; // null terminate for sscanf
		int sscanf_result  = sscanf(buf, "%d %d %hd %hd %hhd %hhd %hd\n",
				&ans.l_quantity[pos],
				&ans.l_extendedprice[pos],
				&ans.l_discount[pos],
				&ans.l_tax[pos],
				&ans.l_returnflag[pos],
				&ans.l_linestatus[pos],
				&ans.l_shipdate[pos]);
		if (sscanf_result < 7 || sscanf_result == EOF) {
			auto errnum = errno;
			error(1, errnum, "failed at sscanf.");
		}

		++pos;
	}

	free(buf);

	if (pos >= lines) {
		printf("Ran out of space for file contents!\n");
		exit(1);
	} else {
		auto errnum = errno;
		if (errnum) { error(1, errnum, "failed at getline."); }
	}

	if (g_verbose) {
		for (size_t i = 0; i < lines && i < 10; ++i){
			ans.printitem(i); // debug.
		}
	}

	ans.len = pos; //set it to actual length.
	return ans;
}

LineitemColumnar from_file(string filename, size_t lines) {
	auto *f = fopen(filename.c_str(), "r");
	if (!f) {
		error(1, errno, "failed at fopen");
	}

	LineitemColumnar ans(lines);
	size_t pos = 0;

	size_t buf_size = 100;
	auto buf = (char*)malloc(buf_size);
	long int readline_result = 0;

	while ((readline_result = getline(&buf, &buf_size, f)) > 0 && pos < lines) {

		buf[readline_result] = '\0'; // null terminate for sscanf
		int sscanf_result  = sscanf(buf, "%d %d %d %d %d %d %d\n",
				&ans.l_quantity[pos],
				&ans.l_extendedprice[pos],
				&ans.l_discount[pos],
				&ans.l_tax[pos],
				&ans.l_returnflag[pos],
				&ans.l_linestatus[pos],
				&ans.l_shipdate[pos]);
		if (sscanf_result < 7 || sscanf_result == EOF) {
			auto errnum = errno;
			error(1, errnum, "failed at sscanf.");
		}

		++pos;
	}

	free(buf);

	if (pos >= lines) {
		printf("Ran out of space for file contents!\n");
		exit(1);
	} else {
		auto errnum = errno;
		if (errnum) { error(1, errnum, "failed at getline."); }
	}

	if (g_verbose) {
		for (size_t i = 0; i < lines && i < 10; ++i){
			ans.printitem(i); // debug.
		}
	}

	ans.len = pos; //set it to actual length.
	return ans;
}


int main(int ac, char** av){
	vector<string> variants {};
	string file;
	if (getenv("FILE")) {
		file = string(getenv("FILE"));
	} else {
		printf("need a file\n");
		exit(1);
	}

	if (getenv("VARIANT")){
		variants.push_back(getenv("VARIANT"));
	} else {
		variants.push_back("plain");
	}

	int cutoff = 18500; // 100%
	bool pause = true;
	if (getenv("NOPAUSE")){
		pause = false;
	}
	bool results = false;
	if (getenv("RESULTS")){
		results = true;
	}

	int lines = 59986053;
	int reps = 10;
	if (getenv("REPS")){
		reps = 100;
	}

	vector<TaskData> task_data(1);
	task_data[0].data = from_file(file, lines);

	bool first = true;

	q1group ref_answer[k_flags][k_status] {};

	while (true) {

	for (auto &variant : variants){
		if (pause) {
			printf("about to start variant \"%s\". Press any key to continue...\n", variant.c_str());
			getchar();
		}
	auto f = dispatch_function(variant);

	vector<int> timing_info;

	map<string, string> params;
	int actual_selectivity  = 0;
	for (int repno = 0; repno < reps; ++repno) {

		auto task = [](TaskData *w, int cutoff, variant_t f) {
			q1group ans[k_flags][k_status] {}; //ensure outputs are read written in the stack. (assuming there is no false sharin then)
			f(&w->data, ans, cutoff);
			copy_groups(w->ans, ans);
		};

		auto before = clk::now();
		for (auto & w : task_data){ w.t = thread(task, &w, cutoff, f); }
		for (auto & w : task_data) { w.t.join(); }
		auto after = clk::now();

		size_t selected_count = 0;
		for (int i = 0; i < k_flags; ++i) {
			for (int j = 0; j < k_status; ++j) {
				bool error_found = false;
				selected_count += task_data[0].ans[i][j].count;
				if (task_data[0].ans[i][j].sum_base_price < task_data[0].ans[i][j].sum_disc_price) {
					cerr << "WARNING: base price less than discount prices" << endl;
					error_found = true;
				}

				if (task_data[0].ans[i][j].sum_disc_price > task_data[0].ans[i][j].sum_charge) {
					cerr << "WARNING: discount price larger than after-tax price" << endl;
					error_found = true;
				}

				if (first) {
					ref_answer[i][j] = task_data[0].ans[i][j];
					continue;
				}

				if (ref_answer[i][j] != task_data[0].ans[i][j]) {
					cerr << "WARNING: output mismatch with plain plan." << endl;
					error_found = true;
				}

				if (error_found) {
					cerr << "linestatus: " << j << " returnflag: " << i << endl;
					cerr << "Expected: " << endl << ref_answer[i][j] << endl;
					cerr << "Actual: " << endl << task_data[0].ans[i][j] << endl;
				}
			}
		}

		if (results) {
			for (int i = 0; i < k_flags; ++i) {
				for (int j = 0; j < k_status; ++j) {
					cerr << "l_linestatus: " << j << " l_returnflag: " << i << " " << task_data[0].ans[i][j] << endl;
				}
			}
		}

		actual_selectivity =  selected_count * 100 / task_data[0].data.len;
		timing_info.push_back(duration_millis(before, after));
	}

	printf("%s %d ", variant.c_str(), actual_selectivity);
	for (int i = 0; i < reps; ++i){
		printf("%d ", timing_info[i]);
	}
	printf("\n");
	first = false;
	}
	}
	}
