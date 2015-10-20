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


inline int64_t sum_lanes_8(const __m256i & vector){
	int64_t total = 0;
	const int32_t *p = (int32_t*)&vector;
	for (int lane = 0; lane < 8; ++lane) {
		total += p[lane];
	}
	return total;
}


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
		l_extendedprice = allocate<int64_t>(len);
		l_discount = allocate<int32_t>(len); /*0.00 to 100.00*/
		l_tax = allocate<int32_t>(len); /*0.00 to 100.00*/
		l_returnflag = allocate<int32_t>(len); /* 2 values*/
		l_linestatus = allocate<int32_t>(len); /* 3 values*/
	}

	void printitem(size_t i) {
		printf("%d %ld %d %d %d %d %d\n",
				l_quantity[i], l_extendedprice[i], l_discount[i], l_tax[i], l_returnflag[i], l_linestatus[i], l_shipdate[i]);
	}

	word w1 {};
	size_t len {};
	int32_t *l_shipdate {}; //where
	int32_t *l_quantity {};
	int64_t *l_extendedprice {};
	int32_t *l_discount {};
	int32_t *l_tax {};
	int32_t *l_returnflag {};
	int32_t *l_linestatus {};
	word w2 {};
};


struct LineitemColumnarOpt {
	LineitemColumnarOpt() = default;

	LineitemColumnarOpt(size_t len) : len(len) {
		l_shipdate = allocate<uint16_t>(len);
		l_quantity = allocate<int32_t>(len);
		l_extendedprice = allocate<int64_t>(len);
		l_discount = allocate<uint16_t>(len); /*0.00 to 100.00*/
		l_tax = allocate<uint16_t>(len); /*0.00 to 100.00*/
		l_returnflag = allocate<uint8_t>(len); /* 2 values*/
		l_linestatus = allocate<uint8_t>(len); /* 3 values*/
	}

	void printitem(size_t i) {
		printf("%d %ld %d %d %d %d %d\n",
				l_quantity[i], l_extendedprice[i], l_discount[i], l_tax[i], l_returnflag[i], l_linestatus[i], l_shipdate[i]);
	}

	word w1 {};
	size_t len {};
	uint16_t *l_shipdate {}; //where
	int32_t *l_quantity {};
	int64_t *l_extendedprice {};
	uint16_t *l_discount {};
	uint16_t *l_tax {};
	uint8_t *l_returnflag {};
	uint8_t *l_linestatus {};
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

#define as_array(r) ((int32_t*)(&(r)))
static const size_t k_vecsize = 8;
const __m256i _minus1 = _mm256_set1_epi32(0xffffffff);
const __m256i _100s = _mm256_set1_epi32(100);
const __m256i _ones = _mm256_set1_epi32(1);

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


void tpch_q1_columnar_cond_avx256(const LineitemColumnar *l, q1result out, int cutoff)
{
	__m256i cutoffv = _mm256_set1_epi32(cutoff);

	for ( size_t i = 0; i < l->len; i+= k_vecsize) {
		auto datev = _mm256_load_si256((__m256i*)&l->l_shipdate[i]);
		auto compgt = _mm256_cmpgt_epi32(datev, cutoffv);
		auto mask = _mm256_xor_si256(compgt, _minus1);

		if (!_mm256_testz_si256(mask, mask)) { // may want to remove for high selectivity
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

			// go through each result and increment the correct counter or none
			for (size_t elt = 0; elt < k_vecsize; ++elt) {
				if (as_array(mask)[elt]) {
					auto &f = as_array(flagv)[elt];
					auto &s = as_array(statusv)[elt];
				    auto &op = out[f][s];

					op.count += 1;
					op.sum_qt += as_array(quantityv)[elt];
					op.sum_base_price += as_array(pricev)[elt];
					op.sum_disc_price += as_array(discounted_pricesX100)[elt];
					op.sum_charge += as_array(taxed_priceX10k)[elt];
				}
			}
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
		int sscanf_result  = sscanf(buf, "%d %ld %d %d %d %d %d\n",
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
	po::options_description desc("Allowed options");
	vector<uint32_t> selectivities  {90};
	vector<int> threadlevels {1,2};
	vector<string> input_variants {"plain"};

	desc.add_options()
		("help", "show this")
		("threadlevels", po::value<vector<int>>()->multitoken()->default_value(threadlevels, "1, 2"), "different numbers of threads to try")
	    ("items", po::value<int>()->default_value(1024), "items in lineitem")
	    ("reps", po::value<int>()->default_value(1), "number of repetitions")
		("selectivities", po::value<vector<uint32_t>>()->multitoken()->default_value(selectivities, "10, 90"), "from 1 to 100, percentage of tuples that qualify.")
		("sorted", po::value<bool>()->default_value(true), "0 to leave data in position, or 1 to sort workload by shipdate")
		("results", po::value<bool>()->default_value(false), "0 hides results for the last run of the first thread, 1 shows them")
		("variants", po::value<vector<string>>()->multitoken()->default_value(input_variants, "{plain}"), "variant to use")
		("file", po::value<string>(), "dont generate. instead load data from file in format: qty eprice discount tax rflag lstatus sdate. all are ints")
		("verbose", po::value<bool>()->default_value(false), "verbose")
		("lines", po::value<int>(), "upper bound on lines in file")
		("pause", po::value<bool>()->default_value(false), "pause before every method")
		("cutoff", po::value<int>()->default_value(17500), "cutoff for file loaded date");

	po::variables_map vm;
	po::store(po::parse_command_line(ac, av, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
	    cout << desc << "\n";
	    return 1;
	}

	g_verbose = vm["verbose"].as<bool>();

	int items = vm["items"].as<int>();
	int reps = vm["reps"].as<int>();
	selectivities = vm["selectivities"].as<vector<uint32_t>>();
	vm.erase("selectivities");
	threadlevels = vm["threadlevels"].as<vector<int>>();
	assert(threadlevels.size() > 0);
	std::sort(threadlevels.begin(), threadlevels.end());
	vm.erase("threadlevels");
	bool pause = vm["pause"].as<bool>();

	bool sorted = vm["sorted"].as<bool>();
	bool results = vm["results"].as<bool>();
	input_variants = vm["variants"].as<vector<string>>();
	vm.erase("variants");

	vector<TaskData> task_data(threadlevels.back());
	for (int i = 0; i < threadlevels.back(); ++i) {
			task_data[i].data = LineitemColumnar(items);
	}

	if (vm.count("file") > 0) {
		if (vm.count("lines") == 0) {
			printf("If file is given, then lines must be given\n");
			exit(1);
		} else {
			task_data[0].data = from_file(vm["file"].as<string>(), (size_t)vm["lines"].as<int>());
			if (task_data.size() > 1) {
				printf("Not supporing multi core for data from file yet\n");
				exit(1);
			}
		}
	} else {
		generateData(task_data, sorted);
	}

	BenchmarkOutput bo(vm);
	bool first = true;
	vector<string> variants {"plain"};

	for (auto &v : input_variants){
		if (v != "plain") {
			variants.push_back(v);
		}
	}

	q1group ref_answer[k_flags][k_status] {};

	for (auto &variant : variants){
		if (pause) {
			printf("about to start variant \"%s\". Press any key to continue...\n", variant.c_str());
			getchar();
		}
	auto f = dispatch_function(variant);
	for (auto selectivity : selectivities) {
		uint32_t cutoff;
		if (!vm.count("file")){
			 cutoff= ((selectivity * (1 << 12))/ 100); // warning: careful with small values.
		} else {
			cutoff = vm["cutoff"].as<int>();
			if (selectivities.size() > 1){
				printf("WARNING: when cutoff is given, selectivites should not\n");
				exit(1);
			}
		}
	for (auto threads : threadlevels) {

	vector<int> timing_info;
	vector<int> setup_timing;

	for (int repno = 0; repno < reps; ++repno) {

		auto task = [](TaskData *w, int cutoff, variant_t f) {
			q1group ans[k_flags][k_status] {}; //ensure outputs are read written in the stack. (assuming there is no false sharin then)
			f(&w->data, ans, cutoff);
			copy_groups(w->ans, ans);
		};

		auto before = clk::now();
		for (auto & w : task_data){ w.t = thread(task, &w, cutoff, f); }
		auto between = clk::now();
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

		int actual_selectivity =  selected_count * 100 / task_data[0].data.len;
		int actual_lines = task_data[0].data.len;

		ADD(bo, actual_lines);
		ADD(bo, actual_selectivity);
		ADD(bo, threads);
		ADD(bo, variant);
		timing_info.push_back(duration_millis(before, after));
		setup_timing.push_back(duration_millis(before, between));
	}

	for (int i = 0; i < reps; ++i) {
		bo.set_variable("duration_millis_rep" + std::to_string(i), timing_info[i]);
		bo.set_variable("setup_millis_rep" + std::to_string(i), setup_timing[i]);
	}

	if (first) { bo.display_param_names(); first = false; }
	bo.display_param_values();

	}
	}
	}
}
