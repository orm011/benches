#include <tuple>
#include <cilkpub/dotmix.h>
#include <cilkpub/sort.h>
#include <ostream>
#include "common.h"
#include "immintrin.h"

using std::ostream;


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


struct q1out {
	int64_t count {};
	int64_t sum_qt {};
	int64_t sum_base_price {};
	int64_t sum_disc_price {};
	int64_t sum_charge {};

	friend ostream & operator<<(ostream &o, const q1out &q) {
		o << "{ "\
				<< OUT(q.count)\
				<< OUT(q.sum_qt) \
				<< OUT(q.sum_base_price) \
				<< OUT(q.sum_disc_price) \
				<< OUT(q.sum_charge)
		<< "}";
		return o;
	}

	q1out &operator=(const q1out &rhs){
		count = rhs.count;
		sum_qt = rhs.sum_qt;
		sum_base_price = rhs.sum_base_price;
		sum_disc_price = rhs.sum_disc_price;
		sum_charge = rhs.sum_charge;

		return *this;
	}

	friend bool operator==(const q1out &l, const q1out &r) {
			return l.count == r.count &&
					l.sum_qt == r.sum_qt &&
					l.sum_base_price == r.sum_base_price &&
					l.sum_disc_price == r.sum_disc_price &&
					l.sum_charge == r.sum_charge;
	}

	friend bool operator!=(const q1out &l, const q1out &r) {
		return ! (l == r);
	}
};


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

static const int k_flags = 3;
static const int k_status = 2;

void copy_groups(q1out dst[][k_status], const q1out src[][k_status]) {
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

inline int32_t sum_lanes(const __m128i & vector){
	int32_t total = 0;
	const int32_t *p = (int32_t*)&vector;
	for (int lane = 0; lane < 4; ++lane) {
		total += p[lane];
	}

	return total;
}

void tpch_q1_columnar_double_masked_avx128(const LineitemColumnar *l, q1out out[k_flags][k_status], int cutoff)
{

	__m128i acc_counts[k_flags][k_status] {};
	__m128i acc_quantity[k_flags][k_status] {};
	__m128i acc_baseprice[k_flags][k_status] {};
	__m128i acc_discounted[k_flags][k_status] {};
	__m128i acc_disctax[k_flags][k_status] {};

	__m128i cutoffv = _mm_set1_epi32(cutoff);

	__m128i _minus1 = _mm_set1_epi32(0xffffffff);
	__m128i _1s = _mm_set1_epi32(0x1);
	__m128i _100s = _mm_set1_epi32(100);

	for ( size_t i = 0; i < l->len; i+=4 ) {
		auto datev = _mm_load_si128((__m128i*)&l->l_shipdate[i]);

		auto compgt = _mm_cmpgt_epi32(datev, cutoffv);
		auto mask = _mm_xor_si128(compgt, _minus1);

		auto flagv = _mm_load_si128((__m128i*)&l->l_returnflag[i]);
		auto statusv = _mm_load_si128((__m128i*)&l->l_linestatus[i]);
		auto quantityv = _mm_load_si128((__m128i*)&l->l_quantity[i]);
		auto pricev = _mm_load_si128((__m128i*)&l->l_extendedprice[i]);
		auto discountv = _mm_load_si128((__m128i*)&l->l_discount[i]);
		auto taxv = _mm_load_si128((__m128i*)&l->l_tax[i]);

		for (int f = 0; f < k_flags; ++f) {
			auto fv = _mm_set1_epi32(f);
			auto eqf = _mm_cmpeq_epi32(flagv, fv);

			for (int s = 0; s < k_status; ++s) {
				auto sv = _mm_set1_epi32(s);
				auto eqs = _mm_cmpeq_epi32(statusv, sv);

				auto botheqv = _mm_and_si128(eqs, eqf);
				auto maskfsv = _mm_and_si128(botheqv, mask);
				acc_counts[f][s] =  _mm_add_epi32(_mm_and_si128(maskfsv, _1s), acc_counts[f][s]);
				acc_quantity[f][s] = _mm_add_epi32(_mm_and_si128(maskfsv, quantityv), acc_quantity[f][s]);
				acc_baseprice[f][s] = _mm_add_epi32(_mm_and_si128(maskfsv, pricev), acc_baseprice[f][s]);

				auto actual_pcntX100 = _mm_sub_epi32(_100s, discountv);
				auto discounted_pricesX100 = _mm_mullo_epi32(pricev, actual_pcntX100);
				acc_discounted[f][s] = _mm_add_epi32(_mm_and_si128(maskfsv, discounted_pricesX100), acc_discounted[f][s]);

				auto tax_pcntX100 = _mm_add_epi32(_100s, taxv);
				auto taxed_priceX10k = _mm_mullo_epi32(discounted_pricesX100, tax_pcntX100);
				acc_disctax[f][s] = _mm_add_epi32(_mm_and_si128(maskfsv, taxed_priceX10k), acc_disctax[f][s]);
			}
		}
	}

	for (int flag = 0; flag < k_flags; flag++) {
	  for (int status = 0; status < k_status; status++) {
	    auto & op = out[flag][status];

    	op.sum_base_price = sum_lanes(acc_baseprice[flag][status]);
    	op.sum_qt = sum_lanes(acc_quantity[flag][status]);
    	op.sum_disc_price= sum_lanes(acc_discounted[flag][status])/100;
    	op.sum_charge = sum_lanes(acc_disctax[flag][status])/10000;
    	op.count = sum_lanes(acc_counts[flag][status]);
	  }
	}
}


#define as_array(r) ((int32_t*)(&(r)))

static const size_t k_vecsize = 8;
void tpch_q1_columnar_cond_avx256(const LineitemColumnar *l, q1out out[k_flags][k_status], int cutoff)
{
	__m256i cutoffv = _mm256_set1_epi32(cutoff);
	__m256i _minus1 = _mm256_set1_epi32(0xffffffff);
	__m256i _100s = _mm256_set1_epi32(100);

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

	for (int f = 0; f < k_flags; ++f) {
		for (int s = 0; s < k_status; ++s) {
			out[f][s].sum_disc_price /= 100;
			out[f][s].sum_charge /= 100;
		}
	}
}


void tpch_q1_columnar_double_masked(const LineitemColumnar *l, q1out out[k_flags][k_status], int cutoff)
{

	int32_t acc_counts[k_flags][k_status] {};
	int32_t acc_quantity[k_flags][k_status] {};

	int32_t maskfs {};
	int64_t acc_baseprice[k_flags][k_status] {};
	int64_t acc_discounted[k_flags][k_status] {};
	int64_t acc_disctax[k_flags][k_status] {};

	for ( size_t i = 0; i < l->len; i+=1 ) {
		auto mask = (l->l_shipdate[i] <= cutoff) ? 0xffffffff : 0;
		auto &flag = l->l_returnflag[i];
		auto &status = l->l_linestatus[i];

		for (int f = 0; f < k_flags; ++f) {
			for (int s = 0; s < k_status; ++s) {
				maskfs = (mask && (flag == f) && (status == s)) ? 0xffffffff : 0;
				acc_counts[f][s] += (maskfs & 1);
				acc_quantity[f][s] += (maskfs & l->l_quantity[i]);
				acc_baseprice[f][s] += (maskfs & l->l_extendedprice[i]);
				auto discounted = (maskfs & (l->l_extendedprice[i] * (100 - l->l_discount[i])));
				acc_discounted[flag][status] += discounted;
				acc_disctax[flag][status]  +=  discounted * (100 + l->l_tax[i]);
			}
		}
	}

	for (int flag = 0; flag < k_flags; flag++) {
	  for (int status = 0; status < k_status; status++) {
	    auto & op = out[flag][status];
	    op.sum_base_price = acc_baseprice[flag][status];
	    op.sum_qt = acc_quantity[flag][status];
	    op.sum_disc_price= acc_discounted[flag][status]/100;
	    op.sum_charge = acc_disctax[flag][status]/10000;
	    op.count = acc_counts[flag][status];

	  }
	}
}




void tpch_q1_columnar_condstore_direct(const LineitemColumnar *l, q1out out[k_flags][k_status], int cutoff)
{

	int32_t acc_counts[k_flags][k_status] {};
	int32_t acc_quantity[k_flags][k_status] {};

	int64_t acc_baseprice[k_flags][k_status] {};
	int64_t acc_discounted[k_flags][k_status] {};
	int64_t acc_disctax[k_flags][k_status] {};

	for ( size_t i = 0; i < l->len; i+=1 ) {
		auto &flag = l->l_returnflag[i];
		auto &status = l->l_linestatus[i];

		auto mask = (l->l_shipdate[i] <= cutoff);

		int32_t count = mask ? 1 : 0;
		acc_counts[flag][status] += count;

		int32_t quant = mask ? l->l_quantity[i] : 0;
		acc_quantity[flag][status] += quant;

		int32_t basepr = mask ? l->l_extendedprice[i] : 0;
		acc_baseprice[flag][status] += basepr;

		int discounted = mask ? (l->l_extendedprice[i] * (100 - l->l_discount[i])) : 0;
		acc_discounted[flag][status] += discounted;

		int taxed = mask ? (discounted * (100 + l->l_tax[i])) : 0;
		acc_disctax[flag][status] += taxed;
	}


	for (int flag = 0; flag < k_flags; flag++) {
	  for (int status = 0; status < k_status; status++) {
	    auto & op = out[flag][status];
	    op.sum_base_price = acc_baseprice[flag][status];
	    op.sum_qt = acc_quantity[flag][status];
	    op.sum_disc_price= acc_discounted[flag][status]/100;
	    op.sum_charge = acc_disctax[flag][status]/10000;
	    op.count = acc_counts[flag][status];

	  }
	}
}


void tpch_q1_columnar_masked_direct(const LineitemColumnar *l, q1out out[k_flags][k_status], int cutoff)
{

	int32_t acc_counts[k_flags][k_status] {};
	int32_t acc_quantity[k_flags][k_status] {};

	int64_t acc_baseprice[k_flags][k_status] {};
	int64_t acc_discounted[k_flags][k_status] {};
	int64_t acc_disctax[k_flags][k_status] {};

	for ( size_t i = 0; i < l->len; i+=1 ) {
		auto &flag = l->l_returnflag[i];
		auto &status = l->l_linestatus[i];

		auto mask = (l->l_shipdate[i] <= cutoff) ? 0xffffffff : 0;

		acc_counts[flag][status] += mask & 1;
		acc_quantity[flag][status] += mask & l->l_quantity[i];
		acc_baseprice[flag][status] += mask & l->l_extendedprice[i];

		int discounted = (l->l_extendedprice[i] * (100 - l->l_discount[i]));
		acc_discounted[flag][status] += mask & discounted;
		int taxed = (discounted * (100 + l->l_tax[i]));
		acc_disctax[flag][status] += mask & taxed;
	}


	for (int flag = 0; flag < k_flags; flag++) {
	  for (int status = 0; status < k_status; status++) {
	    auto & op = out[flag][status];
	    op.sum_base_price = acc_baseprice[flag][status];
	    op.sum_qt = acc_quantity[flag][status];
	    op.sum_disc_price= acc_discounted[flag][status]/100;
	    op.sum_charge = acc_disctax[flag][status]/10000;
	    op.count = acc_counts[flag][status];
	  }
	}
}



void tpch_q1_columnar_plain(const LineitemColumnar *l, q1out out[k_flags][k_status], int cutoff)
{

	int32_t acc_counts[k_flags][k_status] {};
	int32_t acc_quantity[k_flags][k_status] {};

	int64_t acc_baseprice[k_flags][k_status] {};
	int64_t acc_discounted[k_flags][k_status] {};
	int64_t acc_disctax[k_flags][k_status] {};

	for ( size_t i = 0; i < l->len; i+=1 ) {
		auto &flag = l->l_returnflag[i];
		auto &status = l->l_linestatus[i];

		if (l->l_shipdate[i] <= cutoff) {
				acc_counts[flag][status] += 1;
				acc_quantity[flag][status] += l->l_quantity[i];
				acc_baseprice[flag][status] += l->l_extendedprice[i];

				int discounted = (l->l_extendedprice[i] * (100 - l->l_discount[i]));
				acc_discounted[flag][status] += discounted;
				int taxed = (discounted * (100 + l->l_tax[i]));
				acc_disctax[flag][status] += taxed;
		}
	}


	for (int flag = 0; flag < k_flags; flag++) {
	  for (int status = 0; status < k_status; status++) {
	    auto & op = out[flag][status];
	    op.sum_base_price = acc_baseprice[flag][status];
	    op.sum_qt = acc_quantity[flag][status];
	    op.sum_disc_price= acc_discounted[flag][status]/100;
	    op.sum_charge = acc_disctax[flag][status]/10000;
	    op.count = acc_counts[flag][status];

	  }
	}
}

struct TaskData {
	LineitemColumnar data {};
	q1out ans[k_flags][k_status] {};
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
	cilkpub::pedigree_scope scope = cilkpub::pedigree_scope::current();
	cilkpub::DotMix c(0xabc);
	c.init_scope(scope);

	int len = l[0].data.len;

	_Cilk_for (size_t vec = 0; vec < l.size(); ++vec) {
		_Cilk_for (int item = 0; item < len; ++item) {
			int s = c.get();
			l[vec].data.l_quantity[item] = MarsagliaXOR(&s) % 100;
			l[vec].data.l_extendedprice[item] = MarsagliaXOR(&s) % 1000 + 100;
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
}

void runBench(TaskData *w, int reps, uint16_t cutoff, string variant) {
	void (*f)(const LineitemColumnar *, q1out (*)[2], int) = nullptr;

	if (variant == "plain"){
		f = tpch_q1_columnar_plain;
	} else if (variant == "masked_direct") {
		f = tpch_q1_columnar_masked_direct;
	} else if (variant == "double_masked") {
		f = tpch_q1_columnar_double_masked;
	} else if (variant == "condstore_direct") {
		f = tpch_q1_columnar_condstore_direct;
	} else if (variant == "double_masked_avx128"){
		f = tpch_q1_columnar_double_masked_avx128;
	} else {
		assert(0);
		return;
	}

	for (int i = 0; i < reps; ++i) {
		memset(&w->ans, 0, sizeof(w->ans));
		(*f)(&w->data, w->ans, cutoff);
	}
}

int main(int ac, char** av){
	po::options_description desc("Allowed options");
	vector<int> selectivities  {90};
	vector<int> threadlevels {1,2};
	vector<string> input_variants {"plain"};

	desc.add_options()
		("help", "show this")
		("threadlevels", po::value<vector<int>>()->multitoken()->default_value(threadlevels, "1, 2"), "different numbers of threads to try")
	    ("items", po::value<int>()->default_value(1024), "items in lineitem")
	    ("reps", po::value<int>()->default_value(1), "number of repetitions")
		("selectivities", po::value<vector<int>>()->multitoken()->default_value(selectivities, "10, 90"), "from 1 to 100, percentage of tuples that qualify.")
		("sorted", po::value<bool>()->default_value(true), "0 to leave data in position, or 1 to sort workload by shipdate")
		("results", po::value<bool>()->default_value(false), "0 hides results for the last run of the first thread, 1 shows them")
		("variants", po::value<vector<string>>()->multitoken()->default_value(input_variants, "{plain}"), "variant to use");

	po::variables_map vm;
	po::store(po::parse_command_line(ac, av, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
	    cout << desc << "\n";
	    return 1;
	}

	int items = vm["items"].as<int>();
	int reps = vm["reps"].as<int>();
	selectivities = vm["selectivities"].as<vector<int>>();
	vm.erase("selectivities");
	threadlevels = vm["threadlevels"].as<vector<int>>();
	assert(threadlevels.size() > 0);
	std::sort(threadlevels.begin(), threadlevels.end());
	vm.erase("threadlevels");

	bool sorted = vm["sorted"].as<bool>();
	bool results = vm["results"].as<bool>();
	input_variants = vm["variants"].as<vector<string>>();
	vm.erase("variants");

	vector<TaskData> task_data(threadlevels.back());
	for (int i = 0; i < threadlevels.back(); ++i) {
			task_data[i].data = LineitemColumnar(items);
	}

	generateData(task_data, sorted);

	BenchmarkOutput bo(vm);
	bool first = true;
	vector<string> variants {"plain"};

	for (auto &v : input_variants){
		if (v != "plain") {
			variants.push_back(v);
		}
	}

	q1out ref_answer[k_flags][k_status] {};

	for (auto &variant : variants){
	for (auto selectivity : selectivities) {
	int cutoff = ((selectivity * (1 << 12))/ 100); // warning: careful with small values.
	for (auto threads : threadlevels) {
		auto before = clk::now();
		for (auto & w : task_data){ w.t = thread(runBench, &w, reps, cutoff, variant); }
		auto between = clk::now();
		for (auto & w : task_data) { w.t.join(); }
		auto after = clk::now();

		if (first) {
			for (int i = 0; i < k_flags; ++i) {
				for (int j = 0 ;  j < k_status; ++j) {
					ref_answer[i][j] = task_data[0].ans[i][j];
				}
			}
		} else {

			for (int i = 0; i < k_flags; ++i) {
				for (int j = 0; j < k_status; ++j) {
					if (ref_answer[i][j] != task_data[0].ans[i][j]){
						cerr << "WARNING: output mismatch with plain implementation found at l_linestatus: "
								<< j << " l_returnflag: " << i << endl;
						cerr << "Expected: " << endl << ref_answer[i][j] << endl;
						cerr << "Actual: " << endl << task_data[0].ans[i][j] << endl;
					}
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

		ADD(bo, selectivity);
		ADD(bo, threads);
		ADD(bo, variant);

		auto time_millis = duration_millis(before, after);
		ADD(bo, time_millis);

		auto setup_millis = duration_millis(before, between);
		ADD(bo, setup_millis);

		if (first) { bo.display_param_names(); }
		bo.display_param_values();
		first = false;
	}
	}
	}
}
