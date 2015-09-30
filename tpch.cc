#include <tuple>
#include <cstdlib>
#include <cassert>
#include <cilkpub/dotmix.h>
#include <iostream>
#include <ostream>
#include <string.h>

using std::ostream;

int MarsagliaXOR(int *p_seed) {
    int seed = *p_seed;

    if (seed == 0) {
        seed = 1;
    }

    seed ^= seed << 6;
    seed ^= ((unsigned)seed) >> 21;
    seed ^= seed << 7;

    *p_seed = seed;

    return seed & 0x7FFFFFFF;
}


struct Lineitem {
	int l_orderkey; // too narrow?
	int l_partkey;
	int l_suppkey;
	int l_linenumber;
	int l_quantity;
	int l_extendedprice;
	char l_discount;
	char l_tax;
	char l_returnflag;
	char l_linestatus;
	int l_shipdate;
	int l_commitdate;
	int l_receiptdate;
	int l_shipinstruct;
	int l_shipmode; // too wide?
	int l_comment; // too narrow?

	auto toTuple() const {
		auto x =  std::tie(l_orderkey,
					l_partkey,
					l_suppkey,
					l_linenumber,
					l_quantity,
					l_extendedprice,
					l_discount,
					l_tax,
					l_returnflag,
					l_linestatus,
					l_shipdate,
					l_commitdate,
					l_receiptdate,
					l_shipinstruct,
					l_shipmode,
					l_comment);
		return x;
	}

	bool operator==(const Lineitem &other) const {
		return (this->toTuple() == other.toTuple());
	}
};

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
	int64_t  sum_qt;
	int64_t sum_base_price;
	int64_t sum_disc_price;
	int64_t sum_charge;
	int64_t avg_qty;
	int64_t avg_price;
	int64_t avg_disc;
	int64_t count;

	friend ostream & operator<<(ostream &o, const q1out &q) {
		o << "{ "\
				<< OUT(q.sum_qt) \
				<< OUT(q.sum_base_price) \
				<< OUT(q.sum_disc_price) \
				<< OUT(q.sum_charge) \
				<< OUT(q.avg_qty) \
				<< OUT(q.avg_price) \
				<< OUT(q.avg_disc) \
				<< OUT(q.count)
		<< "}";

		return o;
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
int date_of(int yr, int month, int day){
	return yr; // todo: fix. vary.;
}

static const int k_flags = 3;
static const int k_status = 2;

struct LineitemColumnar {

	LineitemColumnar(size_t len) : len(len) {
		l_quantity = new int[len];
		l_shipdate = new int[len];
		l_extendedprice = new int[len];
		l_discount = new char[len]; /*0 to 100.*/
		l_tax = new char[len]; /*0 to 100*/
		l_returnflag = new char[len]; /* 2 values*/
		l_linestatus = new char[len]; /* 3 values*/
	}

	size_t len;
	int *l_quantity;
	int *l_extendedprice;
	char *l_discount;
	char *l_tax;
	char *l_returnflag;
	char *l_linestatus;
	int *l_shipdate; //where
};

void tpch_q1_columnar(const LineitemColumnar *l, q1out out[k_flags][k_status]){
	int64_t acc_counts[k_flags][k_status] {};
	int64_t acc_quantity[3][2] {};
	int64_t acc_baseprice[3][2] {};
	int64_t acc_discounted[3][2] {};
	int64_t acc_disctax[3][2] {};

	const int k_date = date_of(1998, 1, 1); // 10% selectivity.

	for (size_t i = 0; i < l->len; ++i) {
		if (l->l_shipdate[i] <= k_date) {
			auto &flag = l->l_returnflag[i];
			auto &status = l->l_linestatus[i];

			acc_counts[flag][status] += 1;
			acc_quantity[flag][status] += l->l_quantity[i];
			acc_baseprice[flag][status] += l->l_extendedprice[i];

			int discounted = (l->l_extendedprice[i] * (100 - l->l_discount[i]))/100;
			acc_discounted[flag][status] += discounted;

			int taxed = (discounted * (100 + l->l_tax[i]))/100;
			acc_disctax[flag][status] += taxed;
		}
	}


	for (int flag = 0; flag < k_flags; flag++) {
	  for (int status = 0; status < k_status; status++) {
	    auto & op = out[flag][status];
	    op.sum_base_price = acc_baseprice[flag][status];
	    op.sum_qt = acc_quantity[flag][status];
	    op.sum_disc_price= acc_discounted[flag][status];
	    op.sum_charge = acc_disctax[flag][status];
	    op.count = acc_counts[flag][status];
	    op.avg_disc = op.sum_disc_price/op.count;
	    op.avg_price = op.sum_base_price/op.count;
	    op.avg_qty = op.sum_qt/op.count;
	  }
	}
}

void generateItem (Lineitem *l, cilkpub::DotMix &c) {
  uint64_t v = c.get();
  int g_s = v;
  l->l_quantity = MarsagliaXOR(&g_s) % 100;
  l->l_extendedprice = MarsagliaXOR(&g_s) % 1000 + 100;
  l->l_discount = MarsagliaXOR(&g_s) % 40 + 1;
  l->l_tax = MarsagliaXOR(&g_s) % 10 + 1;
  l->l_returnflag = MarsagliaXOR(&g_s) % 3;
  l->l_linestatus = MarsagliaXOR(&g_s) % 2;
  l->l_shipdate = date_of(1990 + (MarsagliaXOR(&g_s) % 10), 0, 0);
}

void generateItem (LineitemColumnar *l, size_t i, cilkpub::DotMix &c){
	  uint64_t v = c.get();
	  int g_s = v;
	  l->l_quantity[i] = MarsagliaXOR(&g_s) % 100;
	  l->l_extendedprice[i] = MarsagliaXOR(&g_s) % 1000 + 100;
	  l->l_discount[i] = MarsagliaXOR(&g_s) % 40 + 1;
	  l->l_tax[i] = MarsagliaXOR(&g_s) % 10 + 1;
	  l->l_returnflag[i] = MarsagliaXOR(&g_s) % 3;
	  l->l_linestatus[i] = MarsagliaXOR(&g_s) % 2;
	  l->l_shipdate[i] = date_of(1990 + (MarsagliaXOR(&g_s) % 10), 0, 0);
}

void generateDataColumns(LineitemColumnar *l)
{
  cilkpub::pedigree_scope scope = cilkpub::pedigree_scope::current();
  cilkpub::DotMix dprng(0xabc);
  dprng.init_scope(scope);

  _Cilk_for ( size_t i = 0; i < l->len; i++ )  {
    generateItem(l, i, dprng);
  }
}


void generateDataRows(Lineitem *l, size_t len)
{
  cilkpub::pedigree_scope scope = cilkpub::pedigree_scope::current();
  cilkpub::DotMix dprng(0xabc);
  dprng.init_scope(scope);
  
  _Cilk_for ( size_t i = 0; i < len; i++ )  {
    generateItem(l + i, dprng);
  }
}

int main(){

	char * repsvar = getenv("REPS");
	int reps = 1;

	char * itemsvar = getenv("ITEMS");
	int items = 1000;

	if (repsvar) {
		reps = std::strtol(repsvar, nullptr, 10);
	}

	if (itemsvar) {
		items = std::strtol(itemsvar, nullptr, 10);
	}

	LineitemColumnar data(items);
	generateDataColumns(&data);

	q1out ans[k_flags][k_status] {};

	for (int i = 0; i < reps; ++i) {
		memset(ans, 0, sizeof(ans));
		tpch_q1_columnar(&data, ans);
	}

	std::cout << "reps: " << reps << ", items:" << items << std::endl;
	for (int i = 0; i < k_flags; ++i){
		for (int j = 0; j < k_status; ++j){
			std::cout << "l_linestatus: " << j << " l_returnflag: " << i << " " << ans[i][j] << std::endl;
		}
	}
}
