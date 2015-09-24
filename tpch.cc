#include <tuple>
#include <cstdlib>
#include <cassert>
#include <cilkpub/dotmix.h>

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
	int l_orderkey;
	int l_partkey;
	int l_suppkey;
	int l_linenumber;
	int l_quantity;
	int l_extendedprice;
	int l_discount;
	int l_tax;
	char l_returnflag;
	char l_linestatus;
	int l_shipdate;
	int l_commitdate;
	int l_receiptdate;
	int l_shipinstruct;
	int l_shipmode;
	int l_comment;

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


struct q1out {
	char l_returnflag;
	char l_linestatus;
	int sum_qt;
	int sum_base_price;
	int sum_disc_price;
	int sum_charge;
	int avg_qty;
	int avg_price;
	int avg_disc;
	int count;
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

void tpch_q1_as(Lineitem *l, size_t len, q1out *out) {
	int acc_counts[k_flags][k_status] {};
	int acc_quantity[3][2] {};
	int acc_baseprice[3][2] {};
	int acc_discounted[3][2] {};
	int acc_disctax[3][2] {};

	const int k_date = date_of(1998, 1, 1); // 10% selectivity.
  
	for (size_t i = 0; i < len; ++i) {
		const auto & item = l[i];
		if (item.l_shipdate <= k_date) {
			auto &flag = item.l_returnflag;
			auto &status = item.l_linestatus;

			acc_counts[flag][status] += 1;
			acc_quantity[flag][status] += item.l_quantity;
			acc_baseprice[flag][status] += item.l_extendedprice;
			acc_discounted[flag][status] += (item.l_extendedprice * (100 - item.l_discount))/100;
			acc_disctax[flag][status] += (item.l_extendedprice * (100 - item.l_discount) * (100 + item.l_tax))/ 1'00'00 ;
		}
	}


	for (int flag = 0; flag < k_flags; flag++){
	  for (int status = 0; status < k_status; status++){
	    auto & op = out[flag * k_status + status];
	    op.l_linestatus = status;
	    op.l_returnflag = flag;
	    op.sum_base_price = acc_baseprice[flag][status];
	  }
	}
}

struct Lineitems {
	int *l_quantity;
	int *l_extendedprice;
	int *l_discount;
	int *l_tax;
	int *l_returnflag;
	int *l_linestatus;
	int *l_shipdate; //where
};

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

void generateData(Lineitem *l, size_t len)
{
  cilkpub::pedigree_scope scope = cilkpub::pedigree_scope::current();
  cilkpub::DotMix dprng(0xabc);
  dprng.init_scope(scope);
  
  _Cilk_for ( size_t i = 0; i < len; i++ )  {
    generateItem(l + i, dprng);
  }
}

int main(){
	auto *items = new Lineitem[ITEMS];
	generateData(items, ITEMS);

	q1out ans[k_flags*k_status] {};

	for (int i = 0; i < 10; ++i){
		tpch_q1_as(items, ITEMS, ans);
	}
}
