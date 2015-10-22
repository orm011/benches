/*
 * common.h
 *
 *  Created on: Sep 30, 2015
 *      Author: orm
 */

#ifndef COMMON_H_
#define COMMON_H_

#include <cstring>
#include <string>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <chrono>
#include <cassert>
#include <thread>
#include <utility>
#include <unordered_map>
#include <boost/program_options.hpp>

using namespace std;
using namespace chrono;
using clk = high_resolution_clock;
namespace po = boost::program_options;

inline int duration_millis(auto before, auto after){
	return std::chrono::duration_cast<std::chrono::milliseconds>(after - before).count();
}

class my_variable_value {
public:
    my_variable_value()  {}

    my_variable_value(const boost::any& xv)
    : v(xv) {}

    /** If stored value if of type T, returns that value. Otherwise,
        throws boost::bad_any_cast exception. */
   template<class T>
   const T& as() const {
       return boost::any_cast<const T&>(v);
   }
   /** @overload */
   template<class T>
   T& as() {
       return boost::any_cast<T&>(v);
   }

private:
    boost::any v;
};


string to_string(const  my_variable_value &v) {
	try {
		return v.as<string>();
	} catch (exception &e) { try {
		return std::to_string(v.as<int>());
	} catch (exception &e){ try {
		return std::to_string(v.as<bool>());
	} catch (exception &e) {
		return string("");
	}}}
}

#define ADD(bo, x) (bo).set_variable(#x, (x))

template <typename VM> class BenchmarkOutput {

public:
	BenchmarkOutput(){ }

	BenchmarkOutput(const VM &vm){
		for (const auto &p: vm) {
			pairs[p.first] = to_string(p.second);
		}
	}

	template <typename T> void set_variable(const std::string & a, const T &b) {
		pairs[a] = to_string(b);
	}

	void set_variable(const std::string & a, const string &b) {
		pairs[a] = b;
	}

	void display_param_names(const std::string & delimiter = ",") const {
		for (auto & p : pairs) {
			cout << p.first << delimiter;
		}
		 cout << endl;
	}

	void display_param_values(const std::string &delimiter = ",") const {
		for (auto & p : pairs){
			cout << p.second << delimiter;
		}

		cout << endl;
	}

private:
	map<string,string> pairs {};
};

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
};

struct word {
	int64_t _pad[8];
};

/**
 * 64 bytes -> 256 bits for avx2
 */
template <typename T> T* allocate(size_t len, size_t byte_alignment = 64) {
	 char *p = (char*)malloc(sizeof(T)*len + byte_alignment); // extra padding

	 if (byte_alignment > 0  && ((size_t)p) % byte_alignment != 0) {
		 p += byte_alignment;
		 auto remainder = ((size_t)p) % byte_alignment;
		 p -= remainder;
		 assert (((size_t)p % byte_alignment) == 0);
	 }

	 return (T*)p;
}


#endif /* COMMON_H_ */
