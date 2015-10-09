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
#include <boost/program_options.hpp>
#include <thread>
#include <utility>
#include <unordered_map>

using namespace std;
using namespace chrono;
using clk = high_resolution_clock;
namespace po = boost::program_options;

inline int duration_millis(auto before, auto after){
	return std::chrono::duration_cast<std::chrono::milliseconds>(after - before).count();
}


string to_string(const po::variable_value &v) {
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

class BenchmarkOutput {

public:
	BenchmarkOutput(){ }

	BenchmarkOutput(const po::variables_map &vm){
		for (const auto &p: vm) {
			pairs[p.first] = to_string(p.second);
		}
	}

	template <typename T> void set_variable(const std::string & a, const T &b) {
		pairs[a] = to_string(b);
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
	unordered_map<string,string> pairs {};
};


#endif /* COMMON_H_ */
