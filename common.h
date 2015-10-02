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

using namespace std;
using namespace chrono;
using clk = high_resolution_clock;
namespace po = boost::program_options;

inline int duration_millis(auto before, auto after){
	return std::chrono::duration_cast<std::chrono::milliseconds>(after - before).count();
}


inline void output_param_names(const po::variables_map &vm){
	for (const auto &p: vm) {
		cout << p.first << ",";
	}
}

inline void output_param_values(const po::variables_map &vm){
	for (const auto &p: vm) {
		try {
			cout << p.second.as<string>() << ",";
		} catch (exception &e) {
			try {
				cout << p.second.as<int>() << ",";
			} catch (exception &e){
				try {
					cout << p.second.as<bool>() << ",";
				} catch (exception &e) {
					throw e;
				}
			}
		}
	}
}

#endif /* COMMON_H_ */
