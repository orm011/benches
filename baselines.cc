/*
 * baselines.cc
 *
 *  Created on: Oct 12, 2015
 *      Author: orm
 */
#include <cilkpub/dotmix.h>
#include <cilkpub/sort.h>
#include <ostream>
#include "common.h"
#include <thread>


struct TaskData {
	word *words;
	thread t;
};

void generateWords (const vector<TaskData>  &lword, size_t len) {
	cilkpub::pedigree_scope scope = cilkpub::pedigree_scope::current();
	cilkpub::DotMix c(0xabc);
	c.init_scope(scope);

	_Cilk_for (size_t vec = 0; vec < lword.size(); ++vec) {
		_Cilk_for (size_t item = 0; item < len; ++item) {
			int s = c.get();
			for (int i = 0; i < 8; ++i) {
				lword[vec].words->_pad[i] = MarsagliaXOR(&s);
			}
		}
	}
}


const size_t k_unroll = 8;
void baseline(const word *l, size_t len,  int64_t *out) {
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

int64_t runBaseline(const word *l, size_t len, int reps) {
	int64_t out = 0;

	for (int i = 0; i < reps; ++i) {
		baseline(l, len, &out);
	}

	return out;
}


int main(int ac, char** av){
	po::options_description desc("Allowed options");
	vector<int> selectivities  {90};
	vector<int> threadlevels {1,2};

	desc.add_options()
		("help", "show this")
		("threadlevels", po::value<vector<int>>()->multitoken()->default_value(threadlevels, "1, 2"), "different numbers of threads to try")
	    ("items", po::value<int>()->default_value(1024), "items in lineitem")
	    ("reps", po::value<int>()->default_value(1), "number of repetitions");

	po::variables_map vm;
	po::store(po::parse_command_line(ac, av, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
	    cout << desc << "\n";
	    return 1;
	}

	int items = vm["items"].as<int>();
	int reps = vm["reps"].as<int>();
	threadlevels = vm["threadlevels"].as<vector<int>>();
	assert(threadlevels.size() > 0);
	std::sort(threadlevels.begin(), threadlevels.end());
	vm.erase("threadlevels");

	vector<TaskData> task_data(threadlevels.back());

	for (int i = 0; i < threadlevels.back(); ++i) {
		task_data[i].words = new word[items];
	}

	generateWords(task_data, items);

	BenchmarkOutput bo(vm);

	bool first = true;
	for (auto threads : threadlevels) {
			auto before = clk::now();

			for (auto & w : task_data) {
				w.t = thread(runBaseline, w.words, items, reps);
			}

			auto between = clk::now();
			for (auto & w : task_data) { w.t.join(); }
			auto after = clk::now();

			ADD(bo, threads);
			auto time_millis = duration_millis(before, after);
			ADD(bo, time_millis);
			auto setup_millis = duration_millis(before, between);
			ADD(bo, setup_millis);

			if (first) { bo.display_param_names(); }
			first = false;

			bo.display_param_values();
	}
}
