#include "common.h"
#include <vector>
#include <functional>
#include <unordered_map>
#include "emmintrin.h"

enum struct Task {
	MEMAGG,
	LIBCMEMSET,
	NAIVEMEMSET
};

struct params {
	int sizemb;
	int reps;
	int threads;
	Task task;
	po::variables_map * vm;

	size_t getSizeB() const {
		return ((size_t)sizemb) * (1 << 20);
	}

	void outputParamNames() const {
		output_param_names(*vm);
	}

	void outputParamValues() const {
		output_param_values(*vm);
	}
};

void bench_memset(const params &p) {
	vector<thread> workers;
	vector<long long *> arrs;

	for (auto i = 0; i < p.threads; ++i) {
		arrs.push_back(new long long[p.getSizeB()/8]);
	}

	auto manual_var = [&](int thnum){
		for  (int i = 0; i < p.reps; ++i) {
			for (size_t pos = 0; pos < p.getSizeB()/8; ++pos) {
				long long  * p = &arrs[thnum][pos];
				_mm_stream_si64 (p, (long long)i);
			}
		}
	};

	auto libc_memset= [&](int thnum) {
		for  (int i = 0; i < p.reps; ++i) {
			memset(arrs[thnum], i, p.getSizeB());
		}
	};

	function<void(int)> variant = libc_memset;

	if (p.task == Task::NAIVEMEMSET) {
		variant = manual_var;
	}

	auto startt = clk::now();
	for (auto i = 0; i < p.threads; ++i) {
		workers.emplace_back(variant, i);
	}
	auto forkt = clk::now();
	for (auto i = 0; i < p.threads; ++i) {
		workers[i].join();
	}

	auto endt = clk::now();

	p.outputParamNames();
	cout << "duration_millis" << ", fork_millis" << endl;
	p.outputParamValues();
	cout << duration_millis(startt, endt) << ", " <<  duration_millis(startt, forkt) << endl;
}

void bench_agg(const params &p){
	vector<thread> workers;
	vector<uint64_t*> arrs;

	for (auto i = 0; i < p.threads; ++i) {
		arrs.push_back(new uint64_t[p.getSizeB()/8]);
		workers.emplace_back(memset, arrs[i], 0xff, p.getSizeB());
	}

	for (auto i = 0; i < p.threads; ++i) {
		workers[i].join();
	}

	workers.clear();
	auto agg = [&](int thnum, int *out){
		int aggregate = 0;
		for (int i = 0; i < p.reps; ++i){
			for (size_t pos =0; pos < p.getSizeB()/8; ++pos){
				aggregate ^= arrs[thnum][pos];
			}
		}

		*out = aggregate;
	};

	auto startt = clk::now(); // over-write
	int *ans = new int[p.threads];

	for (auto i = 0; i < p.threads; ++i){
		workers.emplace_back(agg, i, &ans[i]);
	}
	auto forkt = clk::now();

	for (auto i = 0; i < p.threads; ++i){
		workers[i].join();
	}
	auto endt = clk::now();

	p.outputParamNames();
	cout << "duration_millis" << ",fork_millis" << endl;
	p.outputParamValues();
	cout << duration_millis(startt, endt) << "," <<  duration_millis(startt, forkt) << endl;
}



int main(int ac, char** av) {
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "show this")
	    ("sizemb", po::value<int>()->default_value(32), "size in mb")
	    ("reps", po::value<int>()->default_value(1), "number of repetitions")
		("threads", po::value<int>()->default_value(1), "number of threads")
		("task", po::value<string>()->default_value("memagg"), "naivememset,libcmemset,memagg");

	po::variables_map vm;
	po::store(po::parse_command_line(ac, av, desc), vm);
	po::notify(vm);

	unordered_map<string, Task> dict {
		{"naivememset", Task::NAIVEMEMSET},
		{"libcmemset", Task::LIBCMEMSET},
		{"memagg", Task::MEMAGG}
	};

	if (vm.count("help")) {
	    cout << desc << "\n";
	    return 1;
	}

	params p;
	p.sizemb = vm["sizemb"].as<int>();
	p.reps = vm["reps"].as<int>();
	p.threads = vm["threads"].as<int>();
	p.task = dict.at(vm["task"].as<string>());
	p.vm = &vm;

	if (p.task == Task::LIBCMEMSET || p.task == Task::NAIVEMEMSET) {
		bench_memset(p);
	} else if (p.task == Task::MEMAGG) {
		bench_agg(p);
	}
}
