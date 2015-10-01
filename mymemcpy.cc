#include "common.h"
#include <vector>

int main(int ac, char** av) {
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "show this")
	    ("sizemb", po::value<int>()->default_value(32), "size in mb")
	    ("reps", po::value<int>()->default_value(1), "number of repetitions")
		("threads", po::value<int>()->default_value(1), "number of threads");

	po::variables_map vm;
	po::store(po::parse_command_line(ac, av, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
	    cout << desc << "\n";
	    return 1;
	}

	int sizemb = vm["sizemb"].as<int>();
	int reps = vm["reps"].as<int>();
	int threads = vm["threads"].as<int>();

	uint64_t sizeb = ((uint64_t)sizemb)*(1 << 20);
	vector<uint64_t*> arrs(threads);
	vector<thread> workers(threads);

	for (auto i = 0; i < threads; ++i) {
		arrs.push_back(new uint64_t[sizeb/8]);
	}

	auto run = [&](int thnum){
		for  (int i = 0; i < reps; ++i) {
			memset(arrs[thnum], i, sizeb);
		}
	};

	auto startt = clk::now();
	for (auto i = 0; i < threads; ++i){
		workers.emplace_back(run, i);
	}

	for (auto i = 0; i < threads; ){
		workers[i].join();
	}
	auto endt = clk::now();

	std::cout << "{size_mb:" << sizemb << ", reps:" << reps \
			<< ", duration_millis:" << duration_millis(startt, endt) \
			<< ", threads:" << threads << "}" << std::endl;
}
