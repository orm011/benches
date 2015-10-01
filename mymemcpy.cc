#include "common.h"

int main(int ac, char** av) {
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "show this")
	    ("sizemb", po::value<int>()->default_value(32), "size in mb")
	    ("reps", po::value<int>()->default_value(1), "number of repetitions")
		("cores", po::value<int>()->default_value(1), "number of cores");

	po::variables_map vm;
	po::store(po::parse_command_line(ac, av, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
	    cout << desc << "\n";
	    return 1;
	}

	int sizemb = vm["sizemb"].as<int>();
	int reps = vm["reps"].as<int>();
	int cores = vm["cores"].as<int>();

	uint64_t sizeb = ((uint64_t)sizemb)*(1 << 20);
	uint64_t * bytes = new uint64_t[sizeb/8];
	auto startt = clk::now();

#pragma cilk grainsize = 1
	_Cilk_for (int i = 0; i < cores; ++i){
		for  (int i = 0; i < reps; ++i) {
			memset(bytes, i, sizeb);
		}
	}
	auto endt = clk::now();

	std::cout << "{size_mb:" << sizemb << ", reps:" << reps << ", duration_millis:" << duration_millis(startt, endt) << "}" << std::endl;
}
