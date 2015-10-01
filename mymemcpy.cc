#include <cstring>
#include <string>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <chrono>

using namespace std::chrono;
using clk = high_resolution_clock;


int main() {
	uint64_t byteslen = 1 << 25; // 32MB
	uint64_t reps = 2;

	if (auto b = getenv("BYTES")){
		byteslen = std::strtol(b, NULL, 10);
	}

	if (auto r = getenv("REPS")){
		reps = std::strtol(r, NULL, 10);
	}


	uint64_t * bytes = new uint64_t[byteslen/8];
	auto startt = clk::now();
	for  (uint64_t i = 0; i < reps; ++i) {
		memset(bytes, i, byteslen);
	}
	auto endt = clk::now();

	auto dur = duration_cast<milliseconds>(endt - startt).count();
	std::cout << "{byteslen:" << byteslen << ", reps:" << reps << ", duration_millis:" << dur << "}" << std::endl;
}
