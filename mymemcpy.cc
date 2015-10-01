#include <cstring>
#include <string>
#include <cstdlib>
#include <cstdint>

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
	for  (uint64_t i = 0; i < reps; ++i) {
		memset(bytes, i, byteslen);
	}
}
