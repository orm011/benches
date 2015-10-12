/*
 * testavx.cc
 *
 *  Created on: Oct 12, 2015
 *      Author: orm
 */

#include <emmintrin.h>
#include "common.h"


int32_t avx256sum(int32_t *x) {

	__m256i sums;

	for (int i = 0; i < 512; i+=8) {
		auto elts = _mm256_load_si256((__m256i*)(x + i));
		sums = _mm256_add_epi32(sums, elts);
	}

	int32_t total =0;
	for (int lane = 0; lane < 8; ++lane){
		total += reinterpret_cast<int32_t*>(&sums)[lane];
	}
	return total;
}

int main() {
	int32_t x[512]; // alignment?
	for (int i = 0; i < 512; ++i) {
		x[i] = 1;
	}

	auto total = avx256sum(x);
	cout << "total: " << total << endl;
}
