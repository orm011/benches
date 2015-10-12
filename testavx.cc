/*
 * testavx.cc
 *
 *  Created on: Oct 12, 2015
 *      Author: orm
 */

#define _XOPMMINTRIN_H_INCLUDED

#include <immintrin.h>
#include "common.h"


int32_t avx256sum(int32_t *x, int size) {

	__m256i sums {};

	for (int i = 0; i < size; i+=8) {
		auto elts = _mm256_load_si256((__m256i*)(x + i));
		sums = _mm256_add_epi32(sums, elts);
	}

	int32_t total =0;
	for (int lane = 0; lane < 8; ++lane){
		total += reinterpret_cast<int32_t*>(&sums)[lane];
	}
	return total;
}

int32_t avx128sum(int32_t *x, int size) {
	__m128i sums = _mm_set1_epi32(0);

	for (int i = 0; i < size; i+=4) {
		auto elts = _mm_load_si128((__m128i*)(x + i));
		sums = _mm_add_epi32(sums, elts);
	}

	int32_t total =0;
	for (int lane = 0; lane < 4; ++lane){
		total += reinterpret_cast<int32_t*>(&sums)[lane];
	}
	return total;
}


int main() {
	int32_t x[512]; // alignment?
	for (int i = 0; i < 512; ++i) {
		x[i] = 1;
	}

	auto total = avx128sum(x, 512);
	cout << "total: " << total << endl;

	__m128i data = _mm_set_epi32(3, 2, 1, 0);
	__m128i comp = _mm_set1_epi32(1);
	__m128i res = _mm_cmpgt_epi32(data, comp);

	cout << "compare"<< endl;
	int32_t * res2 = reinterpret_cast<int32_t *>(&res);
	printf("%x %x %x %x\n", res2[0], res2[1], res2[2], res2[3]);
}
