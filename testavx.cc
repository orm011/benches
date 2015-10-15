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

void avx256gather() {
	int32_t * base = allocate<int32_t>(32);
	for (int i = 0; i < 32; ++i) {
		base[i] = i;
	}

	__m256i pos = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
	__m256i pos2 = _mm256_set_epi32(0, 1, 2, 3, 4, 5, 6, 7);

	__m256i res1 = _mm256_i32gather_epi32(base, pos, 4);
	__m256i res1rev = _mm256_i32gather_epi32(base, pos2, 4);
	__m256i res2 = _mm256_i32gather_epi32(base, pos, 4);

	int32_t * res1i = (int32_t*)(&res1);
	int32_t * res1irev = (int32_t*)(&res1rev);
	int32_t * res2i = (int32_t*)(&res2);

	printf("1: %d %d %d %d %d %d %d %d\n", res1i[0], res1i[1], res1i[2], res1i[3], res1i[4], res1i[5], res1i[6], res1i[7]);
	printf("1rev: %d %d %d %d %d %d %d %d\n", res1irev[0], res1irev[1], res1irev[2], res1irev[3], res1irev[4], res1irev[5], res1irev[6], res1irev[7]);
	printf("2: %d %d %d %d %d %d %d %d\n", res2i[0], res2i[1], res2i[2], res2i[3], res2i[4], res2i[5], res2i[6], res2i[7]);

	const int32_t ffs = 0xffffffff;
	__m256i mask = _mm256_set_epi32(0,ffs,0,ffs,0,ffs,0,ffs);
	__m256i source = _mm256_set_epi32(50, 50, 50, 50, 50, 50, 50, 50);
	__m256i res_mask = _mm256_mask_i32gather_epi32(source, base, pos, mask, 1);

	int32_t * res_maski = (int32_t*)(&res_mask);
	printf("masked: %d %d %d %d %d %d %d %d\n", res_maski[0], res_maski[1], res_maski[2], res_maski[3], res_maski[4], res_maski[5], res_maski[6], res_maski[7]);
}

void compare() {
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


int main() {
	avx256gather();
}
