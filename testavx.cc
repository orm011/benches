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


void printvector(const char * name, __m256i & v){
	auto vi = (int32_t*)&v;
	printf("%s:\t%d %d %d %d %d %d %d %d\n", name, vi[0], vi[1], vi[2], vi[3], vi[4], vi[5], vi[6], vi[7]);
}

#define DISPLAY(v) printvector(#v, v)


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
	DISPLAY(res1);
	DISPLAY(res1rev);
	DISPLAY(res2);

	__m256i mask = _mm256_set_epi32(0,-1,0,-1,0,-1,0,-1);
	__m256i source = _mm256_set_epi32(50, 50, 50, 50, 50, 50, 50, 50);
	__m256i res_mask = _mm256_mask_i32gather_epi32(source, base, pos, mask, 1);
	DISPLAY(res_mask);

	__m256i permutation = _mm256_set_epi32(1,0,3,2,5,4,7,6);
	__m256i permutation2 = _mm256_set_epi32(0,0,1,1,2,2,3,3); // not really a perm
	auto permres = _mm256_permutevar8x32_epi32(*(__m256i*)base, permutation);
	auto perm2res = _mm256_permutevar8x32_epi32(*(__m256i*)base, permutation2);
	DISPLAY(permres);
	DISPLAY(perm2res);

	__m256i beforemask = _mm256_set_epi32(-1, 1, -1, 1, -1, 1, -1, 1); //0000 0000 1010 1010. -> 00aa.
	auto aftermask = _mm256_movemask_ps(_mm256_castsi256_ps(beforemask));
	printf("aftermask: %x\n", aftermask);
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
