#pragma once
/*
 * @file tools.c
 *
 * @date Created on: Aug 25, 2012
 * @author Camille Vuillaume
 * @copyright Camille Vuillaume, 2012
 *
 * This file is part of Paillier-GMP.
 *
 * Paillier-GMP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * 
 * Paillier-GMP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Paillier-GMP.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <gmp.h>
#include <pthread.h>
#include "tools.h"

 /**
  * The function prints debug messages to stderr; it is compiled out if PAILLIER_DEBUG is not defined.
  */
inline void debug_msg(const char* str) {
#ifdef PAILLIER_DEBUG
	fputs(str, stderr);
#endif
}

/**
 * Generate a random number using /dev/urandom.
 * Random number generation does not block.
 */
int gen_pseudorandom(mpz_t rnd, mp_bitcnt_t len) {
	FILE* dev_urandom;
	int byte_count, byte_read;
	char* seed;

	byte_count = BIT2BYTE(len);

	dev_urandom = fopen("/dev/urandom", "r");
	if (dev_urandom == NULL) {
		fprintf(stderr, "cannot open random number device!\n");
		exit(1);
	}
	seed = (char*)malloc(sizeof(char) * byte_count);

	//printf("adrr seed = % #X\n", (void*)seed);
	//printf("+++++++++++++++++++byte_count = %d\n", byte_count);

	byte_read = 0;
	//generate the bytes with /dev/urandom
	while (byte_read < byte_count) {
		byte_read += fread(seed, sizeof(char), byte_count, dev_urandom);
	}
	fclose(dev_urandom);
	mpz_import(rnd, byte_count, 1, sizeof(seed[0]), 0, 0, seed);

	/*printf("adrr rnd = % #X\n", (void*)rnd->_mp_d);
	printf("size rnd = %d\n", rnd->_mp_size);
	printf("alloc rnd = %d\n", rnd->_mp_alloc);*/


	free(seed);
	return 0;
}

/**
 * Generate a stable number to fix result.
 */

int gen_stablekey(mpz_t stb, mp_bitcnt_t len)
{
	int byte_count, byte_read;
	char* fixData;

	byte_count = BIT2BYTE(len);

	fixData = (char*)malloc(sizeof(char) * byte_count);
	int i;
	for (i = 0; i < 4; i++)
	{
		/* 0 ~ 255 */
		fixData[i] = (char)10;
	}
	for (i = 4; i < byte_count; i++)
	{
		fixData[i] = (char)0;
	}
	mpz_import(stb, byte_count, 1, sizeof(fixData[0]), 0, 0, fixData);
	free(fixData);
}

/**
 * Generate a random number using /dev/random for the first 128 bits and then /dev/urandom.
 * This guarantees that there is enough entropy in the pool and that it is safe to use /dev/urandom.
 * Since /dev/random is used, if entropy is insufficient the program will block.
 * In that case, it is necessary to feed /dev/random with entropy, for example by moving the mouse.
 */
int gen_random(mpz_t rnd, mp_bitcnt_t len) {
	FILE* dev_random, * dev_urandom;
	int byte_count, byte_read;
	char* seed;

	byte_count = BIT2BYTE(len);

	dev_random = fopen("/dev/random", "r");
	if (dev_random == NULL) {
		fprintf(stderr, "cannot open random number device!\n");
		exit(1);
	}
	dev_urandom = fopen("/dev/urandom", "r");
	if (dev_urandom == NULL) {
		fprintf(stderr, "cannot open random number device!\n");
		exit(1);
	}

	seed = (char*)malloc(sizeof(char) * byte_count);

	byte_read = 0;
	//generate the first 16 bytes with /dev/random
	while (byte_read < 16 && byte_read < byte_count) {
		byte_read += fread(seed, sizeof(char), byte_count, dev_random);
	}
	fclose(dev_random);
	//generate the remaining bytes with /dev/urandom
	while (byte_read < byte_count) {
		byte_read += fread(seed, sizeof(char), byte_count, dev_urandom);
	}
	fclose(dev_urandom);

	mpz_import(rnd, byte_count, 1, sizeof(seed[0]), 0, 0, seed);
	free(seed);
	return 0;
}

/**
 * Generate a random prime number using /dev/random and /dev/urandom as a source of randomness.
 * @see gen_random
 */
int gen_prime(mpz_t prime, mp_bitcnt_t len) {
	mpz_t rnd;

	mpz_init(rnd);

	gen_random(rnd, len);

	//set most significant bit to 1
	mpz_setbit(rnd, len - 1);
	//look for next prime
	mpz_nextprime(prime, rnd);

	mpz_clear(rnd);
	return 0;
}

/** Threaded exponentiation
 * @ingroup Tools
 *
 * This function reduces the input modulo the given modulus, and performs a modular exponentiation.
 * It is intended to be run in a pthread.
 *
 * @param[in,out] args arguments for the exponentiation, a pointer to an exp_args, with
 * - exp_args::result storing the result of the exponentiation
 * - exp_args::basis storing the basis
 * - exp_args::exponent storing the exponent
 * - exp_args::modulus storing the modulus
 *
 */
void* do_exponentiate(void* args) {
	mpz_t basis_reduced;
	exp_args* args_struct = (exp_args*)args;

	mpz_init(basis_reduced);
	mpz_mod(basis_reduced, args_struct->basis, args_struct->modulus);
	mpz_powm(args_struct->result, basis_reduced, args_struct->exponent, args_struct->modulus);

	mpz_clear(basis_reduced);
	pthread_exit(NULL);
}


/**
 * The exponentiation is computed using Garner's method for the CRT:
 * - Exponentiation mod p: y_p = (x mod p)^{exp_p} mod p
 * - Exponentiation mod q: y_q = (x mod q)^{exp_q} mod q
 * - Recombination: y = y_p + p*(p^{-1} mod q)*(y_q-y_p) mod n
 * .
 * The exponentiations mod p and mod q run in their own thread.
 */
int crt_exponentiation(mpz_t result, mpz_t base, mpz_t exp_p, mpz_t exp_q, mpz_t pinvq, mpz_t p, mpz_t q) {
	mpz_t pq;
	exp_args* args_p, * args_q;

#ifdef PAILLIER_THREAD
	pthread_t thread1, thread2;
#endif

	mpz_init(pq);

	//prepare arguments for exponentiation mod p
	args_p = (exp_args*)malloc(sizeof(exp_args));

	mpz_init(args_p->result);
	mpz_init(args_p->basis);
	mpz_init(args_p->exponent);
	mpz_init(args_p->modulus);

	mpz_set(args_p->basis, base);
	mpz_set(args_p->exponent, exp_p);
	mpz_set(args_p->modulus, p);

	//prepare arguments for exponentiation mod q
	args_q = (exp_args*)malloc(sizeof(exp_args));

	mpz_init(args_q->result);
	mpz_init(args_q->basis);
	mpz_init(args_q->exponent);
	mpz_init(args_q->modulus);

	mpz_set(args_q->basis, base);
	mpz_set(args_q->exponent, exp_q);
	mpz_set(args_q->modulus, q);

#ifdef PAILLIER_THREAD
	//compute exponentiation modulo p
	pthread_create(&thread1, NULL, do_exponentiate, (void*)args_p);

	//compute exponentiation modulo q
	pthread_create(&thread2, NULL, do_exponentiate, (void*)args_q);

	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);

#else
	//compute exponentiation modulo p
	mpz_mod(args_p->result, base, p);
	mpz_powm(args_p->result, args_p->result, exp_p, p);

	//compute exponentiation modulo q
	mpz_mod(args_q->result, base, q);
	mpz_powm(args_q->result, args_q->result, exp_q, q);
#endif

	//recombination
	mpz_mul(pq, p, q);
	mpz_sub(result, args_q->result, args_p->result);
	mpz_mul(result, result, p);
	mpz_mul(result, result, pinvq);
	mpz_add(result, result, args_p->result);
	mpz_mod(result, result, pq);

	mpz_clear(pq);
	mpz_clear(args_p->result);
	mpz_clear(args_p->basis);
	mpz_clear(args_p->exponent);
	mpz_clear(args_p->modulus);
	mpz_clear(args_q->result);
	mpz_clear(args_q->basis);
	mpz_clear(args_q->exponent);
	mpz_clear(args_q->modulus);
	free(args_p);
	free(args_q);

	return 0;
}
