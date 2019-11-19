#pragma once
/**
 * @file tools.h
 *
 * @date 		Created on: Aug 25, 2012
 * @author 		Camille Vuillaume
 * @copyright 	Camille Vuillaume, 2012
 * @defgroup	Tools Tools for Paillier-GMP
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

#ifndef TOOLS_H_
#define TOOLS_H_

#include <stdio.h>
#include <gmp.h>

 /** Convert bit length to byte length
  *
  * @ingroup Tools
  */
#define BIT2BYTE(a) (a+7)>>3

  /** Print debug message
   *
   * @ingroup Tools
   * @param[in] str input debug message
   */
inline void debug_msg(const char* str);


/** Structure for threaded exponentiation
 *
 */
typedef struct {
	mpz_t result; /**< result of exponentiation */
	mpz_t basis; /**< basis of exponentiation */
	mpz_t exponent; /**< exponent of exponentiation */
	mpz_t modulus; /**< modulus of exponentiation */
} exp_args;

/** Generate a pseudo-random number
 *
 * @ingroup Tools
 * @param[out] rnd output random number, randomness coming from /dev/urandom
 * @param[in] len input bit length of the random number to generate
 */
int gen_pseudorandom(
	mpz_t rnd,
	mp_bitcnt_t len);

/** Generate a random number
 *
 * @ingroup Tools
 * @param[out] rnd output random number, randomness coming from /dev/random first and then /dev/urandom
 * @param[in] len input bit length of the random number to generate
 */
int gen_random(
	mpz_t rnd,
	mp_bitcnt_t len);

/** Generate prime number
 *
 * @ingroup Tools
 * @param[out] prime output prime number, randomness coming from /dev/random
 * @param[in] len input bit length of prime number to generate
 */
int gen_prime(
	mpz_t prime,
	mp_bitcnt_t len);

/** Exponentiation with Chinese Remainder Theorem
 *
 * @ingroup Tools
 * @param[out] result output exponentiation result
 * @param[in] base input basis of the exponentiation
 * @param[in] exp_p input exponent for modulo p exponentiation
 * @param[in] exp_q input exponent for modulo q exponentiation
 * @param[in] pinvq input CRT parameter
 * @param[in] p input modulus p
 * @param[in] q input modulus q
 */
int crt_exponentiation(
	mpz_t result,
	mpz_t base,
	mpz_t exp_p,
	mpz_t exp_q,
	mpz_t pinvq,
	mpz_t p,
	mpz_t q);


#endif /* TOOLS_H_ */
