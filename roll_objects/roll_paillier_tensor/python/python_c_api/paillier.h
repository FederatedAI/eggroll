#pragma once
#include <stdio.h>
#include <gmp.h>

#define LOG2_BASE 4
#define BASE 16
#define FLOAT_MANTISSA_BITS 53

typedef int int32_t;
typedef long int int64_t;

typedef unsigned int uint32_t;
typedef unsigned long int uint64_t;

typedef float float32_t;
typedef double float64_t;

typedef struct Point {
	double x, y;
} Point;


typedef struct
{
	mpz_t bot;
	uint64_t exp;
}mpx;


typedef struct
{
	mpz_t* field;
	int64_t* exp;
	int row;
	int col;
	int ifmal;
	int ifenc;
}mpz_manager;


typedef struct
{
	mpz_t g;
	mpz_t n;
	mpz_t n2;
	mpz_t max_int;
	mpz_t sub_n_max;
	mp_bitcnt_t len;

}eggroll_public_key;

typedef struct
{
	mpz_t p;
	mpz_t q;
	mpz_t p2;
	mpz_t q2;
	mpz_t qinver;
	mpz_t hp;
	mpz_t hq;
	mp_bitcnt_t len;

}eggroll_private_key;


/* Key Gen*/
void eggroll_keygen(eggroll_public_key* pub, eggroll_private_key* priv, unsigned long int Len);

void eggroll_public_init(eggroll_public_key* pub);

void eggroll_private_init(eggroll_private_key* priv);

void eggroll_keygen_stable(eggroll_public_key* pub,
	eggroll_private_key* priv, unsigned long int Len);

/* eggroll Math Function*/
void eggroll_raw_encrypt(eggroll_public_key *pub, mpz_t plaintext, mpz_t ciphertext);

void eggroll_raw_decrypt(eggroll_public_key* pub, eggroll_private_key* priv,
	mpz_t ciphertext, mpz_t plaintext);

void eggroll_non_obfuscator(eggroll_public_key* pub, mpz_t ciphertext);

void eggroll_obfuscator(eggroll_public_key* pub, mpz_t ciphertext);

void eggroll_mulc(mpz_t ciphertext2, mpz_t ciphertext1, mpz_t constant, eggroll_public_key* pub);

void testeggrollPaillier(FILE* public_key, FILE* private_key, FILE* plaintext);

void eggroll_add(mpz_t ciphertext3, mpz_t ciphertext1, mpz_t ciphertext2, eggroll_public_key* pub);



/*================================= exp encode ===========================================*/
static void mpx_init(mpx* i)
{
	mpz_init(i->bot);
	i->exp = 0;
}

static void mpx_delete(mpx* i)
{
	mpz_clear(i->bot);
	i->exp = 0;
}

int64_t alineCiper(eggroll_public_key* pub, mpz_t cipher1, mpz_t cipher2, int64_t exp1, int64_t exp2);


int64_t encode_float64(eggroll_public_key* pub, mpz_t base, float64_t val,
	int64_t precision, int64_t max_exp);

int64_t encode_int64(eggroll_public_key* pub, mpz_t base, int64_t val,
	int64_t precision, int64_t max_exp);

float64_t decode_float64(eggroll_public_key* pub, mpz_t base, int64_t exponent);

float64_t decode_int64(eggroll_public_key* pub, mpz_t base, int64_t exponent);

/* ============================= Mat Vec protect  =============================== */

void encrpytProtect(mpz_manager* mng, eggroll_public_key* pub);

/* ============================= Mat Vec function  =============================== */

void keyInit(eggroll_public_key* pub, eggroll_private_key* priv);

void initManager(mpz_manager* target, int col, int row, int bits);

//void dataEncrpyt(mpz_t* ciphy, mpz_t* data,
//	eggroll_public_key* pub, int col, int row);
//void dataDecrpyt(mpz_t* cipyh, mpz_t* data, eggroll_public_key* pub,
//	eggroll_private_key* priv, int col, int row);

void dataEncrpyt(mpz_manager* ciphy, mpz_manager* data,
	eggroll_public_key* pub, int col, int row);

void dataRawEncrpyt(mpz_t* ciphy, uint64_t* data,
	eggroll_public_key* pub, int col, int row);

void dataDecrpyt(mpz_manager* ciphy, mpz_manager* data,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row);

void dataDecode(mpz_manager* coder, double* val,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row);

void dataObfuscator(mpz_t* ciphy, eggroll_public_key* pub, eggroll_private_key* priv,
	int col, int row);

void matMul(mpz_manager* res, mpz_manager* mat1, mpz_manager* mat2,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row);

void matDotMul(mpz_manager* res, mpz_manager* mat1, mpz_manager* mat2,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row);

void matSlcMul(mpz_manager* res, mpz_manager* mat, double slc,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row);

void matDotAdd(mpz_manager* res, mpz_manager* mat1, mpz_manager* mat2,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row);


void rToC(uint64_t* a, int row, int col);

/*================================= about D/Serialization  ===================================*/
int get_public_len(eggroll_public_key* pub);
int get_private_len(eggroll_private_key* priv);

int dumpingPub(eggroll_public_key* pub, uint64_t* mem);
int dumpingPriv(eggroll_private_key* priv, uint64_t* mem);

int loadPub(eggroll_public_key* pub, uint64_t* loading);
int loadPriv(eggroll_private_key* priv, uint64_t* loading);
