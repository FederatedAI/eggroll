
#ifdef __linux
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <cuda.h>
#include <time.h> 
#include <sys/time.h>
#include <assert.h>
#include <math.h>

#include <gmp.h>
#include "cgbn/cgbn.h"
#include "../utility/support.h"

#include "../paillier.h"
#endif

#ifdef _WIN32	

#include<stdio.h>
#include "./cgbn.h"
#include "./cgbn_cuda.h"
#include "cuda_runtime.h"
#include "cuda_device_runtime_api.h"
#include "device_launch_parameters.h"
#include <assert.h>
#include "../paillier.h"

#endif

extern "C" 
{	
	#include "eggPaillier.h"
}
#include "vector_functions.h"

#define TPI 32
//#define BITS 4096
#define BITS 3072
#define INSTANCES 100000
#define BIT2BYTE(a) (a+7)>>3

typedef cgbn_context_t<TPI>         context_t;
typedef cgbn_env_t<context_t, BITS> env_t;

typedef typename env_t::cgbn_t                bn_t;
typedef typename env_t::cgbn_local_t          bn_local_t;
typedef cgbn_mem_t<BITS> gpu_mpz;

typedef struct
{
	cgbn_mem_t<BITS> g;
	cgbn_mem_t<BITS> n;
	cgbn_mem_t<BITS> n2;
	cgbn_mem_t<BITS> max_int;
	cgbn_mem_t<BITS> sub_n_max;
	mp_bitcnt_t len;

}cgbn_public_key;

typedef struct
{
	cgbn_mem_t<BITS> p;
	cgbn_mem_t<BITS> q;
	cgbn_mem_t<BITS> p2;
	cgbn_mem_t<BITS> q2;
	cgbn_mem_t<BITS> qinver;
	cgbn_mem_t<BITS> hp;
	cgbn_mem_t<BITS> hq;
	mp_bitcnt_t len;

}cgbn_private_key;


typedef struct
{
	gpu_mpz* d_field;
	uint64_t* d_exp;
	short* d_sign;
	int row;
	int col;
	int ifmal;
	int ifenc;
}gpu_manager;

extern "C" 
{
#define MASK64_EXP  0x7FF0000000000000
#define MASK64_MANT 0x000FFFFFFFFFFFFF
#define MASK64_ONE  0x0010000000000000

#define MASK32_EXP  0x7F800000
#define MASK32_MANT 0x007FFFFF
#define MASK32_ONE  0x00800000

	static void initManager_d(mpz_manager* target, int col, int row, int bits)
	{
		int i;
		target->col = col;
		target->row = row;
		target->field = (mpz_t*)malloc(sizeof(mpz_t) * col * row);
		target->exp = (int64_t*)malloc(sizeof(int64_t) * col * row);
		for (i = 0; i < col * row; i++)
		{
			mpz_init(target->field[i]);
			mpz_setbit(target->field[i], bits);
			target->exp[i] = 0;
		}
		target->ifenc = 0;
		target->ifmal = 1;
	}

	void store2dev(cgbn_mem_t<BITS>* address, mpz_t z) {
		size_t words;
		if (mpz_sizeinbase(z, 2) > BITS) {
			printf("error mpz_sizeinbase:%d\n", mpz_sizeinbase(z, 2));
			exit(1);
		}
		mpz_export((uint32_t*)address, &words, -1, sizeof(uint32_t), 0, 0, z);

		while (words < (BITS + 31) / 32)
		{
			((uint32_t*)address)[words++] = 0;
		}
	}

	void store2gmp(mpz_t z, cgbn_mem_t<BITS>* address) {
		mpz_import(z, (BITS + 31) / 32, -1, sizeof(uint32_t), 0, 0, (uint32_t*)address);
	}

	void initGpuManager(gpu_manager* d_target, int col, int row)
	{
		/* malloc */
		gpu_manager* h_contain = (gpu_manager*)malloc(sizeof(gpu_manager));

		h_contain->col = col;
		h_contain->row = row;
		h_contain->ifenc = 0; 
		h_contain->ifmal = 1;
		cudaMalloc((void**)& h_contain->d_field, sizeof(gpu_mpz) * col * row);
		cudaMalloc((void**)& h_contain->d_exp, sizeof(uint64_t) * col * row);
		cudaMemset(h_contain->d_field, 0, sizeof(gpu_mpz) * col * row);
		cudaMemset(h_contain->d_exp, 0, sizeof(uint64_t) * col * row);

		/* init device gpu_manager */
		cudaMemcpy(d_target, h_contain, sizeof(gpu_manager), cudaMemcpyHostToDevice);
		free(h_contain);
	}

	/* ========================= gpu paillier kernel ==================== */

	__device__ void eggroll_mulc_dev(env_t* env, env_t::cgbn_t* ciphertext2,
		env_t::cgbn_t* ciphertext1, env_t::cgbn_t* _const, env_t::cgbn_t* n2,
		env_t::cgbn_t* n, env_t::cgbn_t* sub_n_max, env_t::cgbn_t* nc, 
		env_t::cgbn_t* ns)
	{
		if (cgbn_compare(*env, *_const, *sub_n_max) >= 0)
		{
			cgbn_modular_inverse(*env, *nc, *ciphertext1, *n2);
			cgbn_sub(*env, *ns, *n, *_const);
			cgbn_modular_power(*env, *ciphertext2, *nc, *ns, *n2);
		}
		else
		{
			cgbn_modular_power(*env, *ciphertext2, *ciphertext1, *_const, *n2);
		}
	}

	__device__ uint64_t eggroll_alinexp(env_t* env, uint64_t exp1, uint64_t exp2, 
		env_t::cgbn_t* cipher1, env_t::cgbn_t* cipher2, env_t::cgbn_t* n2,
		env_t::cgbn_t* n, env_t::cgbn_t* sub_n_max, env_t::cgbn_t* _f, 
		env_t::cgbn_t* nc, env_t::cgbn_t* ns)
	{
		if (exp1 < exp2)
		{
			//pow(base, exp2 - exp1)
			cgbn_set_ui32(*env, *_f, 1);
			for (int i = 0; i < exp2 - exp1; i++)
			{
				cgbn_mul_ui32(*env, *_f, *_f, (uint32_t)BASE);
			}

			cgbn_rem(*env, *_f, *_f, *n);
			eggroll_mulc_dev(env, cipher1, cipher1, _f, n2, n, sub_n_max, nc, ns);

			return exp2;
		}
		else
		{
			//change cipher2
			//mpz_set_ui(_f, (int64_t)pow(BASE, exp1 - exp2));
			cgbn_set_ui32(*env, *_f, 1);
			for (int i = 0; i < exp1 - exp2; i++)
			{
				cgbn_mul_ui32(*env, *_f, *_f, (uint32_t)BASE);
			}

			cgbn_rem(*env, *_f, *_f, *n);
			eggroll_mulc_dev(env, cipher2, cipher2, _f, n2, n, sub_n_max, nc, ns);
			return exp1;
		}
	}

	__global__ void dev_encrpyt_kernel(int col, int row, cgbn_public_key* d_pub,
		gpu_manager* d_mng, gpu_manager* d_mng_enc, cgbn_error_report_t* report)
	{

		int n32 = BIT2BYTE(BITS) / 4;

		int id = blockIdx.x * blockDim.x + threadIdx.x;
		int team = (blockIdx.x * blockDim.x + threadIdx.x) / TPI;

		if (team >= col * row)
			return;
	/*	printf("[encrypt] %d\n", team);*/



		context_t      bn_context(cgbn_report_monitor, report, team);   // construct a context
		env_t          bn_env(bn_context.env<env_t>());                     // construct an environment for 1024-bit math

		env_t::cgbn_t  n, n2, plain, sub_n_max, max_int, n_plain, n_cipher, cipher;

		cgbn_load(bn_env, n, &d_pub->n);
		cgbn_load(bn_env, n2, &d_pub->n2);
		cgbn_load(bn_env, max_int, &d_pub->max_int);
		cgbn_load(bn_env, sub_n_max, &d_pub->sub_n_max);
		cgbn_load(bn_env, plain, d_mng->d_field + team);

		if (cgbn_compare(bn_env, plain, sub_n_max) >= 0 &&
			cgbn_compare(bn_env, plain, n) < 0)
		{
			// Very large plaintext, take a sneaky shortcut using inverses
			cgbn_sub(bn_env, n_plain, n, plain);
			cgbn_mul(bn_env, n_cipher, n, n_plain);

			cgbn_add_ui32(bn_env, n_cipher, n_cipher, 1);
			cgbn_rem(bn_env, n_cipher, n_cipher, n2);
			cgbn_modular_inverse(bn_env, cipher, n_cipher, n2);

		}
		else {
			cgbn_mul(bn_env, cipher, n, plain);
			cgbn_add_ui32(bn_env, cipher, cipher, 1);
			cgbn_rem(bn_env, cipher, cipher, n2);
		}
		/* set filed */
		cgbn_store(bn_env, d_mng_enc->d_field + team, cipher);   // store r into sum
		/* set filed */
		d_mng_enc->d_exp[team] = d_mng->d_exp[team];
		d_mng_enc->ifenc = 1;


		//if (id == 0)
		//{
		//	for (int i = 0; i < n32; i++)
		//	{
		//		printf("%d ", plain);
		//	}
		//}
	}

	__global__ void dev_dotadd_kernel(int col, int row, cgbn_public_key* d_pub,
		gpu_manager* d_mat1, gpu_manager* d_mat2, gpu_manager* d_res,
		cgbn_error_report_t* report)
	{

		int n32 = BIT2BYTE(BITS) / 4;

		int id = blockIdx.x * blockDim.x + threadIdx.x;
		int team = (blockIdx.x * blockDim.x + threadIdx.x) / TPI;

		if (team >= col * row)
			return;

		//printf("[encrypt] %d\n", team);


		__shared__ uint64_t exp1[4];
		__shared__ uint64_t exp2[4];
		__shared__ uint64_t power[4];

		context_t      bn_context(cgbn_report_monitor, report, team);   // construct a context
		env_t          bn_env(bn_context.env<env_t>());                     // construct an environment for 1024-bit math

		///* variant */
		env_t::cgbn_t  r, n, n2, sub_n_max, cipher1, cipher2;
		env_t::cgbn_t  p, f, nc, ns;

		exp1[team % 4] = d_mat1->d_exp[team];
		exp2[team % 4] = d_mat2->d_exp[team];

		cgbn_load(bn_env, n2, &d_pub->n2);
		cgbn_load(bn_env, n, &d_pub->n);
		cgbn_load(bn_env, sub_n_max, &d_pub->sub_n_max);
		cgbn_load(bn_env, cipher1, d_mat1->d_field + team);
		cgbn_load(bn_env, cipher2, d_mat2->d_field + team);

		if (exp1[team % 4] == exp2[team % 4])
		{

			//printf("exp1[l] = %ld exp2[r] = %ld\n", exp1[team % 4], exp2[team % 4]);

			cgbn_mul(bn_env, r, cipher1, cipher2);
			cgbn_rem(bn_env, r, r, n2);
			d_res->d_exp[team % 4] = exp1[team % 4];
		}
		else
		{
			printf("exp1[l] = %ld exp2[r] = %ld\n", exp1[team % 4], exp2[team % 4]);

			//aline cipher
			d_res->d_exp[team % 4] = eggroll_alinexp(&bn_env, exp1[team % 4],
				exp2[team % 4], &cipher1, &cipher2, &n2, &n, &sub_n_max, &f, &nc, &ns);

			/* eggroll_add */
			cgbn_mul(bn_env, r, cipher1, cipher2);
			cgbn_rem(bn_env, r, r, n2);
		}
		cgbn_store(bn_env, d_res->d_field + team, r);

	}

	/* gpu mpz dot mul*/
	__global__ void dev_dotmul_kernel(int col, int row, cgbn_public_key* d_pub,
		gpu_manager* d_mat1, gpu_manager* d_mat2, gpu_manager* d_res,
		cgbn_error_report_t* report)
	{
#if 0
		int n32 = BIT2BYTE(BITS) / 4;

		int id = blockIdx.x * blockDim.x + threadIdx.x;
		int team = (blockIdx.x * blockDim.x + threadIdx.x) / TPI;

		if (team >= col * row)
			return;

		context_t      bn_context(cgbn_report_monitor, report, team);   // construct a context
		env_t          bn_env(bn_context.env<env_t>());                     // construct an environment for 1024-bit math

		/* variant */
		env_t::cgbn_t  sub_n_max, n, n2;
		env_t::cgbn_t  nc, ns, cmp, cipher, constant, res;

		cgbn_load(bn_env, sub_n_max, &d_pub->sub_n_max);
		cgbn_load(bn_env, n, &d_pub->n);
		cgbn_load(bn_env, n2, &d_pub->n2);
		cgbn_load(bn_env, cipher, d_mat1->d_field + team);
		cgbn_load(bn_env, constant, d_mat2->d_field + team);
		cgbn_load(bn_env, res, d_res->d_field + team);

		/* todo judgement */

		eggroll_mulc_dev(&bn_env, &res, &cipher, 
			&constant, &n2, &n, &sub_n_max, &nc, &ns);
		
		d_res->d_exp[team] = d_mat1->d_exp[team] + d_mat2->d_exp[team];
		cgbn_store(bn_env, d_res->d_field + team, res);   // store r into sum

#endif
	}

	/* gpu mpz mat mul*/
	__global__ void dev_matmul_c_eql_kernel(int col1, int row1, int col2, int row2,
		cgbn_public_key* d_pub, gpu_manager* d_mat1, gpu_manager* d_mat2, gpu_manager* d_res,
		cgbn_error_report_t* report)
	{
#if 1
		int n32 = BIT2BYTE(BITS) / 4;

		int id = blockIdx.x * blockDim.x + threadIdx.x;
		int team = (blockIdx.x * blockDim.x + threadIdx.x) / TPI;
		

		//launch col1 num team		
		if (team >= row1)
			return;

		__shared__ uint64_t _exp1[4];
		__shared__ uint64_t _exp2[4];
		_exp1[team % 4] = 0;
		_exp2[team % 4] = 0;

		context_t      bn_context(cgbn_report_monitor, report, team);   // construct a context
		env_t          bn_env(bn_context.env<env_t>());                     // construct an environment for 1024-bit math

		/* variant */
		env_t::cgbn_t  sub_n_max, n, n2;
		env_t::cgbn_t  mul_tmp, add_tmp;
		env_t::cgbn_t  r, f, nc, ns, cipher, constant;

		cgbn_load(bn_env, sub_n_max, &d_pub->sub_n_max);
		cgbn_load(bn_env, n, &d_pub->n);
		cgbn_load(bn_env, n2, &d_pub->n2);
		
		cgbn_set_ui32(bn_env, add_tmp, 1);

		/* todo judgement */

		/* travel col1 */
		for (int i = 0; i < col1; i++)
		{
			/* locate offset */
			cgbn_load(bn_env, cipher, d_mat1->d_field + (i + team * col1));
			cgbn_load(bn_env, constant, d_mat2->d_field + i);

			/* do mul => [mul_tmp, _exp1] */
			eggroll_mulc_dev(&bn_env, &mul_tmp, &cipher, 
				&constant, &n2, &n, &sub_n_max, &nc, &ns);

			_exp1[team % 4] =  d_mat1->d_exp[i + team * col1] + d_mat2->d_exp[i];
			
			
			/* do add ==> [add_tmp, _exp2] */
			if (_exp1[team % 4] == _exp2[team % 4])
			{
				cgbn_mul(bn_env, r, add_tmp, mul_tmp);
				cgbn_rem(bn_env, r, r, n2);
				_exp2[team % 4] = _exp1[team % 4];
			}
			else
			{
				//aline cipher
				_exp2[team % 4] = eggroll_alinexp(&bn_env, _exp1[team % 4],
					_exp2[team % 4], &mul_tmp, &add_tmp, &n2,
					&n, &sub_n_max, &f, &nc, &ns);

				/* eggroll_add */
				cgbn_mul(bn_env, r, add_tmp, mul_tmp);
				cgbn_rem(bn_env, r, r, n2);
			}
			cgbn_add_ui32(bn_env, add_tmp, r, 0);
		}
		//test
		cgbn_store(bn_env, d_res->d_field + team, add_tmp);   // store r into sum
		d_res->d_exp[team] = _exp2[team % 4];
#endif
	}
	
	/* gpu_mng decrpyt */
	__global__ void dev_decrypt_kernel(int col, int row, cgbn_public_key* d_pub,
		cgbn_private_key* d_priv,gpu_manager* d_cipher, gpu_manager* d_plain,
		cgbn_error_report_t* report)
	{
#if 1
		int n32 = BIT2BYTE(BITS) / 4;

		int id = blockIdx.x * blockDim.x + threadIdx.x;
		int team = (blockIdx.x * blockDim.x + threadIdx.x) / TPI;

		if (team >= col * row)
			return;

		context_t      bn_context(cgbn_report_monitor, report, team);   // construct a context
		env_t          bn_env(bn_context.env<env_t>());                     // construct an environment for 1024-bit math

		/* variant */
		env_t::cgbn_t cipher, plain;
		env_t::cgbn_t b_cipher;
		env_t::cgbn_t n, hp, p, p2, hq, q, q2, qinv;
		env_t::cgbn_t r, s, u, mp, mq;

		//env_t::cgbn_wide_t r;
		cgbn_load(bn_env, n, &d_pub->n);
		cgbn_load(bn_env, hp, &d_priv->hp);
		cgbn_load(bn_env, p, &d_priv->p);
		cgbn_load(bn_env, p2, &d_priv->p2);
		cgbn_load(bn_env, hq, &d_priv->hq);
		cgbn_load(bn_env, q, &d_priv->q);
		cgbn_load(bn_env, q2, &d_priv->q2);
		cgbn_load(bn_env, qinv, &d_priv->qinver);
		cgbn_load(bn_env, cipher, d_cipher->d_field + team);
		cgbn_load(bn_env, plain, d_plain->d_field + team);

		//mp
		cgbn_sub_ui32(bn_env, r, p, 1);

		if (cgbn_compare(bn_env, cipher, p2) >= 0)
		{
			/* base > moder */
			cgbn_rem(bn_env, b_cipher, cipher, p2);
			cgbn_modular_power(bn_env, s, b_cipher, r, p2);
		}
		else
		{
			/* base < moder */
			cgbn_modular_power(bn_env, s, cipher, r, p2);
		}

		cgbn_sub_ui32(bn_env, r, s, 1);
		cgbn_div(bn_env, s, r, p);
		cgbn_mul(bn_env, r, s, hp);
		cgbn_rem(bn_env, mp, r, p);
		
		//mq
		cgbn_sub_ui32(bn_env, r, q, 1);
		if (cgbn_compare(bn_env, cipher, q2) >= 0)
		{
			/* base > moder */
			cgbn_rem(bn_env, b_cipher, cipher, q2);
			cgbn_modular_power(bn_env, s, b_cipher, r, q2);
		}
		else
		{
			/* base < moder */
			cgbn_modular_power(bn_env, s, cipher, r, q2);
		}
		cgbn_sub_ui32(bn_env, r, s, 1);
		cgbn_div(bn_env, s, r, q);
		cgbn_mul(bn_env, r, s, hq);
		cgbn_rem(bn_env, mq, r, q);

		cgbn_sub(bn_env, r, mp, mq);
		cgbn_mul(bn_env, s, r, qinv);
		cgbn_rem(bn_env, u, s, p);

		cgbn_mul(bn_env, r, u, q);
		cgbn_add(bn_env, s, mq, r);
		cgbn_rem(bn_env, plain, s, n);

		d_plain->d_exp[team] = d_cipher->d_exp[team];
		cgbn_store(bn_env, d_plain->d_field + team, plain);   // store r into sum

#endif
	}


	/* ========================= gpu call function ===================== */

	void gpu_init_pub(eggroll_public_key* h_pub, void** d_pub)
	{
		////cudaSetDevice(1);

		//printf("[host pub.g] = %d\n", h_pub->g->_mp_d[0]);
		//printf("[host pub.n] = %d\n", h_pub->n->_mp_d[0]);
		//printf("[host pub.n2] = %d\n", h_pub->n2->_mp_d[0]);
		//printf("[host pub.max] = %d\n", h_pub->max_int->_mp_d[0]);

		/* ember value */
		cgbn_public_key dev_pub_key;
		store2dev(&dev_pub_key.g, h_pub->g);
		store2dev(&dev_pub_key.n, h_pub->n);
		store2dev(&dev_pub_key.n2, h_pub->n2);
		store2dev(&dev_pub_key.max_int, h_pub->max_int);
		store2dev(&dev_pub_key.sub_n_max, h_pub->sub_n_max);
		dev_pub_key.len = BITS;

		/* pub key host2dev */
		void* tmp_pub;
		cudaMalloc((void**)& tmp_pub, sizeof(cgbn_public_key));
		cudaMemcpy(tmp_pub, &dev_pub_key, sizeof(cgbn_public_key),
			cudaMemcpyHostToDevice);
		//printf("[tmp]: d_pub = %0x\n", tmp_pub);

		/* copy gpu adress*/
		memcpy(d_pub, &tmp_pub, sizeof(void*));
		//printf("[tmp after]: d_pub = %0x\n", *d_pub);

	}

	void gpu_init_priv(eggroll_private_key* h_priv, void** d_priv)
	{
		//printf("[host pub.g] = %d\n", h_priv->hp->_mp_d[0]);

		////cudaSetDevice(1);

		/* ember value */
		cgbn_private_key cgbn_priv;
		store2dev(&cgbn_priv.hp, h_priv->hp);
		store2dev(&cgbn_priv.hq, h_priv->hq);
		store2dev(&cgbn_priv.p, h_priv->p);
		store2dev(&cgbn_priv.q, h_priv->q);
		store2dev(&cgbn_priv.p2, h_priv->p2);
		store2dev(&cgbn_priv.q2, h_priv->q2);
		store2dev(&cgbn_priv.qinver, h_priv->qinver);
		cgbn_priv.len = BITS;

		/* priv key host2dev */
		void* tmp_priv;
		cudaMalloc((void**)& tmp_priv, sizeof(cgbn_private_key));
		cudaMemcpy(tmp_priv, &cgbn_priv, sizeof(cgbn_private_key),
			cudaMemcpyHostToDevice);
		
		/* copy gpu adress*/
		memcpy(d_priv, &tmp_priv, sizeof(void*));
	}

	void load_devPub(eggroll_public_key* h_pub, void** d_pub)
	{
		/* pub key Dev2host */
		cgbn_public_key h_cgbn_pub;
		cudaMemcpy(&h_cgbn_pub, *d_pub, sizeof(cgbn_public_key),
			cudaMemcpyDeviceToHost);

		/* load value */
		store2gmp(h_pub->g, &h_cgbn_pub.g);
		store2gmp(h_pub->n, &h_cgbn_pub.n);
		store2gmp(h_pub->n2, &h_cgbn_pub.n2);
		store2gmp(h_pub->max_int, &h_cgbn_pub.max_int);
		store2gmp(h_pub->sub_n_max, &h_cgbn_pub.sub_n_max);
		h_pub->len = BITS;

	}

	void load_devPriv(eggroll_private_key* h_priv, void** d_priv)
	{
		cgbn_private_key h_cgbn_priv;
		cudaMemcpy(&h_cgbn_priv, *d_priv, sizeof(cgbn_public_key),
			cudaMemcpyDeviceToHost);

		/* ember value */
		cgbn_private_key cgbn_priv;
		store2gmp(h_priv->hp, &cgbn_priv.hp);
		store2gmp(h_priv->hq, &cgbn_priv.hq);
		store2gmp(h_priv->p, &cgbn_priv.p);
		store2gmp(h_priv->q, &cgbn_priv.q);
		store2gmp(h_priv->p2, &cgbn_priv.p2);
		store2gmp(h_priv->q2, &cgbn_priv.q2);
		store2gmp(h_priv->qinver, &cgbn_priv.qinver);
		cgbn_priv.len = BITS;
	}

	void gpu_init_mpz(int col, int row, mpz_manager* c_mng, void** d_buf)
	{
		int i, j;
		int nByte = BIT2BYTE(BITS);
		int count = col * row;
		mpz_t tmp;
		mpz_init(tmp);

		///* init gpu_mng*/
		gpu_manager* d_mng;
		cudaMalloc((void**)& d_mng, sizeof(gpu_manager));
		//initGpuManager(d_mng, col, row);

		/* malloc contain */
		gpu_manager* h_contain = (gpu_manager*)malloc(sizeof(gpu_manager));
		
		h_contain->col = col;
		h_contain->row = row;
		h_contain->ifenc = 0;
		h_contain->ifmal = 1;
		
		cudaMalloc((void**)& h_contain->d_field, sizeof(gpu_mpz) * col * row);
		cudaMalloc((void**)& h_contain->d_exp, sizeof(uint64_t) * col * row);

		/* field host2dev */
		for (i = 0; i < col * row; i++)
		{
			//gmp_printf("[XXXXXgpu init ] : %Zd\n", c_mng->field[i]);
			cudaMemcpy(h_contain->d_field[i]._limbs, c_mng->field[i]->_mp_d,
				nByte, cudaMemcpyHostToDevice);
		}
		/* exp host2dev */
		cudaMemcpy(h_contain->d_exp, c_mng->exp,
			sizeof(uint64_t) * col * row, cudaMemcpyHostToDevice);

		/* contain host2dev*/
		cudaMemcpy(d_mng, h_contain, sizeof(gpu_manager), cudaMemcpyHostToDevice);

		/* set return */
		memcpy(d_buf, &d_mng, sizeof(void*));

		/* free contain*/
		free(h_contain);

		if (0)
		{
			gpu_manager* _tmp;
			cudaMallocHost((void**)& _tmp, sizeof(gpu_manager));
			cudaMemcpy(_tmp, *d_buf, sizeof(gpu_manager), cudaMemcpyDeviceToHost);

			/* load base */
			mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, BITS);
			uint64_t* _e = (uint64_t*)malloc(sizeof(uint64_t) * col * row);
			
			/* exp dev2host*/
			cudaMemcpy(_e, _tmp->d_exp, sizeof(uint64_t) * col * row, cudaMemcpyDeviceToHost);

			for (i = 0; i < col * row; i++)
			{
				cudaMemcpy(_t->_mp_d, _tmp->d_field[i]._limbs, nByte, cudaMemcpyDeviceToHost);
				/*for (int i = 0; i < _t->_mp_alloc; i++)
				{
					printf("[gpu init ] ==%d<<%ld\n", i, _t->_mp_d[i]);
				}*/

				for (int j = _t->_mp_alloc - 2; j >= 0; j--)
				{
					if (_t->_mp_d[j] != (uint64_t)0)
					{
						_t->_mp_size = j + 1;
						printf("++++++++++++-------%d\n", j);
						break;
					}
				}

				printf("[gpu init ]_t alloc %d\n", _t->_mp_alloc);
				printf("[gpu init ] _t size%d\n", _t->_mp_size);
				gmp_printf("[gpu init++] _t = %Zd\n", _t);

				///* load exp */
				printf("[gpu init ] exp = %ld\n", _e[i]);

			}


		}

	}

	void gpu_init_mpz_d(int col, int row, float64_t* c_buf, void** d_buf)
	{

		//cudaSetDevice(1);
		printf("=======================\n");

		int i, j;
		int nByte = BIT2BYTE(BITS);
		int count = col * row;
		mpz_t tmp;
		mpz_init(tmp);

		/* malloc gpu_mng*/
		gpu_manager* d_mng = (gpu_manager*)malloc(sizeof(gpu_manager));
		initGpuManager(d_mng, col, row);

		/* init */
		//dev_encode_kernel_d<<<>>>()

		/* init field*/
		memcpy(d_buf, &d_mng, sizeof(void*));

	}
	
	void gpu_encrypt(void** d_pub, void** d_buf, void** d_enc_buf)
	{
		////cudaSetDevice(1);
		int i, j;
		int nByte = BIT2BYTE(BITS);
		int col, row;

		cgbn_error_report_t* report;
		cgbn_error_report_alloc(&report);

		/* load d_buf */
		gpu_manager* h_buf;
		cudaMallocHost((void**)& h_buf, sizeof(gpu_manager));
		cudaMemcpy(h_buf, *d_buf, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col = h_buf->col;
		row = h_buf->row;
		
		///* malloc d_enc_buf */
		gpu_manager* _enc_buf;
		cudaMalloc((void**)& _enc_buf, sizeof(gpu_manager));
		initGpuManager(_enc_buf, col, row);

		/* calc thread num*/
		int block = (col * row + 3) / 4;
		int thread = 128;
		dev_encrpyt_kernel << <block, thread >> > (col, row, (cgbn_public_key*)(*d_pub),
			(gpu_manager*)(*d_buf), _enc_buf, report);
		cudaDeviceSynchronize();

		memcpy(d_enc_buf, &_enc_buf, sizeof(void*));

		cudaFreeHost(h_buf);

		/* test */

		if (0)
		{
			gpu_manager* _tmp;
			cudaMallocHost((void**)& _tmp, sizeof(gpu_manager));
			cudaMemcpy(_tmp, *d_enc_buf, sizeof(gpu_manager), cudaMemcpyDeviceToHost);

			/* load base */
			mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, BITS);
			uint64_t* _e = (uint64_t*)malloc(sizeof(uint64_t) * col * row);

			/* exp dev2host*/
			cudaMemcpy(_e, _tmp->d_exp, sizeof(uint64_t) * col * row, cudaMemcpyDeviceToHost);

			for (i = 0; i < col * row; i++)
			{
				cudaMemcpy(_t->_mp_d, _tmp->d_field[i]._limbs, nByte, cudaMemcpyDeviceToHost);
				/*for (int i = 0; i < _t->_mp_alloc; i++)
				{
					printf("[gpu init ] ==%d<<%ld\n", i, _t->_mp_d[i]);
				}*/

				for (int j = _t->_mp_alloc - 2; j >= 0; j--)
				{
					if (_t->_mp_d[j] != (uint64_t)0)
					{
						_t->_mp_size = j + 1;
						printf("++++++++++++-------%d\n", j);
						break;
					}
				}

				printf("[gpu encrypt ]_t alloc %d\n", _t->_mp_alloc);
				printf("[gpu encrypt ] _t size%d\n", _t->_mp_size);
				gmp_printf("[gpu encrypt++] _t = %Zd\n", _t);

				///* load exp */
				printf("[gpu encrypt ] exp = %ld\n", _e[i]);

			}
		}
	}

	int gpu_dotadd(void** d_pub, void** d_mat1, void** d_mat2, void** d_res)
	{
		int i, j;
		int nByte = BIT2BYTE(BITS);
		int col1, row1, enc1;
		int col2, row2, enc2;

		cgbn_error_report_t* report;
		cgbn_error_report_alloc(&report);

		/* load d_mat1 */
		gpu_manager* h_mat1;
		cudaMallocHost((void**)& h_mat1, sizeof(gpu_manager));
		cudaMemcpy(h_mat1, *d_mat1, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col1 = h_mat1->col;
		row1 = h_mat1->row;
		enc1 = h_mat1->ifenc;

		/* load d_mat2 */
		gpu_manager* h_mat2;
		cudaMallocHost((void**)& h_mat2, sizeof(gpu_manager));
		cudaMemcpy(h_mat2, *d_mat2, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col2 = h_mat2->col;
		row2 = h_mat2->row;
		enc2 = h_mat2->ifenc;

		if (col1 != col2 || row1 != row2
			|| enc1 != 1 || enc2 != 1)
		{
			printf("[c1 r1 enc1] = [%d %d %d]\n",
				h_mat1->col, h_mat1->row, h_mat1->ifenc);
			printf("[c2 r2 enc21] = [%d %d %d]\n",
				h_mat2->col, h_mat2->row, h_mat2->ifenc);

			return -1;
		}

		/* malloc d_enc_buf */
		gpu_manager* _d_res;
		cudaMalloc((void**)& _d_res, sizeof(gpu_manager));
		initGpuManager(_d_res, col1, row1);

		///* calc thread num */
		int block = (col1 * row1 + 3) / 4;
		int thread = 128;

		dev_dotadd_kernel << <block, thread >> > (col1, row1, (cgbn_public_key*)(*d_pub),
			(gpu_manager*)(*d_mat1), (gpu_manager*)(*d_mat2), _d_res, report);
		cudaDeviceSynchronize();
		
		memcpy(d_res, &_d_res, sizeof(void*));

		cudaFreeHost(h_mat1);
		cudaFreeHost(h_mat2);


		//* test */
		if (0)
		{
			gpu_manager* _tmp;
			cudaMallocHost((void**)& _tmp, sizeof(gpu_manager));
			cudaMemcpy(_tmp, *d_mat1, sizeof(gpu_manager), cudaMemcpyDeviceToHost);

			/* load base */
			mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, BITS);
			uint64_t* _e = (uint64_t*)malloc(sizeof(uint64_t) * col1 * row1);

			/* exp dev2host*/
			cudaMemcpy(_e, _tmp->d_exp, sizeof(uint64_t) * col1 * row1, cudaMemcpyDeviceToHost);

			for (i = 0; i < col1 * row1; i++)
			{
				cudaMemcpy(_t->_mp_d, _tmp->d_field[i]._limbs, nByte, cudaMemcpyDeviceToHost);
				/*for (int i = 0; i < _t->_mp_alloc; i++)
				{
					printf("[gpu init ] ==%d<<%ld\n", i, _t->_mp_d[i]);
				}*/

				for (int j = _t->_mp_alloc - 2; j >= 0; j--)
				{
					if (_t->_mp_d[j] != (uint64_t)0)
					{
						_t->_mp_size = j + 1;
						printf("++++++++++++-------%d\n", j);
						break;
					}
				}

				printf("[gpu dotadd]_t alloc %d\n", _t->_mp_alloc);
				printf("[gpu dotadd ] _t size%d\n", _t->_mp_size);
				gmp_printf("[gpu dotadd] _t = %Zd\n", _t);

				///* load exp */
				printf("[gpu dotadd] exp = %ld\n", _e[i]);

			}


		}

		return 1;

	}

	int gpu_dotmul(void** d_pub, void** d_mat1, void** d_mat2, void** d_res)
	{
		int i, j;
		int nByte = BIT2BYTE(BITS);
		int col1, row1, enc1;
		int col2, row2, enc2;

		cgbn_error_report_t* report;
		cgbn_error_report_alloc(&report);

		/* load d_mat1 */
		gpu_manager* h_mat1;
		cudaMallocHost((void**)& h_mat1, sizeof(gpu_manager));
		cudaMemcpy(h_mat1, *d_mat1, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col1 = h_mat1->col;
		row1 = h_mat1->row;
		enc1 = h_mat1->ifenc;

		/* load d_mat2 */
		gpu_manager* h_mat2;
		cudaMallocHost((void**)& h_mat2, sizeof(gpu_manager));
		cudaMemcpy(h_mat2, *d_mat2, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col2 = h_mat2->col;
		row2 = h_mat2->row;
		enc2 = h_mat2->ifenc;

		if (col1 != col2 || row1 != row2
			|| enc1 != 1 || enc2 != 0)
		{
			printf("[c1 r1 enc1] = [%d %d %d]\n",
				h_mat1->col, h_mat1->row, h_mat1->ifenc);
			printf("[c2 r2 enc21] = [%d %d %d]\n",
				h_mat2->col, h_mat2->row, h_mat2->ifenc);

			return -1;
		}

		/* malloc d_enc_buf */
		gpu_manager* _d_res;
		cudaMalloc((void**)& _d_res, sizeof(gpu_manager));
		initGpuManager(_d_res, col1, row1);

		/* calc thread num */
		int block = (col1 * row1 + 3) / 4;
		int thread = 128;
		dev_dotmul_kernel <<<block, thread >>> (col1, row1, (cgbn_public_key*)(*d_pub),
			(gpu_manager*)(*d_mat1), (gpu_manager*)(*d_mat2), _d_res, report);
		cudaDeviceSynchronize();

		memcpy(d_res, &_d_res, sizeof(void*));

		cudaFreeHost(h_mat1);
		cudaFreeHost(h_mat2);


		//* test */
		if (0)
		{
			gpu_manager* _tmp;
			cudaMallocHost((void**)& _tmp, sizeof(gpu_manager));
			cudaMemcpy(_tmp, *d_res, sizeof(gpu_manager), cudaMemcpyDeviceToHost);

			/* load base */
			mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, BITS);
			uint64_t* _e = (uint64_t*)malloc(sizeof(uint64_t) * col1 * row1);

			/* exp dev2host*/
			cudaMemcpy(_e, _tmp->d_exp, sizeof(uint64_t) * col1 * row1, cudaMemcpyDeviceToHost);

			for (i = 0; i < col1 * row1; i++)
			{
				cudaMemcpy(_t->_mp_d, _tmp->d_field[i]._limbs, nByte, cudaMemcpyDeviceToHost);
				/*for (int i = 0; i < _t->_mp_alloc; i++)
				{
					printf("[gpu init ] ==%d<<%ld\n", i, _t->_mp_d[i]);
				}*/

				for (int j = _t->_mp_alloc - 2; j >= 0; j--)
				{
					if (_t->_mp_d[j] != (uint64_t)0)
					{
						_t->_mp_size = j + 1;
						printf("++++++++++++-------%d\n", j);
						break;
					}
				}

				printf("[gpu dotmul ]_t alloc %d\n", _t->_mp_alloc);
				printf("[gpu dotmul ] _t size%d\n", _t->_mp_size);
				gmp_printf("[gpu dotmul++] _t = %Zd\n", _t);

				///* load exp */
				printf("[gpu dotmul ] exp = %ld\n", _e[i]);

			}

		}
		return 1;

	}

	int gpu_matmul_c_eql(void** d_pub, void** d_mat1, void** d_mat2, void** d_res)
	{
		int i, j;
		int nByte = BIT2BYTE(BITS);
		int col1, row1, enc1;
		int col2, row2, enc2;

		cgbn_error_report_t* report;
		cgbn_error_report_alloc(&report);

		/* load d_mat1 */
		gpu_manager* h_mat1;
		cudaMallocHost((void**)& h_mat1, sizeof(gpu_manager));
		cudaMemcpy(h_mat1, *d_mat1, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col1 = h_mat1->col;
		row1 = h_mat1->row;
		enc1 = h_mat1->ifenc;

		/* load d_mat2 */
		gpu_manager* h_mat2;
		cudaMallocHost((void**)& h_mat2, sizeof(gpu_manager));
		cudaMemcpy(h_mat2, *d_mat2, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col2 = h_mat2->col;
		row2 = h_mat2->row;
		enc2 = h_mat2->ifenc;

		/* mat mul require row_eql*/
		if (col1 != col2 || enc1 != 1 || enc2 != 0)
		{
			printf("[c1 r1 enc1] = [%d %d %d]\n",
				h_mat1->col, h_mat1->row, h_mat1->ifenc);
			printf("[c2 r2 enc21] = [%d %d %d]\n",
				h_mat2->col, h_mat2->row, h_mat2->ifenc);

			return -1;
		}

		/* malloc d_enc_buf */
		gpu_manager* _d_res;
		cudaMalloc((void**)& _d_res, sizeof(gpu_manager));
		initGpuManager(_d_res, row1, row2);

		/* calc thread num */
		int block = (row1 + 3) / 4;
		int thread = 128;
		dev_matmul_c_eql_kernel <<<block, thread >>> (col1, row1, col2, row2, (cgbn_public_key*)(*d_pub),
			(gpu_manager*)(*d_mat1), (gpu_manager*)(*d_mat2), _d_res, report);
		cudaDeviceSynchronize();

		memcpy(d_res, &_d_res, sizeof(void*));

		cudaFreeHost(h_mat1);
		cudaFreeHost(h_mat2);


		//* test */
		if (0)
		{
			gpu_manager* _tmp;
			cudaMallocHost((void**)& _tmp, sizeof(gpu_manager));
			cudaMemcpy(_tmp, *d_res, sizeof(gpu_manager), cudaMemcpyDeviceToHost);

			/* load base */
			mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, BITS);
			uint64_t* _e = (uint64_t*)malloc(sizeof(uint64_t) * row1);

			/* exp dev2host*/
			cudaMemcpy(_e, _tmp->d_exp, sizeof(uint64_t) * row1, cudaMemcpyDeviceToHost);

			for (i = 0; i < row1; i++)
			{
				cudaMemcpy(_t->_mp_d, _tmp->d_field[i]._limbs, nByte, cudaMemcpyDeviceToHost);
				/*for (int i = 0; i < _t->_mp_alloc; i++)
				{
					printf("[gpu init ] ==%d<<%ld\n", i, _t->_mp_d[i]);
				}*/

				for (int j = _t->_mp_alloc - 2; j >= 0; j--)
				{
					if (_t->_mp_d[j] != (uint64_t)0)
					{
						_t->_mp_size = j + 1;
						printf("++++++++++++-------%d\n", j);
						break;
					}
				}

				printf("[gpu dotmul ]_t alloc %d\n", _t->_mp_alloc);
				printf("[gpu dotmul ] _t size%d\n", _t->_mp_size);
				gmp_printf("[gpu dotmul++] _t = %Zd\n", _t);

				///* load exp */
				printf("[gpu dotmul ] exp = %ld\n", _e[i]);

			}

		}
		return 1;

	}

	void gpu_decrypt(void** d_pub, void** d_priv, void** d_cipher,
		void** d_plain)
	{
		int i, j;
		int nByte = BIT2BYTE(BITS);
		int col, row, enc;

		cgbn_error_report_t* report;
		cgbn_error_report_alloc(&report);

		/* load d_cipher */
		gpu_manager* h_cipher;
		cudaMallocHost((void**)& h_cipher, sizeof(gpu_manager));
		cudaMemcpy(h_cipher, *d_cipher, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col = h_cipher->col;
		row = h_cipher->row;
		enc = h_cipher->ifenc;

		///* malloc d_enc_buf */
		gpu_manager* _d_plain;
		cudaMalloc((void**)& _d_plain, sizeof(gpu_manager));
		initGpuManager(_d_plain, col, row);

		/* calc thread num */
		int block = (row * col + 3) / 4;
		int thread = 128;
		dev_decrypt_kernel << <block, thread >> > (col, row, (cgbn_public_key*)(*d_pub),
			(cgbn_private_key*)(*d_priv), (gpu_manager*)(*d_cipher), _d_plain, report);
		cudaDeviceSynchronize();

		memcpy(d_plain, &_d_plain, sizeof(void*));

		cudaFreeHost(h_cipher);

		/* test */
		if (0)
		{
			gpu_manager* _tmp;
			cudaMallocHost((void**)& _tmp, sizeof(gpu_manager));
			cudaMemcpy(_tmp, *d_plain, sizeof(gpu_manager), cudaMemcpyDeviceToHost);

			/* load base */
			mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, BITS);
			uint64_t* _e = (uint64_t*)malloc(sizeof(uint64_t) * col * row);

			/* exp dev2host*/
			cudaMemcpy(_e, _tmp->d_exp, sizeof(uint64_t) * col * row, cudaMemcpyDeviceToHost);

			for (i = 0; i < col * row; i++)
			{
				cudaMemcpy(_t->_mp_d, _tmp->d_field[i]._limbs, nByte, cudaMemcpyDeviceToHost);
				/*for (int i = 0; i < _t->_mp_alloc; i++)
				{
					printf("[gpu init ] ==%d<<%ld\n", i, _t->_mp_d[i]);
				}*/

				for (int j = _t->_mp_alloc - 2; j >= 0; j--)
				{
					if (_t->_mp_d[j] != (uint64_t)0)
					{
						_t->_mp_size = j + 1;
						printf("++++++++++++-------%d\n", j);
						break;
					}
				}

				printf("[gpu decrypt ]_t alloc %d\n", _t->_mp_alloc);
				printf("[gpu decrypt ] _t size%d\n", _t->_mp_size);
				gmp_printf("[gpu decrypt++] _t = %Zd\n", _t);

				///* load exp */
				printf("[gpu decrypt ] exp = %ld\n", _e[i]);

			}



		}

	}

	void gpu_decode(void** d_pub, void** d_plain, mpz_manager* h_mng)
	{
		int i, j;
		int nByte = BIT2BYTE(BITS);
		int col, row, enc;

		/* load d_cipher */
		gpu_manager* h_plain;
		cudaMallocHost((void**)& h_plain, sizeof(gpu_manager));
		cudaMemcpy(h_plain, *d_plain, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		col = h_plain->col;
		row = h_plain->row;
		enc = h_plain->ifenc;

		/* malloc h_mng */
		initManager_d(h_mng, col, row, BITS);
		h_mng->ifenc = 0;

		/* field dev2host */
		for (int i = 0; i < col * row; i++)
		{
			cudaMemcpy(h_mng->field[i]->_mp_d, h_plain->d_field[i]._limbs, 
				nByte, cudaMemcpyDeviceToHost);

			for (int j = h_mng->field[i]->_mp_alloc - 2; j >= 0; j--)
			{
				if (h_mng->field[i]->_mp_d[j] != (uint64_t)0)
				{
					h_mng->field[i]->_mp_size = j + 1;
					printf("++++++++++++-------%d\n", j);
					break;
				}
			}
		}

		/* exp dev2host */
		cudaMemcpy(h_mng->exp, h_plain->d_exp, 
			col * row * sizeof(uint64_t), cudaMemcpyDeviceToHost);
	}

	/* dump */
	int gpu_dump(void** d_mng, void** h_dumpMap)
	{
		int i, j;
		int nByte = BIT2BYTE(BITS);
		int limbs = nByte / sizeof(uint64_t);
		int col, row;
		int enc, mal;

		/* malloc h_contain */
		gpu_manager* h_mng;
		cudaMallocHost((void**)& h_mng, sizeof(gpu_manager));
		cudaMemcpy(h_mng, *d_mng, sizeof(gpu_manager), cudaMemcpyDeviceToHost);
		cudaMemcpy(&col, &h_mng->col, sizeof(int), cudaMemcpyDeviceToHost);
		cudaMemcpy(&row, &h_mng->row, sizeof(int), cudaMemcpyDeviceToHost);
		cudaMemcpy(&enc, &h_mng->ifenc, sizeof(int), cudaMemcpyDeviceToHost);

		//printf("[gpu dump]:col = %d row = %d enc = %d\n", h_mng->col, h_mng->row, h_mng->ifenc);

		///* malloc byte */
		short headerSize = 4;
		long int dumpSize = (row * col * limbs + headerSize) + (row * col);
		*h_dumpMap = (uint64_t*)malloc(dumpSize * sizeof(uint64_t));

		/////* load header */
		uint64_t* iter = (uint64_t*)(*h_dumpMap);

		iter[0] = (uint64_t)row;
		iter[1] = (uint64_t)col;
		iter[2] = (uint64_t)BITS;
		iter[3] = (uint64_t)enc;

		///* load field */
		iter = ((uint64_t*)(*h_dumpMap)) + headerSize;
		cudaMemcpy(iter, h_mng->d_field, nByte * col * row,
			cudaMemcpyDeviceToHost);

		///* load exp */
		iter = ((uint64_t*)(*h_dumpMap)) + headerSize + row * col * limbs;
		cudaMemcpy(iter, h_mng->d_exp, sizeof(uint64_t) * col * row,
			cudaMemcpyDeviceToHost);

		if (0)
		{
			/* host memory */
			gpu_manager* _tmp;
			cudaMallocHost((void**)& _tmp, sizeof(gpu_manager));

			/* fill value 2 host */
			cudaMemcpy(_tmp, *d_mng, sizeof(gpu_manager), cudaMemcpyDeviceToHost);

			/* load base */
			mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, BITS);
			uint64_t _e;

			for (i = 0; i < col * row; i++)
			{
				cudaMemcpy(_t->_mp_d, _tmp->d_field[i]._limbs, nByte, cudaMemcpyDeviceToHost);

				for (int j = _t->_mp_alloc - 2; j >= 0; j--)
				{
					if (_t->_mp_d[j] != (uint64_t)0)
					{
						_t->_mp_size = j + 1;
						printf("++++++++++++-------%d\n", j);
						break;
					}
				}

				printf("[gpu dump ]_t alloc %d\n", _t->_mp_alloc);
				printf("[gpu dump ] _t size%d\n", _t->_mp_size);
				gmp_printf("[...........gpu dump++]%Zd\n", _t);

				/* load exp */
				cudaMemcpy(&_e, _tmp->d_exp, sizeof(uint64_t), cudaMemcpyDeviceToHost);
				printf("[gpu dump ] exp = %ld\n", _e);
			}
		}


		return dumpSize;

	}

	int gpu_load(void** d_mng, uint64_t* h_data)
	{
		int i, j;
		int col, row, bit, enc;
		int nByte = BIT2BYTE(BITS);
		int limbs = nByte / sizeof(uint64_t);
		short headerSize = 4;

		/* load header */
		row = (int)h_data[0];
		col = (int)h_data[1];
		bit = (int)h_data[2];
		enc = (int)h_data[3];
		
		if (bit != BITS)
			return -1;

		/* device memcory */
		gpu_manager* _d_mng;
		cudaMalloc((void**)& _d_mng, sizeof(gpu_manager));

		/* host memory */
		gpu_manager* h_contain;
		cudaMallocHost((void**)&h_contain, sizeof(gpu_manager));
		h_contain->col = col;
		h_contain->row = row;
		h_contain->ifenc = enc;
		h_contain->ifmal = 1;
		cudaMalloc((void**)& h_contain->d_field, sizeof(gpu_mpz) * col * row);
		cudaMalloc((void**)& h_contain->d_exp, sizeof(uint64_t) * col * row);
		

		printf("[gpu load] col = %d row = %d enc = %d bit = %d\n", 
			col, row, enc, bit);

		/* load field */
		uint64_t* iter ;
		for  (i = 0; i < col * row; i++)
		{
			iter = h_data + headerSize + i * limbs;
			cudaMemcpy(h_contain->d_field[i]._limbs, iter, nByte,
				cudaMemcpyHostToDevice);
		}

		/* load exp */
		iter = h_data + headerSize + row * col * limbs;
		cudaMemcpy(h_contain->d_exp, iter, 
			sizeof(uint64_t) * col * row, cudaMemcpyHostToDevice);	

		/* fill value 2 device */
		cudaMemcpy(_d_mng, h_contain, sizeof(gpu_manager), cudaMemcpyHostToDevice);

		/* fill addr 2 host */
		memcpy(d_mng, &_d_mng, sizeof(void*));

		cudaFreeHost(h_contain);
		
		if (0)
		{
			/* host memory */
			gpu_manager* _tmp;
			cudaMallocHost((void**)& _tmp, sizeof(gpu_manager));

			/* fill value 2 host */
			cudaMemcpy(_tmp, *d_mng, sizeof(gpu_manager), cudaMemcpyDeviceToHost);

			/* load base */
			mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, BITS);
			uint64_t _e;

			for (i = 0; i < col * row; i++)
			{
				cudaMemcpy(_t->_mp_d, _tmp->d_field[i]._limbs, nByte, cudaMemcpyDeviceToHost);

				for (int j = _t->_mp_alloc - 2; j >= 0; j--)
				{
					if (_t->_mp_d[j] != (uint64_t)0)
					{
						_t->_mp_size = j + 1;
						printf("++++++++++++-------%d\n", j);
						break;
					}
				}
				printf("[gpu load ]_t alloc %d\n", _t->_mp_alloc);
				printf("[gpu load ] _t size%d\n", _t->_mp_size);
				gmp_printf("[...........gpu load++]%Zd\n", _t);

				/* load exp */
				cudaMemcpy(&_e, &(_tmp->d_exp[i]), sizeof(uint64_t), cudaMemcpyDeviceToHost);
				printf("[gpu load ] exp = %ld\n", _e);
			}
		}

		return 1;
	}

	/* tool test*/
	__global__ void dev_pub_kernel(cgbn_public_key* pub)
	{
		printf("kernel here len++++++++++++++ \n");
		printf("pub.g = %d\n", pub->g._limbs[0]);
		printf("pub.n = %d\n", pub->n._limbs[0]);
		printf("pub.n2 = %d\n", pub->n2._limbs[0]);
		printf("pub.max = %d\n", pub->max_int._limbs[0]);


		int exp;
		double base = frexp(3.121441f, &exp);
		printf("3.121441 :exp = %d base = %lf\n", exp, base);

	}
	__global__ void dev_priv_kernel(cgbn_private_key* priv)
	{
		printf("kernel here len++++++++++++++ \n");
		printf("priv.p =  %d\n", priv->p._limbs[0]);
	}
	__global__ void dev_mpz_kernel(gpu_mpz* d_data)
	{
		
	}

	void gpu_show(void** dev_ptr, int n)
	{
		if (n == 0)
		{
			dev_pub_kernel << <1, 1 >> > ((cgbn_public_key*)(*dev_ptr));
			cudaDeviceSynchronize();
		}
		
		if (n == 1)
		{
		/*	mpz_t _t;
			mpz_init(_t);
			mpz_setbit(_t, 2048);

			cgbn_public_key _p;
			cudaMemcpy(&_p, *dev_ptr, sizeof(cgbn_public_key), cudaMemcpyDeviceToHost);
			
			store2gmp(_t, &_p.g);*/
			dev_priv_kernel << <1, 1 >> > ((cgbn_private_key*)(*dev_ptr));
			cudaDeviceSynchronize();
		}
		
		if (n == 2)
		{
			dev_mpz_kernel << <1, 1 >> > ((gpu_mpz*)(*dev_ptr));
			cudaDeviceSynchronize();

		}

	}

}