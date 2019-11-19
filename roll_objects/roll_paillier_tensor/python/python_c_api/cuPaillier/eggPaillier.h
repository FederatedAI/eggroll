#pragma once
#ifdef __linux
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <cuda.h>
#include <time.h> 
#include <sys/time.h>
#include <assert.h>

#include <gmp.h>
#include "../paillier.h"
//#include "cgbn/cgbn.h"
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

static int divUp(int total, int grain)
{
	return(total + grain - 1) / grain;
}

/* ===================== egg paillier ================================ */
void gpu_init_pub(eggroll_public_key* h_pub, void** d_pub);

void gpu_init_priv(eggroll_private_key* h_priv, void** d_priv);

void load_devPub(eggroll_public_key* h_pub, void** d_pub);

void load_devPriv(eggroll_private_key* h_priv, void** d_priv);

void gpu_init_mpz(int col, int row, mpz_manager* c_mng, void** d_buf);

void gpu_encrypt( void** d_pub, void** d_buf, void** d_enc_buf);


//void gpu_dotadd(int col, int row, void** d_pub, void** d_mat1,
//	void** d_mat2, void** d_res);

int gpu_dotadd(void** d_pub, void** d_mat1, void** d_mat2, void** d_res);

int gpu_dotmul(void** d_pub, void** d_mat1, void** d_mat2, void** d_res);

int gpu_matmul_c_eql(void** d_pub, void** d_mat1, void** d_mat2, void** d_res);

//
//void gpu_decrypt(int col, int row, void** d_pub, void** d_priv,
//	void** d_ciper, void** d_plain);

void gpu_decrypt(void** d_pub, void** d_priv, void** d_ciper,
	void** d_plain);

void gpu_decode(void** d_pub, void** d_plain, mpz_manager* h_mng);

/*static inline*/
void gpu_show(void** dev_ptr, int n);

/* ======================= about multi process ==============================*/

int gpu_dump(void** d_mng, void** h_dumpMap);

int gpu_load(void** d_mng, uint64_t* h_data);

int gpu_load2(void** d_mng, uint64_t* h_data);

/* ======================= key interface ==============================*/

