#pragma once
#include "paillier.h"
#include "tools.h"
#include <time.h>

static eggroll_public_key pub;
static eggroll_private_key priv;

void eggroll_keygen(eggroll_public_key* pub, eggroll_private_key* priv, unsigned long int Len)
{
	mpz_t n, p, q, n2, temp, mask, g;
	mpz_t rnd;

	mpz_init(n);
	mpz_init(p);
	mpz_init(q);
	mpz_init(n2);
	mpz_init(temp);
	mpz_init(mask);
	mpz_init(g);
	mpz_init(rnd);

	//write bit lengths
	priv->len = Len;
	pub->len = Len;

	int len = 0;
	int time = 0;
	while (len != Len /*&& time < 10*/)
	{
		//printf("+++++++++++len = %d\n", Len);

		gen_prime(p, Len / 2);
		mpz_set(q, p);
		
		//gmp_printf("[time] q = %Zd\n", q);

		//gmp_printf("[time] cmp = %Zd\n", mpz_cmp(q, p));

		while (mpz_cmp(q, p) == 0)
		{
			gen_prime(q, Len / 2);
		}
		mpz_mul(n, p, q);
		len = n->_mp_size;
		time++;
	}


	printf("time  = %d \nlen = %d\n",time, Len);
	gmp_printf("n = %Zd\n", n);
	gmp_printf("p = %Zd\n", p);
	gmp_printf("q = %Zd\n", q);

}

void eggroll_public_init(eggroll_public_key* pub)
{
	mpz_init(pub->g);
	mpz_init(pub->max_int);
	mpz_init(pub->n);
	mpz_init(pub->n2);
	mpz_init(pub->sub_n_max);
	pub->len = 0;

}

void eggroll_private_init(eggroll_private_key* priv)
{
	mpz_init(priv->hp);
	mpz_init(priv->hq);
	mpz_init(priv->p);
	mpz_init(priv->p2);
	mpz_init(priv->q);
	mpz_init(priv->q2);
	mpz_init(priv->qinver);
	priv->len;
}

void eggroll_public_free(eggroll_public_key* pub)
{
	mpz_clear(pub->g);
	mpz_clear(pub->max_int);
	mpz_clear(pub->n);
	mpz_clear(pub->n2);
	mpz_clear(pub->sub_n_max);
	pub->len = 0;

}

void eggroll_private_free(eggroll_private_key* priv)
{
	mpz_clear(priv->hp);
	mpz_clear(priv->hq);
	mpz_clear(priv->p);
	mpz_clear(priv->p2);
	mpz_clear(priv->q);
	mpz_clear(priv->q2);
	mpz_clear(priv->qinver);
	priv->len = 0;
}

void h_func(mpz_t h, mpz_t x, mpz_t x2, eggroll_public_key* pub)
{
	mpz_t r, s;
	mpz_init(r);
	mpz_init(s);

	mpz_sub_ui(r, x, 1);
	mpz_powm(s, pub->g, r, x2);

	/* l(x, p)  = (x - 1) // p */
	mpz_sub_ui(s, s, 1);
	mpz_div(r, s, x);

	/* hx*/
	mpz_invert(h, r, x);

	mpz_clear(r);
	mpz_clear(s);

}

void eggroll_keygen_stable(eggroll_public_key* pub, 
	eggroll_private_key* priv, unsigned long int Len)
{
	mpz_t tmp;
	mpz_init(tmp);

	/* gen public init*/

	pub->len = (int)1024;
	mpz_set_str(pub->n, "11066832210114262232103654280316748398195137103\
		293302944158199855719657993677085802321018617705887244523881\
		975051849983624870020407471474440738214879508278446180816131\
		080093353087843103480346753134163269651882519056833834286248\
		290130485567760239998694699231304159991428676937050377591557\
		4121377943523766559823", 10);
	
	mpz_add_ui(pub->g, pub->n, 1);
	mpz_mul(pub->n2, pub->n, pub->n);
	mpz_div_ui(tmp, pub->n, 3);
	mpz_sub_ui(pub->max_int, tmp, 1);
	mpz_sub(pub->sub_n_max, pub->n, pub->max_int);

	/* gen priv init*/
	priv->len = Len;
	mpz_set_str(priv->p,"87605462876808144277426165973883539913237142032\
		4680111025888173012517411282947746307077112948059752617293669\
		2475916007084824643251292293280941599907064927" , 10);
	mpz_set_str(priv->q, "12632582314731416705488970654316872884030266575\
		771457040033359184036685426544448829359932217916476496496957\
		355860404016540385099824592087324722308992159249", 10);
	mpz_mul(priv->p2, priv->p, priv->p);
	mpz_mul(priv->q2, priv->q, priv->q);
	mpz_invert(priv->qinver, priv->q, priv->p);
	mpz_invert(priv->hp, priv->q, priv->p);

	
	/* h(x)*/
	h_func(priv->hp, priv->p, priv->p2, pub);
	h_func(priv->hq, priv->q, priv->q2, pub);


	mpz_clear(tmp);

}

void eggroll_raw_encrypt(eggroll_public_key *pub, mpz_t plaintext, mpz_t ciphertext)
{
	/* nm for neg_plaintext */
	/* nc for neg_ciphertext */
	mpz_t r;
	mpz_t nm;
	mpz_t nc;
	mpz_init(r);
	mpz_init(nm);
	mpz_init(nc);

	if (mpz_cmp(plaintext, pub->sub_n_max) >= 0 && mpz_cmp(plaintext, pub->n) < 0)
	{
		//printf("---------------\n");

		/* neg_plaintext = n - plaintext */
		mpz_sub(nm, pub->n, plaintext);
		
		/* neg_ciphertext = n * neg_plaintext + 1) % n2 */
		mpz_mul(r, pub->n, nm);
		mpz_add_ui(r, r, 1);
		mpz_mod(nc, r, pub->n2);

		/* ciphertext = invert(neg_ciphertext, n2) */
		mpz_invert(ciphertext, nc, pub->n2);
	}
	else
	{
		//printf("+++++++++++++\n");
		/* (n * plaintext + 1) % n2 */
		mpz_mul(r, pub->n, plaintext);
		mpz_add_ui(r, r, 1);

		//gmp_printf("r = %Zd\n", r);
		mpz_mod(ciphertext, r, pub->n2);
	}

	mpz_clear(r);
	mpz_clear(nm);
	mpz_clear(nc);

}

void eggroll_non_obfuscator(eggroll_public_key *pub, mpz_t ciphertext)
{
	mpz_t r;
	mpz_t obf;
	/* gen random */
	gen_stablekey(r, pub->len);
	mpz_mod(r, r, pub->n);
	if (mpz_cmp_ui(r, 0) == 0) {
		fputs("random number is zero!\n", stderr);
		mpz_clear(r);
		exit(1);
	}

	/* obfuscator */
	mpz_powm(obf, r, pub->n, pub->n2);
	mpz_mul(r, obf, ciphertext);
	mpz_mod(ciphertext, r, pub->n2);
}

void eggroll_obfuscator(eggroll_public_key* pub, mpz_t ciphertext)
{
	mpz_t r;
	mpz_t obf;
	mpz_init(r);
	mpz_init(obf);
	/* gen random */
	gen_stablekey(r, pub->len);
	mpz_mod(r, r, pub->n);
	if (mpz_cmp_ui(r, 0) == 0) {
		fputs("random number is zero!\n", stderr);
		mpz_clear(r);
		exit(1);
	}

	/* obfuscator */
	mpz_powm(obf, r, pub->n, pub->n2);
	mpz_mul(r, obf, ciphertext);
	mpz_mod(ciphertext, r, pub->n2);
	
	mpz_clear(r);
	mpz_clear(obf);
}

void eggroll_mulc(mpz_t ciphertext2, mpz_t ciphertext1, mpz_t constant, eggroll_public_key* pub)
{
	/* nc for neg_cipher */
	/* ns for neg_scalar */
	mpz_t nc;
	mpz_t ns;
	mpz_init(nc);
	mpz_init(ns);
	if (mpz_cmp(constant, pub->sub_n_max) >= 0)
	{
		/* constant is huge, play trick */
		mpz_invert(nc, ciphertext1, pub->n2);
		mpz_sub(ns, pub->n, constant);
		mpz_powm(ciphertext2, nc, ns, pub->n2);

	}
	else
	{
		mpz_powm(ciphertext2, ciphertext1, constant, pub->n2);
	}

	//gmp_printf("[cpu dotmul ]res = %Zd\n", ciphertext2);

	mpz_clear(nc);
	mpz_clear(ns);

}

void eggroll_add(mpz_t ciphertext3, mpz_t ciphertext1, mpz_t ciphertext2, eggroll_public_key* pub)
{
	mpz_t r;
	mpz_init(r);
	mpz_mul(r, ciphertext1, ciphertext2);
	mpz_mod(ciphertext3, r, pub->n2);

	//gmp_printf("[!!!!!!!!!dotadd]res = %Zd\n", ciphertext1);
	//gmp_printf("[!!!!!!!!!dotadd]res = %Zd\n", ciphertext2);
	//gmp_printf("[!!!!!!!!!dotadd]ciper1 size = %d\n", ciphertext1->_mp_size);
	//gmp_printf("[!!!!!!!!!dotadd]ciper2 size = %d\n", ciphertext2->_mp_size);
	//gmp_printf("[_________dotadd]:res = %Zd\n", ciphertext3);

	mpz_clear(r);
}

void eggroll_raw_decrypt(eggroll_public_key* pub, eggroll_private_key* priv, mpz_t ciphertext, mpz_t plaintext)
{

	//gmp_printf("hhhhhhhhhhhhhhhhhhhhhhhhhhere!!!\n");

	mpz_t mp, mq;
	mpz_t r, s;
	mpz_t u;
	mpz_init(mp);
	mpz_init(mq);
	mpz_init(r); mpz_init(s);
	mpz_init(u);
	
	//mp
	/* tmp1 = powm(ciphertext, p-1, p2)*/
	mpz_sub_ui(r, priv->p, 1);
	mpz_powm(s, ciphertext, r, priv->p2);

	//gmp_printf("[cpu decrpyt] s = %Zd\n", r);

	/* tmp2 = ( tmp1 -1 ) / p */
	mpz_sub_ui(r, s, 1);
	mpz_div(s, r, priv->p);

	/* tmp3 = tmp2 * hp % p */
	mpz_mul(r, s, priv->hp);
	mpz_mod(mp, r, priv->p);

	//gmp_printf("[cpu decrpyt] mp = %Zd\n", mp);

	//mq
	/* tmp1 = powm(ciphertext, q-1, q2)*/
	mpz_sub_ui(r, priv->q, 1);
	mpz_powm(s, ciphertext, r, priv->q2);
	/* tmp2 = ( tmp1 -1 ) / q */
	mpz_sub_ui(r, s, 1);
	mpz_div(s, r, priv->q);
	/* tmp3 = tmp2 * hq % q */
	mpz_mul(r, s, priv->hq);
	mpz_mod(mq, r, priv->q);

	/* ctr(mq, mq) */
	/* tmp1 = (mp - mq) * qinverse % p */
	mpz_sub(r, mp, mq);
	mpz_mul(s, r, priv->qinver);
	mpz_mod(u, s, priv->p);

	/* plaintext =  (mq + (tmp1 * q)) % n */
	mpz_mul(r, u, priv->q);
	mpz_add(s, mq, r);
	mpz_mod(plaintext, s, pub->n);

	mpz_clear(r);
	mpz_clear(s);
	mpz_clear(u);
	mpz_clear(mp);
	mpz_clear(mq);
}

void testeggrollPaillier(FILE* public_key, FILE* private_key, FILE* plaintext)
{
	eggroll_keygen_stable(&pub, &priv, (unsigned long int)1024);

	printf("+_+++++%d\n", pub.len);

	//gmp_printf("pub.n = %Zd\n", pub.n);
	//gmp_printf("pub.g = %Zd\n", pub.g);
	//gmp_printf("pub.maxInt = %Zd\n", pub.max_int);
	//gmp_printf("pub.n2 = %Zd\n", pub.n2);

	//gmp_printf("priv.q = %Zd\n", priv.q);
	//gmp_printf("priv.p = %Zd\n", priv.p);
	//gmp_printf("priv.p2 = %Zd\n", priv.p2);
	//gmp_printf("priv.q2 = %Zd\n", priv.q2);
	//gmp_printf("priv.qinv = %Zd\n", priv.qinver);

	//gmp_printf("priv.hq = %Zd\n", priv.hq);
	//gmp_printf("priv.hp = %Zd\n", priv.hp);

	mpz_t m;
	mpz_t c1, c2, c3;
	mpz_init(c1);
	mpz_init(c2);
	mpz_init(c3);
	mpz_init_set_ui(m, 3);
	eggroll_raw_encrypt(&pub, m, c1);
	eggroll_raw_encrypt(&pub, m, c2);

	//int i;
	//clock_t str = clock();
	//for (i = 0; i < 1000; i++)
	//{
	//	eggroll_obfuscator(&pub, c);
	//}
	//clock_t end = clock();
	//printf("eggroll_obfuscator 1000 cost time = %lf ms\n",
	//	(double)(end - str) * 1000 / CLOCKS_PER_SEC);

/*	eggroll_mulc(c2, c, m, &pub);*/
	//gmp_printf("encrpyt(0) = %Zd\n", c3);

	eggroll_add(c3, c1, c2, &pub);
	gmp_printf("C3 = %Zd\n", c3);

	eggroll_raw_decrypt(&pub, &priv, c3, m);
	gmp_printf("C3 = %Zd\n", m);


}

/*================================= exp encode ===========================================*/
int64_t alineCiper(eggroll_public_key* pub, mpz_t cipher1, mpz_t cipher2, int64_t exp1, int64_t exp2) {
	//float64 factor = 0;
	mpz_t _f;
	mpz_t _base;
	mpz_init(_f);
	mpz_init_set_ui(_base, BASE);

	printf("[add] %d %d\n", exp1, exp2);

	if (exp1 < exp2) 
	{
		//change ciper1
		mpz_pow_ui(_f, _base, exp2 - exp1);
		mpz_mod(_f, _f, pub->n);
		eggroll_mulc(cipher1, cipher1, _f, pub);
		
		mpz_clear(_f);
		return exp2;
	}
	else
	{
		//change ciper2
		mpz_pow_ui(_f, _base, exp1 - exp2);
		mpz_mod(_f, _f, pub->n);
		eggroll_mulc( cipher2, cipher2, _f, pub);
		mpz_clear(_f);
		return exp1;
	}

}

//void exponent_to(eggroll_public_key* pub, mpz_t n_ciph,
//	int64_t n_exp, mpz_t ciph, int64_t exp) {
//	//float64 factor = 0;
//	mpz_t factor;
//	mpz_init(factor);
//
//	if (n_exp < exp) {
//		printf("New exponent %d should be greater than old exponent %d", n_exp, exp);
//	}
//	mpz_set_si(factor, (int64_t)pow(BASE, n_exp - exp)); 
//	mpz_mod(factor, factor, pub->n);
//	eggroll_mulc(n_ciph, ciph, factor, pub);
//	mpz_clear(factor);
//}
//

int64_t encode_float64(eggroll_public_key* pub, mpz_t base, float64_t val, int64_t precision, int64_t max_exp) {
	int64_t _exp = 0;
	mpz_t int_fixpoint;
	mpf_t _base;
	mpf_t _factor, _scalar;

	int _expp = 0;


	if (fabs(val) < 1e-200) {
		mpz_set_si(base, 0);
		return 0;
	}

	if (pub == NULL) {
		printf("error : public key is NULL\n");
	}

	mpz_init(int_fixpoint);
	mpf_init_set_ui(_base, BASE);
	mpf_init(_factor);
	mpf_init(_scalar);

	if (precision < 0) {
		frexp(val, &_expp);
		_exp = (int64_t)_expp;	
		_exp = FLOAT_MANTISSA_BITS - _exp;
		_exp = floor(_exp / LOG2_BASE);
	}
	else 
	{
		_exp = floor(log(precision) / log(BASE));
	}
	if ((max_exp > 0) && (max_exp > _exp)) 
		_exp = max_exp;

	//int_fixpoint = round(val * pow(BASE, exp));

	mpf_pow_ui(_factor, _base, _exp);
	mpf_set_d(_scalar, val);

	mpf_mul(_scalar, _scalar, _factor);

	mpf_set_d(_factor, .5f);
	mpf_add(_scalar, _scalar, _factor);
	mpf_floor(_scalar, _scalar);
	mpz_set_f(int_fixpoint, _scalar);


	//gmp_printf("[*************]exp = %.*Ff\n", 10, _scalar);
	//gmp_printf("[*************]base = %Zd\n", int_fixpoint);

	//if abs(int_fixpoint) > max_int
	if (mpz_cmpabs(int_fixpoint, pub->max_int) > 0) {
		printf("error :int_fixpoint is larger than max_int !\n");
		mpz_clear(int_fixpoint);
		mpf_clear(_factor);
		mpf_clear(_scalar);
		return -1;
	}

	//encoding = int_fixpoint % n;
	mpz_mod(base, int_fixpoint, pub->n);

	/*    printf("------- max_exponent = :%ld\n", max_exp);
	    printf("----------- exponent = :%ld\n", _exp);
	gmp_printf("------------------ n = :%Zd\n", pub->n);
	gmp_printf("------------ max_int = :%Zd\n", pub->max_int);    
	gmp_printf("------------------ base = :%Zd\n", base);
*/
	mpz_clear(int_fixpoint);
	mpf_clear(_factor);
	mpf_clear(_scalar);
	return (int64_t)_exp;
}

int64_t encode_int64(eggroll_public_key* pub, mpz_t base, int64_t val, int64_t precision, int64_t max_exp) {
	int64_t exp = 0;
	mpz_t int_fixpoint;

	if (0 == val) {
		mpz_set_si(base, 0);
		return 0;
	}
	if (pub == NULL) {
		printf("error : public key is NULL\n");
	}

	mpz_init(int_fixpoint);

	if (precision < 0) {
		exp = 0;
	}
	else {
		exp = floor(log(precision) / log(BASE));
	}

	if (max_exp > exp) {
		exp = max_exp;
	}

	if (exp <= 0) {
		mpz_set_si(int_fixpoint, val);
	}
	else {
		//int_fixpoint = round((scalar * pow(BASE, exp));
		mpz_set_si(int_fixpoint, pow(BASE, exp));
		mpz_mul_si(int_fixpoint, int_fixpoint, val);
	}
	//if abs(int_fixpoint) > max_int
	if (mpz_cmpabs(int_fixpoint, pub->max_int) > 0) {
		printf("error :int_fixpoint is larger than max_int !\n");
		mpz_clear(int_fixpoint);
		return -1;
	}

	//*pint_fixpoint = int_fixpoint % n;
	mpz_mod(base, int_fixpoint, pub->n);

	mpz_clear(int_fixpoint);
	return exp;
}

float64_t decode_float64(eggroll_public_key* pub, mpz_t base, int64_t exp) {
	float64_t scalar;
	mpz_t threshold;
	mpf_t float_mantissa, float_factor;

	if (pub == NULL) {
		printf("error : public key is NULL\n");
	}

	mpz_init(threshold);
	mpf_init(float_mantissa);
	mpf_init(float_factor);

	if (mpz_cmp(base, pub->n) >= 0) {
		// Should be mod n
		printf("error :attempted to decode corrupted number !\n");
		return 0;
	}
	else if (mpz_cmp(base, pub->max_int) <= 0) {
		// Positive
		mpf_set_z(float_mantissa, base); //float_mantissa = encoding;
	}
	else {
		mpz_sub(threshold, pub->n, pub->max_int);
		if (mpz_cmp(base, threshold) >= 0) {
			// Negative
			mpz_sub(threshold, base, pub->n);
			mpf_set_z(float_mantissa, threshold); //float_mantissa = encoding - n;
		}
		else {
			printf("error :overflow detected in decode number !\n");
		}
	}
	//scalar = float_mantissa * pow(BASE, -exp);
	mpf_set_d(float_factor, pow(BASE, -exp));
	mpf_mul(float_mantissa, float_mantissa, float_factor);
	scalar = mpf_get_d(float_mantissa);

	mpz_clear(threshold);
	mpf_clear(float_mantissa);
	mpf_clear(float_factor);

	return scalar;
}

float64_t decode_int64(eggroll_public_key* pub, mpz_t base, int64_t exp) {
	return (int64_t)decode_float64(pub, base, exp);
}

/*================================= exp encode ===========================================*/



/*================================= Mat - Vec protect  ===================================*/
void encrpytProtect(mpz_manager* mng, eggroll_public_key* pub)
{
	int i, j;
	if (mng->ifenc)
	{
		return;
	}
	printf("[dotAdd encrpyt]: encrpyt Protect! \n");

	for ( i = 0; i < mng->row; i++)
	{
		for ( j = 0; j < mng->col; j++)
		{
			eggroll_raw_encrypt(pub, mng->field[i * mng->col + j],
				mng->field[i * mng->col + j]);
		}
	}

	mng->ifenc = 1;

}


/*================================= Mat - Vec function  ===================================*/

/* fundation function */

void keyInit(eggroll_public_key* pub, eggroll_private_key* priv)
{
	eggroll_keygen_stable(pub, priv, 1024);
}

void initManager(mpz_manager* target, int col, int row, int bits)
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

//void dataEncrpyt(mpz_t* ciphy, mpz_t* data,
//	eggroll_public_key* pub, int col, int row)
//{
//	int i, j;
//	int idx;
//	mpz_t m;
//	mpz_init_set_ui(m, data[0]);
//
//	for (i = 0; i < row; i++)
//	{
//		for (j = 0; j < col; j++)
//		{
//			idx = i * col + j;
//			eggroll_raw_encrypt(pub, data[idx], ciphy[idx]);
//
//			//eggroll_obfuscator(pub, ciphy[idx]);
//
//			//gmp_printf("[dataEncrpyt] : %Zd\n", ciphy[idx]);
//		}
//	}
//	//printf("[dataEncrpyt] boxxxxxxxxxxxxx: %ld\n", data[0]);
//
//}

void dataEncrpyt(mpz_manager* ciphy, mpz_manager* data,
	eggroll_public_key* pub, int col, int row)
{
	int i, j;
	int idx;

	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;
			eggroll_raw_encrypt(pub, data->field[idx], ciphy->field[idx]);
			ciphy->exp[idx] = data->exp[idx];

			//printf("##############%ld\n", ciphy->exp[idx]);
			/*printf("[dataEncrpyt] exp : %ld\n", ciphy->exp[idx]);
			printf("[dataEncrpyt] size : %d\n", ciphy->field[idx]->_mp_size);
			printf("[dataEncrpyt] alloc : %d\n", ciphy->field[idx]->_mp_alloc);
			gmp_printf("[dataEncrpyt] : %Zd\n", ciphy->field[idx]);*/
		}
	}
	//printf("[dataEncrpyt] boxxxxxxxxxxxxx: %ld\n", data[0]);

}

void dataRawEncrpyt(mpz_t* ciphy, uint64_t* data,
	eggroll_public_key* pub, int col, int row)

{
	int i, j;
	int idx;
	mpz_t m;
	mpz_init_set_ui(m, data[0]);

	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;
			mpz_init_set_ui(m, data[idx]);
			eggroll_raw_encrypt(pub, m, ciphy[idx]);
			//gmp_printf("[dataEncrpyt] : %Zd\n", ciphy[idx]);
		}
	}
	//printf("[dataEncrpyt] boxxxxxxxxxxxxx: %ld\n", data[0]);

}

void dataDecrpyt(mpz_manager* ciphy, mpz_manager* data,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row)
{
	int i, j;
	int idx;

	float tmp;
	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;


			//gmp_printf("[cpu decrpyt ciper ]:%Zd\n", ciphy->field[idx]);

			eggroll_raw_decrypt(pub, priv, ciphy->field[idx], data->field[idx]);
			data->exp[idx] = ciphy->exp[idx];
		
			/*tmp = decode_float64(pub, data->field[idx], ciphy->exp[idx]);
			printf("%f\n", tmp);*/
			//gmp_printf("[cpu decrpyt plain]:%Zd\n", data->field[idx]);
			//printf("[cpu decrpyt exp ]: %ld\n", data->exp[idx]);
		}
	}
	data->ifenc = 0;

	/*uint64_t* iter = data[0]->_mp_d;
	for (i = 0; i < 32; i++)
	{
		printf("[decrpyt]:%ld\n", iter[i]);
	}*/
}

void dataDecode(mpz_manager* coder, double* val,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row)
{
	int i, j;
	int idx;


	float tmp;
	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;
			val[idx] = decode_float64(pub, coder->field[idx],
				coder->exp[idx]);
			printf("[decode] : %lf \n", val[idx]);
		}
		printf("\n");

	}


}

void decryptPrint(char* adr, mpz_t* p, eggroll_public_key* pub, eggroll_private_key* priv)
{
	mpz_t m;
	mpz_init(m);

	eggroll_raw_decrypt(pub, priv, p, m);
	gmp_printf("[gmp %s]: %Zd\n", adr, m);
}
//
//void matMul(mpz_t* result, mpz_t* ciphy, mpz_t* message,
//	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row)
//{
//	int i, j;
//	int idx;
//
//	///* test */
//	mpz_t test;
//	mpz_init(test);
//
//	mpz_t   _r1, _r2;
//	mpz_init(_r1);
//	mpz_init(_r2);
//
//	mpz_t zero;
//	mpz_init_set_ui(zero, 0);
//
//	for (i = 0; i < row; i++)
//	{
//		eggroll_raw_encrypt(pub, zero, result[i]);
//		for (j = 0; j < col; j++)
//		{
//			idx = i * col + j;
//			eggroll_mulc(_r1, ciphy[idx], message[j], pub);
//			eggroll_add(result[i], result[i], _r1, pub);
//
//			eggroll_raw_decrypt(pub, priv, ciphy[idx], test);
//			gmp_printf("[gmp+print+mul]:%Zd * %Zd\n", test, message[j]);
//
//		}
//	}
//	mpz_clear(_r1);
//	mpz_clear(_r2);
//
//
//	///////* printf*/
//	//mpz_t a1, a2, a3;
//	//mpz_init(a1); mpz_init(a2); mpz_init(a3);
//	//gmp_printf("[!!!!!!!!!!] message %Zd\n", message[0]);
//
//	//gmp_printf("[!!!!!!!!!!!!]a1  %Zd\n", result[0]);
//
////
//	//paillier_encrypt(result[0], zero, pub);
//	//for (j = 0; j < col; j++)
//	//{
//	//	//idx = i * col + j;
//	//	mpz_set_ui(_m, message[j]);
//	//	printf("[!!!!!!!!!!] message %d\n", message[j]);
//
//	//	paillier_homomorphic_multc(_r1, ciphy[j + 4], _m, pub);
//	//	decryptPrint("ciphy", ciphy[j + 4], priv);
//	//	decryptPrint("r1",_r1, priv);
//
//	//	paillier_homomorphic_add(result[i], result[i], _r1, pub);
//
//	//	decryptPrint("result", result[i], priv);
//
//	//}
//
//	/*decryptPrint("result", result[0], priv);
//	decryptPrint("result", result[1], priv);
//	decryptPrint("result", result[2], priv);*/
//
//	//gmp_printf("[!!!!!!!!!!!!]a2  %Zd\n", a2);
//
//	///*8888888888888888888888888*/
//
//}

void matMul(mpz_manager* res, mpz_manager* mat1, mpz_manager* mat2,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row)
{
	int i, j;
	int idx;

	mpx _r1;
	mpx _r2;
	mpz_t zero;
	mpx_init(&_r1);
	mpx_init(&_r2);
	mpz_init(zero);
	mpz_set_ui(zero, 0);

	for (i = 0; i < row; i++)
	{
		/* set result base n exp */
		res->exp[i] = encode_float64(pub, zero, 0.f, -1, -1);
		eggroll_raw_encrypt(pub, zero, res->field[i]);
		
		printf("res->exp[i]  = %ld\n", res->exp[i]);
		gmp_printf("gmp value = %Zd\n", res->field[i]);

		//printf("[col ] = %d [row] = %d\n", col, row);
		/* travel sub data */
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;
			/* a0 * b0 */
			
			/*gmp_printf("[mat mul ]: %d mat2->field[j] = %Zd\n", j, mat2->field[j]);
			gmp_printf("[mat mul ]: n = %Zd\n", pub->n);*/

			if (mpz_cmp_ui(mat2->field[j], 0) < 0 ||
				mpz_cmp(mat2->field[j], pub->n) >= 0)
			{
				printf("[dotMul]: error value out of bounds\n");
				return -1;
			}

			/* _r1 = mat1 * mat2 */
			eggroll_mulc(_r1.bot, mat1->field[idx], mat2->field[j], pub);
			_r1.exp = mat1->exp[idx] + mat2->exp[j];
			
			/*if (i == 0 && j == 0)
			{
				eggroll_raw_decrypt(pub, priv, _r1.bot, _r2.bot);
				float tmp = decode_float64(pub, _r2.bot, _r1.exp);
				printf("[************]%f\n", tmp);

			}*/

		/*	eggroll_raw_decrypt(pub, priv, _r1.bot, _r2.bot);
			float tmp = decode_float64(pub, _r2.bot, _r1.exp);
			printf("[************]%f\n", tmp);*/

			/* sum(a0 * b0) */
			if (res->exp[i] == _r1.exp)
			{
				eggroll_add(res->field[i], res->field[i], _r1.bot, pub);
				res->exp[i] = _r1.exp;
			}
			else
			{
				res->exp[i] = alineCiper(pub, res->field[i], _r1.bot,
					res->exp[i], _r1.exp);
				eggroll_add(res->field[i], res->field[i], _r1.bot, pub);
			}
			
			


		}

		if (i == 0)
		{
			gmp_printf("[matmul]:res->filed[0] %Zd\n", res->field[i]);
			printf("[matmul]:res->exp[0] %ld\n", res->exp[i]);
		}

	/*	eggroll_raw_decrypt(pub, priv, res->field[i], _r2.bot);
		float tmp = decode_float64(pub, _r2.bot, res->exp[i]);
		printf("[>>>>>>>>>>>]%f\n", tmp);*/
	}

	res->ifenc = 1;

	mpx_delete(&_r1);
	mpx_delete(&_r2);
}

void matDotMul(mpz_manager* res, mpz_manager* mat1, mpz_manager* mat2,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row)
{
	int i, j;
	int idx;
	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;
			
			if (mpz_cmp_ui(mat2->field[idx], 0) < 0 ||
				mpz_cmp(mat2->field[idx], pub->n) >= 0)
			{
				printf("[dotMul]: error value out of bounds\n");
				return -1;
			}

			/*gmp_printf("[csv+++++++]%Zd\n", mat2->field[idx]);
			gmp_printf("[csv+++++++]%Zd\n", mat1->field[idx]);*/

			eggroll_mulc(res->field[idx], mat1->field[idx], mat2->field[idx], pub);
			res->exp[idx] = mat1->exp[idx] + mat2->exp[idx];

			gmp_printf("[cpu dotmul] cipher = %Zd\n", res->field[idx]);
			printf("[cpu dotmul] exp = %Zd\n", res->exp[idx]);

		}
	}
}

void matSlcMul(mpz_manager* res, mpz_manager* mat, double slc,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row)
{
	int i, j;
	int idx;

	/* encode scalar */
	int64_t _ex;
	mpz_t _en;
	mpz_init(_en);
	mpz_setbit(_en, 2048);
	_ex = encode_float64(pub, _en, slc, -1, -1);


	if (mpz_cmp_ui(_en, 0) < 0 ||
		mpz_cmp(_en, pub->n) >= 0)
	{
		printf("[dotMul]: error value out of bounds\n");
		return -1;
	}

	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;
			/*gmp_printf("[csv+++++++]%Zd\n", mat2->field[idx]);
			gmp_printf("[csv+++++++]%Zd\n", mat1->field[idx]);*/

			eggroll_mulc(res->field[idx], mat->field[idx], _en, pub);
			res->exp[idx] = mat->exp[idx] + _ex;
		}
	}
}


void matDotAdd(mpz_manager* res, mpz_manager* mat1, mpz_manager* mat2,
	eggroll_public_key* pub, eggroll_private_key* priv, int col, int row)
{
	mpz_t tmp;
	mpz_init(tmp);

	int i, j, idx;
	int64_t exp1, exp2;
	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;
			if (mat1->exp[idx] == mat2->exp[idx])
			{
				eggroll_add(res->field[idx], mat1->field[idx], mat2->field[idx], pub);
				res->exp[idx] = mat1->exp[idx];
			}
			else
			{
				res->exp[idx] = alineCiper(pub, mat1->field[idx], mat2->field[idx],
					mat1->exp[idx], mat2->exp[idx]);
				eggroll_add(res->field[idx], mat1->field[idx], mat2->field[idx], pub);
			}
		
			//gmp_printf("[cpu dotadd ]: %Zd\n", res->field[idx]);
		
		
		}
	}
	mpz_clear(tmp);
}

void dataObfuscator(mpz_t* ciphy, eggroll_public_key* pub, eggroll_private_key* priv,
	int col, int row)
{
	int i, j;
	int idx;

	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			idx = i * col + j;
			eggroll_obfuscator(pub, ciphy[idx]);
			//gmp_printf("[dataEncrpyt] : %Zd\n", ciphy[idx]);
		}
	}



}

void rToC(uint64_t* a, int row, int col)
{
	int tmp;
	tmp = row;
	row = col;
	col = tmp;
	int i;
	float* temp = (float*)malloc(sizeof(float) * row * col);

	for (i = 0; i < row * col; i++)
		temp[i] = a[i / row + i % row * col];
	for (i = 0; i < row * col; i++)
		a[i] = temp[i];
	free(temp);
	return;
}


/*================================= about D/Serialization  ===================================*/

int get_public_len(eggroll_public_key* pub)
{

	/*printf("[get pub len]:g : %d\n", pub->g->_mp_size);
	printf("[get pub len]:n : %d\n", pub->n->_mp_size);
	printf("[get pub len]:n2 : %d\n", pub->n2->_mp_size);
	printf("[get pub len]:max : %d\n", pub->max_int->_mp_size);
	printf("[get pub len]:sub : %d\n", pub->sub_n_max->_mp_size);
	printf("[get pub len]:1 : 1\n");*/

	int len = 0;
	len += pub->g->_mp_size;
	len += pub->n->_mp_size;
	len += pub->n2->_mp_size;
	len += pub->max_int->_mp_size;
	len += pub->sub_n_max->_mp_size;
	len += 1;

	//printf("[get pub len]:len  : %d\n", len);

	return len;

}

int get_private_len(eggroll_private_key* priv)
{
	int len = 0;
	len += priv->p->_mp_size;
	len += priv->q->_mp_size;
	len += priv->p2->_mp_size;
	len += priv->q2->_mp_size;
	len += priv->qinver->_mp_size;
	len += priv->hp->_mp_size;
	len += priv->hq->_mp_size;
	len += 1;

	printf("[get priv len]:p : %d\n", priv->p->_mp_size);
	printf("[get priv len]:q : %d\n", priv->q->_mp_size);
	printf("[get priv len]:p2 : %d\n", priv->p2->_mp_size);
	printf("[get priv len]:q2 : %d\n", priv->q2->_mp_size);
	printf("[get priv len]:qinv : %d\n", priv->qinver->_mp_size);
	printf("[get priv len]:hp : %d\n", priv->hp->_mp_size);
	printf("[get priv len]:hq : %d\n", priv->hq->_mp_size);
	printf("[get priv len]:1 : 1\n");
	printf("[get priv len]:len  : %d\n", len);

	return len;
}


int dumpingPub(eggroll_public_key* pub, uint64_t* dumping)
{
	int i, j;
	int l, f;
	int ms[6] = { pub->g->_mp_size,
				pub->n->_mp_size,
				pub->n2->_mp_size,
				pub->max_int->_mp_size,
				pub->sub_n_max->_mp_size,
				1 };

	int of[6];
	f = 0;
	for (i = 0; i < 6; i++)
	{
		of[i] = f;
		f += ms[i];
	}

	/* insert member len in forn of array */
	uint64_t* iter;
	for (i = 0; i < 6; i++)
	{
		iter = dumping + i;
		*iter = (uint64_t*)ms[i];
		//printf("[dumping pub]: %d\n", *iter);
	}

	///* insert data */
	iter = (dumping + 6) + of[0];
	memcpy(iter, pub->g->_mp_d, sizeof(uint64_t) * pub->g->_mp_size);
	//printf("[dumping pub] g[0] : %ld\n", iter[0]);

	iter = (dumping + 6) + of[1];
	memcpy(iter, pub->n->_mp_d, sizeof(uint64_t) * pub->n->_mp_size);
	//printf("[dumping pub] n[0] : %ld\n", iter[0]);

	iter = (dumping + 6) + of[2];
	memcpy(iter, pub->n2->_mp_d, sizeof(uint64_t) * pub->n2->_mp_size);
	//printf("[dumping pub] n2[0] : %ld\n", iter[0]);

	iter = (dumping + 6) + of[3];
	memcpy(iter, pub->max_int->_mp_d, sizeof(uint64_t) * pub->max_int->_mp_size);
	//printf("[dumping pub] max[0] : %ld\n", iter[0]);

	iter = (dumping + 6) + of[4];
	memcpy(iter, pub->sub_n_max->_mp_d, sizeof(uint64_t) * pub->sub_n_max->_mp_size);
	//printf("[dumping pub] sub[0] : %ld\n", iter[0]);

	iter = (dumping + 6) + of[5];
	memcpy(iter, &pub->len, sizeof(uint64_t));

}

int dumpingPriv(eggroll_private_key* priv, uint64_t* dumping)
{
	int i, j;
	int l, f;
	int ms[8] = { priv->p->_mp_size,
				priv->q->_mp_size,
				priv->p2->_mp_size,
				priv->q2->_mp_size,
				priv->qinver->_mp_size,
				priv->hp->_mp_size,
				priv->hq->_mp_size,
				1 };

	int of[8];
	f = 0;
	for (i = 0; i < 8; i++)
	{
		of[i] = f;
		f += ms[i];
	}

	/* insert member len in forn of array */
	uint64_t* iter;
	for (i = 0; i < 8; i++)
	{
		iter = dumping + i;
		*iter = (uint64_t*)ms[i];
	}


	/* insert data */
	iter = (dumping + 8) + of[0];
	memcpy(iter, priv->p->_mp_d, sizeof(uint64_t) * priv->p->_mp_size);
	printf("[dumping priv] p[0] : %ld\n", iter[0]);

	iter = (dumping + 8) + of[1];
	memcpy(iter, priv->q->_mp_d, sizeof(uint64_t) * priv->q->_mp_size);
	printf("[dumping priv] q[0] : %ld\n", iter[0]);

	iter = (dumping + 8) + of[2];
	memcpy(iter, priv->p2->_mp_d, sizeof(uint64_t) * priv->p2->_mp_size);
	printf("[dumping priv] p2[0] : %ld\n", iter[0]);

	iter = (dumping + 8) + of[3];
	memcpy(iter, priv->q2->_mp_d, sizeof(uint64_t) * priv->q2->_mp_size);
	printf("[dumping priv] q2[0] : %ld\n", iter[0]);

	iter = (dumping + 8) + of[4];
	memcpy(iter, priv->qinver->_mp_d, sizeof(uint64_t) * priv->qinver->_mp_size);
	printf("[dumping priv] qinver[0] : %ld\n", iter[0]);

	iter = (dumping + 8) + of[5];
	memcpy(iter, priv->hp->_mp_d, sizeof(uint64_t) * priv->hp->_mp_size);
	printf("[dumping priv] hp[0] : %ld\n", iter[0]);

	iter = (dumping + 8) + of[6];
	memcpy(iter, priv->hq->_mp_d, sizeof(uint64_t) * priv->hq->_mp_size);
	printf("[dumping priv] hq[0] : %ld\n", iter[0]);

	iter = (dumping + 8) + of[7];
	memcpy(iter, &priv->len, sizeof(uint64_t));
	printf("[dumping priv] len : %ld\n", iter[0]);


}

int loadPub(eggroll_public_key* pub, uint64_t* loading)
{
	int i, j;
	int l, f;

	uint64_t mblen[6];
	for (i = 0; i < 6; i++)
	{
		mblen[i] = loading[i];
		//printf("[!!load pub] meml = %ld\n", mblen[i]);
	}

	int of[6];
	f = 0;
	for (i = 0; i < 6; i++)
	{
		of[i] = f;
		f += mblen[i];
	}

	uint64_t* ld = loading + 6 + of[0];
	mpz_init(pub->g);
	mpz_import(pub->g, mblen[0], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load pub] g[0] = %ld\n", pub->g->_mp_d[0]);

	ld = loading + 6 + of[1];
	mpz_init(pub->n);
	mpz_import(pub->n, mblen[1], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load pub] n[0] = %ld\n", pub->n->_mp_d[0]);

	ld = loading + 6 + of[2];
	mpz_init(pub->n2);
	mpz_import(pub->n2, mblen[2], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load pub] n2[0] = %ld\n", pub->n2->_mp_d[0]);

	ld = loading + 6 + of[3];
	mpz_init(pub->max_int);
	mpz_import(pub->max_int, mblen[3], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load pub] max[0] = %ld\n", pub->max_int->_mp_d[0]);

	ld = loading + 6 + of[4];
	mpz_init(pub->sub_n_max);
	mpz_import(pub->sub_n_max, mblen[4], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load pub] sub[0] = %ld\n", pub->sub_n_max->_mp_d[0]);

	ld = loading + 6 + of[5];
	pub->len = *ld;
	//printf("[load pub] sub[0] = %ld\n", *ld);
}

int loadPriv(eggroll_private_key* priv, uint64_t* loading)
{
	int i, j;
	int l, f;

	uint64_t mblen[8];
	for (i = 0; i < 8; i++)
	{
		mblen[i] = loading[i];
		//printf("[load priv] meml = %ld\n", mblen[i]);

	}

	int of[8];
	f = 0;
	for (i = 0; i < 8; i++)
	{
		of[i] = f;
		f += mblen[i];
	}

	uint64_t* ld = loading + 8 + of[0];
	mpz_init(priv->p);
	mpz_import(priv->p, mblen[0], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load priv] p[0] = %ld\n", priv->p->_mp_d[0]);

	ld = loading + 8 + of[1];
	mpz_init(priv->q);
	mpz_import(priv->q, mblen[1], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load priv] q[0] = %ld\n", priv->q->_mp_d[0]);

	ld = loading + 8 + of[2];
	mpz_init(priv->p2);
	mpz_import(priv->p2, mblen[2], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load priv] p2[0] = %ld\n", priv->p2->_mp_d[0]);

	ld = loading + 8 + of[3];
	mpz_init(priv->q2);
	mpz_import(priv->q2, mblen[3], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load priv] q2[0] = %ld\n", priv->q2->_mp_d[0]);

	ld = loading + 8 + of[4];
	mpz_init(priv->qinver);
	mpz_import(priv->qinver, mblen[4], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load priv] qinver[0] = %ld\n", priv->qinver->_mp_d[0]);

	ld = loading + 8 + of[5];
	mpz_init(priv->hp);
	mpz_import(priv->hp, mblen[5], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load priv] hp[0] = %ld\n", priv->hp->_mp_d[0]);

	ld = loading + 8 + of[6];
	mpz_init(priv->hq);
	mpz_import(priv->hq, mblen[5], -1, sizeof(uint64_t), 0, 0, ld);
	//printf("[load priv] hq[0] = %ld\n", priv->hq->_mp_d[0]);

	ld = loading + 8 + of[7];
	priv->len = *ld;
}
