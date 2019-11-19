#pragma once
#include "Python.h"
#include "paillier.h"
#include "tools.h"
#include "toPyInterface.h"
#include "pypaillierStct.h"
#include "gmp.h"


static PyObject* toManagerObj(mpz_manager* c_mng)
{
	ManagerObject* py_mng;
	py_mng = PyObject_New(ManagerObject, &ManagerType);
	if (py_mng != NULL) {
		py_mng->row = c_mng->row;
		py_mng->col = c_mng->col;
		py_mng->ifenc = c_mng->ifenc;
		py_mng->ifmal = c_mng->ifmal;
		py_mng->field = PyMpz_FromMpz(c_mng->field, 0);
		if (py_mng->field == NULL) {
			Py_DECREF(py_mng);
			return NULL;
		}
	}
	return (PyObject*)py_mng;
}

/* key gen */
static PyObject* py_paillier_keygen(PyObject* self, PyObject* args) {

	int len;
	eggroll_public_key* pub = 
		(eggroll_public_key*)malloc(sizeof(eggroll_public_key));

	eggroll_private_key* priv = 
		(eggroll_private_key*)malloc(sizeof(eggroll_private_key));

	eggroll_public_init(pub);
	eggroll_private_init(priv);

	/*PyObject* x;
	if (!PyArg_ParseTuple(args, "i", &len)) {
		return NULL;
	}*/
	//generate keys
	eggroll_keygen_stable(pub, priv, 1024);
	//gmp_printf("pub key :%Zd\n", pub->n);

	PyObject* pArgs = PyTuple_New(2);
	PyTuple_SetItem(pArgs, 0, PyPublicKey_FromPublicKey(pub, 0));
	PyTuple_SetItem(pArgs, 1, PyPrivateKey_FromPrivateKey(priv, 0));

	return pArgs;
}

static PyObject* py_paillier_init(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	int tag;
	PyObject* x;
	PyObject* py_csv;
	PyObject* py_pub;
	Py_buffer py_view;

	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));

	/* python get size */
	if (!PyArg_ParseTuple(args, "OO", &py_csv, &py_pub))
		return NULL;

	if (PyObject_GetBuffer(py_csv, &py_view,
		PyBUF_ANY_CONTIGUOUS | PyBUF_FORMAT) == -1)
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;

	/* switch fp64 or int64*/
	tag = (!strcmp(py_view.format, "l")||!strcmp(py_view.format, "d")) ? 1 : 0;
	tag = !strcmp(py_view.format, "d") ? 2 : tag;

	if (!tag)
	{
		PyErr_SetString(PyExc_TypeError, "Expected an array of long int or float");
		PyBuffer_Release(&py_view);
		return NULL;
	}

	row = py_view.shape[0];
	col = py_view.shape[1];

	///* malloc */
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));

	//printf("[init]:col = %d, row = %d\n", col, row);

	initManager(c_mng, col, row, 3072);

	/* init int */
	if (tag == 1)
	{
		int64_t* c_buf = (int64_t*)py_view.buf;
		for (i = 0; i < col * row; i++)
		{
			c_mng->exp[i] = encode_int64(c_pub,
				c_mng->field[i], c_buf[i], -1, -1);
			//gmp_printf("[cpu coding ]: %Zd\n", c_mng->field[i]);

		}

		/*for (i = 0; i < row ; i++)
		{
			for ( j = 0; j < col; j++)
			{
				printf("%d ", c_buf[i * col + j]);
			}
			printf("\n");
		}
		printf("\n");
		printf("\n");*/

	}
	else
	{
		float64_t* c_buf = (float64_t*)py_view.buf;
		for (i = 0; i < col * row; i++)
		{
			//printf("[init] val : %lf\n", c_buf[i]);
			c_mng->exp[i] = encode_float64(c_pub,
				c_mng->field[i], c_buf[i], -1, -1);
			/*if (i == 0)
			{
				printf("[encode]src: %lf \n", c_buf[i]);
				printf("[encode]: %d \n", c_mng->exp[i]);
				gmp_printf("[encode]: %Zd \n", c_mng->field[i]);
			}*/

			/*gmp_printf("[+++cpu coding ]: %Zd\n", c_mng->field[i]);
			printf("[+++cpu coding exp ]: %d\n", c_mng->exp[i]);*/

		}
	/*	for (i = 0; i < col * row; i++)
		{
			printf("%lf\n", c_buf[i]);
		}*/

	}
	
	/*	ManagerObject* obj = toManagerObj(c_mng);
	return (PyObject*)obj;
*/
	//printf("[init]:size = %d, alloc = %d\n", c_mng->field[0]->_mp_size, c_mng->field[0]->_mp_alloc);
	return PyMpzField_FromMpzField(c_mng, 0);
}

static PyObject* py_paillier_use_fixkey(PyObject* self, PyObject* args)
{
	int len;
	eggroll_public_key* pub =
		(eggroll_public_key*)malloc(sizeof(eggroll_public_key));

	eggroll_private_key* priv =
		(eggroll_private_key*)malloc(sizeof(eggroll_private_key));

	eggroll_public_init(pub);
	eggroll_private_init(priv);

	/*PyObject* x;
	if (!PyArg_ParseTuple(args, "i", &len)) {
		return NULL;
	}*/
	//generate keys
	eggroll_keygen_stable(pub, priv, 1024);
	//gmp_printf("pub key :%Zd\n", pub->n);

	PyObject* pArgs = PyTuple_New(2);
	PyTuple_SetItem(pArgs, 0, PyPublicKey_FromPublicKey(pub, 0));
	PyTuple_SetItem(pArgs, 1, PyPrivateKey_FromPrivateKey(priv, 0));

	return pArgs;

}

//static PyObject* py_paillier_encrpyt(PyObject* self, PyObject* args)
//{
//	int i, j;
//	int row, col;
//	PyObject* py_mng;
//	PyObject* py_pub;
//
//	mpz_manager* c_mng = malloc(sizeof(mpz_manager));
//	mpz_manager* c_res_mng = malloc(sizeof(mpz_manager));
//	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
//	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));
//
//	/* Get the passed Python object */
//	if (!PyArg_ParseTuple(args, "OO", &py_mng, &py_pub))
//		return NULL;
//
//	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
//		return NULL;
//	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
//		return NULL;
//
//	/* accert */
//	if (c_mng->ifenc)
//	{
//		printf("Mat already encrypt!\n");
//		return py_mng;
//	}
//	if (!c_mng->ifmal)
//	{
//		printf("Mat didn't malloc\n");
//		return py_mng;
//	}
//
//	//rToC(c_buf, row, col);
//	//printf("[encrpy] :c_mng->exp[0] = %ld\n", c_mng->exp[0]);
//
//	/*for (i = 0; i < row; i++)
//	{
//		for (j = 0; j < col; j++)
//		{
//			printf("%ld ", c_buf[i * col + j]);
//		}
//		printf("\n");
//	}*/
//
//	//printf("format = %s dim = %d, row = %d , col = %d\n", 
//	//	py_view.format,py_view.ndim, py_view.shape[0], py_view.shape[1]);
//
//	/* init */
//	initManager(c_res_mng, c_mng->col, c_mng->row, 2048);
//	
//	dataEncrpyt(c_res_mng, c_mng, c_pub, c_mng->col, c_mng->row);
//	c_res_mng->ifenc = 1;
//
//
//	//gmp_printf("[!!!!!!!!!!!!cpu encrpyt]: %Zd\n",c_res_mng->field[0]);
//
//
//	return PyMpzField_FromMpzField(c_res_mng, 0);
//}


static PyObject* py_paillier_encrpyt(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_mng;
	PyObject* py_pub;
	
	mpz_manager* c_mng = malloc(sizeof(mpz_manager));
	mpz_manager* c_res_mng = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OO", &py_mng, &py_pub))
		return NULL;

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;

	/* accert */
	if (c_mng->ifenc)
	{
		printf("Mat already encrypt!\n");
		return py_mng;
	}
	if (!c_mng->ifmal)
	{
		printf("Mat didn't malloc\n");
		return py_mng;
	}

	/* init */
	initManager(c_res_mng, c_mng->col, c_mng->row, 3072);

	dataEncrpyt(c_res_mng, c_mng, c_pub, c_mng->col, c_mng->row);
	c_res_mng->ifenc = 1;


	return PyMpzField_FromMpzField(c_res_mng, 0);
}

static PyObject* py_paillier_raw_encrpyt(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_mng;
	PyObject* py_csv;
	PyObject* py_pub;
	Py_buffer py_view;

	mpz_manager* c_mng = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOO", &py_mng, &py_csv, &py_pub))
		return NULL;

	if (PyObject_GetBuffer(py_csv, &py_view,
		PyBUF_ANY_CONTIGUOUS | PyBUF_FORMAT) == -1)
		return NULL;

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;

	if (strcmp(py_view.format, "l") != 0)
	{
		PyErr_SetString(PyExc_TypeError, "Expected an array of long int ");
		PyBuffer_Release(&py_view);
		return NULL;
	}

	uint64_t* c_buf = (uint64_t*)py_view.buf;
	row = py_view.shape[0];
	col = py_view.shape[1];
	printf("py_view.format = %s, col = %d row = %d\n", py_view.format, col, row);


	//rToC(c_buf, row, col);
	/* accert */
	if (row != c_mng->row || col != c_mng->col)
	{
		printf("malloc size is diff from csv size!\n");
		return NULL;
	}

	dataRawEncrpyt(c_mng->field, c_buf, c_pub, col, row);

	return py_mng;

}

static PyObject* py_paillier_decrpyt(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_mng;
	PyObject* py_mng_dec;
	PyObject* py_pub;
	PyObject* py_priv;

	mpz_manager* c_mng = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_dec = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOO", &py_mng, &py_pub, &py_priv))
		return NULL;

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	row = c_mng->row;
	col = c_mng->col;

	printf("[~~~~~~~~~~~~]: col = %d  row = %d\n", col, row);

	/* malloc result */
	initManager(c_mng_dec, col, row, 2048);

	/* decrypt */
	dataDecrpyt(c_mng, c_mng_dec, c_pub, c_priv, col, row);

	return PyMpzField_FromMpzField(c_mng_dec, 0);

}

static PyObject* py_paillier_decode(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_mng;
	PyObject* py_mng_dec;
	PyObject* py_pub;
	PyObject* py_priv;

	mpz_manager* c_mng = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOO", &py_mng, &py_pub, &py_priv))
		return NULL;

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	row = c_mng->row;
	col = c_mng->col;

	//printf("[~~~~~~~~~~~~]: col = %d  row = %d\n", col, row);

	double* c_res = (double*)malloc(sizeof(double) * row * col);

	/* decrypt */
	dataDecode(c_mng, c_res, c_pub, c_priv, col, row);


	Point* a = (Point*)malloc(sizeof(Point));
	a->x = 1;
	a->y = 1;

	return PyPoint_FromPoint(a, 1);

}

static PyObject* py_paillier_mul_c_eql(PyObject* self, PyObject* args)
{
	int i, j;
	int row_l, col_l;
	int row_r, col_r;
	PyObject* py_mng_l;
	PyObject* py_mng_r;

	PyObject* py_pub;
	PyObject* py_priv;
	Py_buffer py_view;

	mpz_manager* c_mng_l = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_r = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_res = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	printf("MULLLLLLLLLLLLLLLLLLLLLLLLLLLL\n");

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOOO", &py_mng_l, &py_mng_r, &py_pub, &py_priv))
		return NULL;

	if (!(c_mng_l = PyMpzField_AsMpzField(py_mng_l)))
		return NULL;
	if (!(c_mng_r = PyMpzField_AsMpzField(py_mng_r)))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	///////* accert */
	if (c_mng_l->col != c_mng_r->col)
	{
		printf("Mat's row is not equal to data's row!\n");
		printf("l (%d %d)\n", c_mng_l->col, c_mng_l->row);
		printf("r (%d %d)\n", c_mng_r->col, c_mng_r->row);
		return py_mng_l;
	}
	
	col_l = c_mng_l->col;
	row_l = c_mng_l->row;
	col_r = c_mng_r->col;
	row_r = c_mng_r->row;

	printf("col_l = %d, row_l = %d, col_r = %d, row_r =  %d \n",
		c_mng_l->col, c_mng_l->row, c_mng_r->col, c_mng_r->row);

	///* init result data */
	initManager(c_mng_res, row_l, row_r, 2048);

	///* calc */
	matMul(c_mng_res, c_mng_l, c_mng_r, c_pub,
		c_priv, col_l, row_l);


	return PyMpzField_FromMpzField(c_mng_res, 0);
}

static PyObject* py_paillier_mul_r_eql(PyObject* self, PyObject* args)
{
	int i, j;
	int row_l, col_l;
	int row_r, col_r;
	PyObject* py_mng_l;
	PyObject* py_mng_r;

	PyObject* py_pub;
	PyObject* py_priv;
	Py_buffer py_view;

	mpz_manager* c_mng_l = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_r = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_res = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));
	
	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOOO", &py_mng_l, &py_mng_r, &py_pub, &py_priv))
		return NULL;

	if (!(c_mng_l = PyMpzField_AsMpzField(py_mng_l)))
		return NULL;
	if (!(c_mng_r = PyMpzField_AsMpzField(py_mng_r)))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	///////* accert *
	// combine by row
	if (c_mng_l->row != c_mng_r->row)
	{
		printf("Mat's row is not equal to data's row!\n");
		printf("l (%d %d)\n", c_mng_l->col, c_mng_l->row);
		printf("r (%d %d)\n", c_mng_r->col, c_mng_r->row);
		return py_mng_l;
	}

	printf("col_l = %d, row_l = %d, col_r = %d, row_r =  %d \n",
		c_mng_l->col, c_mng_l->row, c_mng_r->col, c_mng_r->row);

	col_l = c_mng_l->col;
	row_l = c_mng_l->row;
	col_r = c_mng_r->col;
	row_r = c_mng_r->row;

	///* init result data */
	initManager(c_mng_res, col_l, col_r, 2048);

	printf("[matmul]: r_eql here!\n");


	///* calc */
	matMul(c_mng_res, c_mng_l, c_mng_r, c_pub,
		c_priv, col_l, row_l);

	return PyMpzField_FromMpzField(c_mng_res, 0);
}

static PyObject* py_paillier_dotmul(PyObject* self, PyObject* args)
{
	int i, j;
	int row_l, col_l;
	int row_r, col_r;
	PyObject* py_mng_l;
	PyObject* py_mng_r;
	PyObject* py_pub;
	PyObject* py_priv;

	mpz_manager* c_mng_l = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_r = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_res = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOOO", &py_mng_l, &py_mng_r, &py_pub, &py_priv))
		return NULL;

	if (!(c_mng_l = PyMpzField_AsMpzField(py_mng_l)))
		return NULL;
	if (!(c_mng_r = PyMpzField_AsMpzField(py_mng_r)))
		return NULL;

	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	row_l = c_mng_l->row;
	col_l = c_mng_l->col;
	row_r = c_mng_r->row;
	col_r = c_mng_r->col;

	///* accert */
	assert(row_l == row_r);
	assert(col_l == col_r);
	assert(c_mng_r->ifenc < 1);

	/* init result data */
	initManager(c_mng_res, col_l, row_l, 2048);

	///* calc */
	matDotMul(c_mng_res, c_mng_l, c_mng_r, c_pub,
		c_priv, col_l, row_l);

	return PyMpzField_FromMpzField(c_mng_res, 0);
}

static PyObject* py_paillier_slcmul(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_mat;
	PyObject* py_pub;
	PyObject* py_priv;

	float64_t c_slc = 0.f;

	mpz_manager* c_mat= malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_res = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));


	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OdOO", &py_mat, &c_slc, &py_pub, &py_priv))
		return NULL;

	if (!(c_mat = PyMpzField_AsMpzField(py_mat)))
		return NULL;

	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	row = c_mat->row;
	col = c_mat->col;

	///* accert */
	assert(c_mat->ifenc > 0);

	///* init result data */
	initManager(c_mng_res, col, row, 2048);


	//printf("[mat mul] ifenc: %d\n", c_mat->ifenc);

	int64_t _val;


	for ( i = 0; i < c_mat->col * c_mat->row; i++)
	{
		//eggroll_raw_decrypt(c_pub, c_priv, c_mat->field[i], c_mng_res->field[i]);
		//decode_int64(&_val, c_mng_res->field[i], c_mat->exp[i]);

		/*gmp_printf("[mat mul]exp = %Zd\n", c_mat->field[i]);
		printf("[mat mul]exp = %ld\n", c_mat->exp[i]);*/
		//gmp_printf("[mat mul] : %Zd\n", c_mat->field[i]);
	}

	///* calc */
	matSlcMul(c_mng_res, c_mat, c_slc, c_pub, c_priv,
		col, row);

	return PyMpzField_FromMpzField(c_mng_res, 0);
}

static PyObject* py_paillier_obf(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_mng;
	PyObject* py_pub;
	PyObject* py_priv;

	mpz_manager* c_mng = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOO", &py_mng, &py_pub, &py_priv))
		return NULL;

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;

	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	row = c_mng->row;
	col = c_mng->col;
	
	///* calc */
	dataObfuscator(c_mng->field, c_pub, c_priv, col, row);
	
	return py_mng;
}

static PyObject* py_paillier_dotadd(PyObject* self, PyObject* args)
{
	int i, j;
	int row_l, col_l;
	int row_r, col_r;
	PyObject* py_mng_l;
	PyObject* py_mng_r;
	PyObject* py_pub;
	PyObject* py_priv;

	mpz_manager* c_mng_l = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_r = malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_res = malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOOO", &py_mng_l, &py_mng_r, &py_pub, &py_priv))
		return NULL;

	if (!(c_mng_l = PyMpzField_AsMpzField(py_mng_l)))
		return NULL;
	if (!(c_mng_r = PyMpzField_AsMpzField(py_mng_r)))
		return NULL;

	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	row_l = c_mng_l->row;
	col_l = c_mng_l->col;
	row_r = c_mng_r->row;
	col_r = c_mng_r->col;

	///* accert */
	assert(row_l == row_r);
	assert(col_l == col_r);

	encrpytProtect(c_mng_l, c_pub);
	encrpytProtect(c_mng_r, c_pub);

	///* init result data */
	initManager(c_mng_res, col_l, row_l, 2048);

	///* calc */
	matDotAdd(c_mng_res, c_mng_l,
		c_mng_r, c_pub, c_priv, col_l, row_l);

	return PyMpzField_FromMpzField(c_mng_res, 0);
}

static PyObject* py_paillier_free(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_mng;

	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "O", &py_mng))
		return NULL;

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;

	row = c_mng->row;
	col = c_mng->col;

	///* accert */
	assert(c_mng->ifmal == 1);

	for ( i = 0; i < col * row; i++)
	{
		mpz_clear(c_mng->field[i]);
	}
	free(c_mng->exp);
	free(c_mng);

	
	Point* a = (Point*)malloc(sizeof(Point));
	a->x = 1;
	a->y = 1;

	return PyPoint_FromPoint(a, 1);
}

/* tool : remove optionally */

static PyObject* py_paillier_print(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col, idx;
	PyObject* py_mng;
	PyObject* py_pub;
	PyObject* py_priv;

	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OOO", &py_mng, &py_pub, &py_priv))
	{
		printf("Roll_paillier_tensor Print must be py_mng object\n");
		return NULL;
	}

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;


	row = c_mng->row;
	col = c_mng->col;

	///* accert */
	mpz_t _r;
	mpz_init(_r);

	if (c_mng->ifenc)
	{
		printf("========[decrypt] [decode] [%d X %d]=========\n", row, col);
		for (i = 0; i < row; i++)
		{
			for (j = 0; j < col; j++)
			{
				idx = i * col + j;

				eggroll_raw_decrypt(c_pub, c_priv, c_mng->field[idx], _r);
				float tmp = decode_float64(c_pub, _r, c_mng->exp[idx]);
				printf("%f ", tmp);
			}
			printf("\n");
		}
		printf("========[decrypt] [decode] [%d X %d]=========\n\n", row, col);

	}
	else
	{
		printf("========[decode] [%d X %d]=========\n", row, col);
		for (i = 0; i < row; i++)
		{
			for (j = 0; j < col; j++)
			{
				idx = i * col + j;
				float tmp = decode_float64(c_pub, c_mng->field[idx], c_mng->exp[idx]);
				printf("%f  ", tmp);
			}
			printf("\n");
		}
		printf("========[decode] [%d X %d]=========\n\n", row, col);


	}

	
	Point* a = (Point*)malloc(sizeof(Point));
	a->x = 1;
	a->y = 1;

	return PyPoint_FromPoint(a, 1);
}

static PyObject* py_Point(PyObject* self, PyObject* args) {

	Point* a;
	double p1, p2;
	if (!PyArg_ParseTuple(args, "dd", &p1, &p2)) {
		return NULL;
	}

	a = (Point*)malloc(sizeof(Point));
	a->x = p1;
	a->y = p2;

	return PyPoint_FromPoint(a, 1);
}

static PyObject* py_pointPrint(PyObject* self, PyObject* args) {

	Point* point_x;
	PyObject* x;
	if (!PyArg_ParseTuple(args, "O", &x)) {
		return NULL;
	}
	if (!(point_x = PyPoint_AsPoint(x))) {
		return NULL;
	}

	printf("Point: x = %f, y = %f\n", point_x->x, point_x->y);
	return x;
}

static PyObject* py_mpz(PyObject* self, PyObject* args) {

	mpz_t* p = (mpz_t*)malloc(sizeof(mpz_t));
	int x;
	if (!PyArg_ParseTuple(args, "i", &x)) {
		return NULL;
	}
	mpz_init_set_ui(p, x);

	printf("[gmp] alloc = %d\n", p[0]->_mp_alloc);
	printf("[gmp] size = %d\n", p[0]->_mp_size);
	printf("[gmp] buff = %ld\n", p[0]->_mp_d[0]);


	//return PyMpzPtr_FromMpzPtr(_P, 0);
	return PyMpz_FromMpz(p, 0);
}

static PyObject* py_mpzPrint(PyObject* self, PyObject* args) {

	mpz_t* gmp_x = (mpz_t*)malloc(sizeof(mpz_t));

	PyObject* x;
	if (!PyArg_ParseTuple(args, "O", &x)) {
		printf("------------\n");

		return NULL;
	}
	if (!(gmp_x = PyMpz_AsMpz(x))) {
		return NULL;
	}


	gmp_printf("[ggggggggggggggggmp]%Zd\n", gmp_x);

	return x;
}


/* about multi process */ 
static PyObject* py_paillier_dump_pub(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_pub;

	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "O", &py_pub))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	int plen = get_public_len(c_pub);

	//*serilize key*/
	void* dumping = malloc(sizeof(uint64_t) * (plen + 6));

	dumpingPub(c_pub, dumping);

	uint64_t* tmp = (uint64_t*)dumping;
	//for (i = 0; i < 6; i++)
	//{
	//	//printf("********%ld\n", tmp[i]);
	//}

	return PyBytes_FromStringAndSize(dumping, sizeof(uint64_t) * (plen + 6));

}

static PyObject* py_paillier_dump_priv(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_priv;

	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "O", &py_priv))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	int plen = get_private_len(c_priv);

	/*serilize key*/
	void* dumping = malloc(sizeof(uint64_t) * (plen + 8));

	dumpingPriv(c_priv, dumping);

	return PyBytes_FromStringAndSize(dumping, sizeof(uint64_t) * (plen + 8));
}

static PyObject* py_paillier_load_pub(PyObject* self, PyObject* args)
{
	int i, j;
	void* l_pub;

	Py_ssize_t size;
	PyObject* py_pub;
	eggroll_public_key* c_pub = (eggroll_public_key*)malloc(sizeof(eggroll_public_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#", &l_pub, &size))
		return NULL;

	loadPub(c_pub, (uint64_t*)l_pub);

	return PyPublicKey_FromPublicKey(c_pub, 0);
}

static PyObject* py_paillier_load_priv(PyObject* self, PyObject* args)
{
	int i, j;
	void* l_priv;

	Py_ssize_t size;
	PyObject* py_priv;
	eggroll_private_key* c_priv = (eggroll_private_key*)malloc(sizeof(eggroll_private_key));
	
	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#", &l_priv, &size))
		return NULL;

	loadPriv(c_priv, (uint64_t*)l_priv);

	return PyPrivateKey_FromPrivateKey(c_priv, 0);
}

//static PyObject* py_paillier_slice_n_dump(PyObject* self, PyObject* args)
//{
//	int i, j;
//	int k, l;
//	int blockNumx, blockNumy;
//	int blockDimx, blockDimy;
//	int nodNum;
//
//	uint64_t* dumpMap;
//
//	PyObject* py_mng;
//	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));
//	mpz_manager* c_mng_dump = (mpz_manager*)malloc(sizeof(mpz_manager));
//	/* Get the whole data*/
//
//	if (!PyArg_ParseTuple(args, "Oiii", &py_mng, &blockDimx, &blockDimy, &nodNum))
//		return NULL;
//	
//	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
//		return NULL;
//	
//	blockNumx = (c_mng->col + blockDimx - 1) / blockDimx;
//	blockNumy = (c_mng->row + blockDimy - 1) / blockDimy;
//
//	int limbs = (BIT2BYTE(2048)) / sizeof(uint64_t);
//	int n = blockNumx * blockDimx * blockNumy * blockDimy;
//	
//	/* realloc mpz field*/
//	dumpMap = (uint64_t*)malloc(sizeof(uint64_t) * n * limbs);
//	memset(dumpMap, 0, sizeof(uint64_t) * n * limbs);
//
//	printf("[slice]:limbs = %d , n = %d blockDimx = %d blockDimy = %d blockNumx = %d blockNumy = %d\n",
//		limbs, n, blockDimx, blockDimy, blockNumx, blockNumy);
//
//	printf("[slice]: c_mng.field[0] = %ld size = %d \n", c_mng->field[0]->_mp_d[0], c_mng->field[0]->_mp_size);
//	printf("[slice]: col = %d row = %d\n", c_mng->col, c_mng->row);
//
//
//	/*for (i = 0; i < n; i++)
//	{
//		printf("[slice]: c_mng.field[%d] = %ld \n", i, c_mng->field[i]->_mp_d[0]);
//	}*/
//
//
//	/* serilize */
//	int gx, gy, gpos, arrpos;
//	uint64_t* iter;
//	for (i = 0; i < blockNumy; i++)
//	{
//		for ( j = 0; j < blockNumx; j++)
//		{
//			for (k = 0; k < blockDimy; k++)
//			{
//				for (l = 0; l < blockDimx; l++)
//				{
//					gx = blockDimx * j + l;
//					gy = blockDimy * i + k;
//
//					//printf("%d %d %d ============\n", gx, gy, gpos);
//					// gy * (c_mng->col) + gx
////					if (gx == 0 && gy ==0)
////					{
////						gpos = gy * (blockDimx * blockNumx) + gx;
////						iter = dumpMap + limbs * gpos;
////
////						/*mpz_export((void*)iter, limbs, 1, sizeof(uint64_t),
////							0, 0, c_mng->field[0]);
////*/
////					}
//					//mpz_srcptr
//					if (gx < c_mng->col && gy < c_mng->row)
//					{
//						
//						//gpos = gy * (blockDimx * blockNumx) + gx;
//						arrpos = (k * blockDimx + l) + (j + i * blockNumx)
//							* (blockDimx * blockDimy);
//
//						iter = dumpMap + limbs * arrpos;
//						memcpy(iter, c_mng->field[gy * (c_mng->col) + gx]->_mp_d,
//							limbs * sizeof(uint64_t));
//						/*mpz_export(iter, limbs, -1, sizeof(uint64_t), 
//							0, 0, c_mng->field[gy * (c_mng->col) + gx]);*/
//						
//				
//						//printf("++++++++++++%d \t %ld\n", arrpos, iter[0]);
//
//					}
//				}
//			}
//		}
//	}
//
//	return PyBytes_FromStringAndSize((char*)dumpMap, sizeof(uint64_t)* n * limbs);
//
//	/*Point* a;
//	a = (Point*)malloc(sizeof(Point));
//	a->x = 1;
//	a->y = 2;
//
//	return PyPoint_FromPoint(a, 1);*/
//
//}


static PyObject* py_paillier_slice_n_dump(PyObject* self, PyObject* args)
{
	int i, j;
	int k, l;
	int blockNumx, blockNumy;
	int blockDimx, blockDimy;
	int nodNum;

	uint64_t* dumpMap;

	PyObject* py_mng;
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));
	mpz_manager* c_mng_dump = (mpz_manager*)malloc(sizeof(mpz_manager));
	/* Get the whole data*/

	if (!PyArg_ParseTuple(args, "Oiii", &py_mng, &blockDimx, &blockDimy, &nodNum))
		return NULL;

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;

	blockNumx = (c_mng->col + blockDimx - 1) / blockDimx;
	blockNumy = (c_mng->row + blockDimy - 1) / blockDimy;

	int limbs = (BIT2BYTE(2048)) / sizeof(uint64_t);
	int n = blockNumx * blockDimx * blockNumy * blockDimy;
	short headerSize = 4;

	/* realloc mpz field*/

	dumpMap = (uint64_t*)malloc(sizeof(uint64_t) * (n * limbs + headerSize + n));
	memset(dumpMap, 0, sizeof(uint64_t) * (n * limbs + headerSize + n));

	dumpMap[0] = (uint64_t)c_mng->row;
	dumpMap[1] = (uint64_t)c_mng->col;
	dumpMap[2] = 2048;
	dumpMap[3] = (uint64_t)c_mng->ifenc;

	printf("[slice_n_dump]:limbs = %d , n = %d blockDimx = %d blockDimy = %d blockNumx = %d blockNumy = %d\n",
		limbs, n, blockDimx, blockDimy, blockNumx, blockNumy);

	printf("[slice_n_dump]: c_mng.field[0] = %ld size = %d \n", c_mng->field[0]->_mp_d[0], c_mng->field[0]->_mp_size);
	printf("[slice_n_dump]: col = %d row = %d\n", dumpMap[1], dumpMap[0]);

	/* dump field n exp */
	int gx, gy, gpos, arrpos;
	uint64_t* bIter;
	uint64_t* eIter;
	for (i = 0; i < blockNumy; i++)
	{
		for (j = 0; j < blockNumx; j++)
		{
			for (k = 0; k < blockDimy; k++)
			{
				for (l = 0; l < blockDimx; l++)
				{
					gx = blockDimx * j + l;
					gy = blockDimy * i + k;

					//printf("%d %d %d ============\n", gx, gy, gpos);
					// gy * (c_mng->col) + gx
//					if (gx == 0 && gy ==0)
//					{
//						gpos = gy * (blockDimx * blockNumx) + gx;
//						iter = dumpMap + limbs * gpos;
//
//						/*mpz_export((void*)iter, limbs, 1, sizeof(uint64_t),
//							0, 0, c_mng->field[0]);
//*/
//					}
					//mpz_srcptr
					if (gx < c_mng->col && gy < c_mng->row)
					{
						//gpos = gy * (blockDimx * blockNumx) + gx;
						arrpos = (k * blockDimx + l) + (j + i * blockNumx)
							* (blockDimx * blockDimy);

						/* dump filed*/
						bIter = dumpMap + headerSize + limbs * arrpos;
						memcpy(bIter, c_mng->field[gy * (c_mng->col) + gx]->_mp_d,
							limbs * sizeof(uint64_t));

						/* dump exp */
						eIter = dumpMap + headerSize + limbs * n + arrpos;
						*eIter = c_mng->exp[gy * (c_mng->col) + gx];

						/*printf("[silce n dump] : %ld\n", *eIter);*/

					}
				}
			}
		}
	}

	return PyBytes_FromStringAndSize((char*)dumpMap, sizeof(uint64_t) * (n * limbs + headerSize + n));
}


static PyObject* py_paillier_slice(PyObject* self, PyObject* args)
{
	int i, j;
	int k, l;
	int blockNumx, blockNumy;
	int blockDimx, blockDimy;
	int nodNum;

	PyObject* py_mng;
	mpz_manager* c_mng_slice = (mpz_manager*)malloc(sizeof(mpz_manager));
	/* Get the whole data*/

	if (!PyArg_ParseTuple(args, "Oiii", &py_mng, &blockDimx, &blockDimy, &nodNum))
		return NULL;

	ManagerObject* _obj = (ManagerObject*)py_mng;

	blockNumx = (_obj->col + blockDimx - 1) / blockDimx;
	blockNumy = (_obj->row + blockDimy - 1) / blockDimy;

	int newCol = blockDimx * blockDimx;
	int newRow = blockDimy * blockDimy;

	initManager(c_mng_slice, newCol, newRow, 2048);

	int byte = BIT2BYTE(2048);

	mpz_t* mng_field = PyMpz_AsMpz(_obj->field);

	int gx, gy, gpos, arrpos;
	uint64_t* iter;
	for (i = 0; i < blockNumy; i++)
	{
		for (j = 0; j < blockNumx; j++)
		{
			for (k = 0; k < blockDimy; k++)
			{
				for (l = 0; l < blockDimx; l++)
				{
					gx = blockDimx * j + l;
					gy = blockDimy * i + k;

					if (gx < _obj->col && gy < _obj->row)
					{
						//gpos = gy * (blockDimx * blockNumx) + gx;
						arrpos = (k * blockDimx + l) + (j + i * blockNumx)
							* (blockDimx * blockDimy);

						memcpy(c_mng_slice->field[arrpos]->_mp_d,
							mng_field[gy * (_obj->col) + gx]->_mp_d, byte);
						c_mng_slice->field[arrpos]->_mp_alloc = mng_field[gy * (_obj->col) + gx]->_mp_alloc;
						c_mng_slice->field[arrpos]->_mp_size = mng_field[gy * (_obj->col) + gx]->_mp_size;

					}
					else 
					{
						mpz_set_ui(c_mng_slice->field[arrpos], 0);
					}
				}
			}
		}
	}


	for ( i = 0; i < newRow; i++)
	{
		for (j = 0; j < newCol; j++)
		{
			gmp_printf("%Zd\n", c_mng_slice->field[i * newCol + j]);
		}
		printf("\n");
	}

	
	return toManagerObj(c_mng_slice);
}

static PyObject* py_paillier_dump(PyObject* self, PyObject* args)
{
	int i, j;
	int col, row;

	uint64_t* dumpMap;
	PyObject* py_mng;
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));
	/* Get the whole data*/

	if (!PyArg_ParseTuple(args, "O", &py_mng))
		return NULL;

	if (!(c_mng = PyMpzField_AsMpzField(py_mng)))
		return NULL;

	col = c_mng->col;
	row = c_mng->row;

	int limbs = (BIT2BYTE(3072)) / sizeof(uint64_t);
	short headerSize = 4;

	long int dumpSize = (row * col * limbs + headerSize) + (row * col);
	dumpMap = (uint64_t*)malloc(sizeof(uint64_t) * dumpSize);

	/* serilize */
	uint64_t* iter;
	
	/* dump header */
	dumpMap[0] = (uint64_t)row;
	dumpMap[1] = (uint64_t)col;
	dumpMap[2] = (uint64_t)3072;
	dumpMap[3] = (uint64_t)c_mng->ifenc;


	/* dump base */
	for (i = 0; i < row * col; i++)
	{
		iter = dumpMap + headerSize + i * limbs;
		memcpy(iter, c_mng->field[i]->_mp_d,
			limbs * sizeof(uint64_t));
	}

	/* dump exp */
	iter = dumpMap + (row * col * limbs + headerSize);
	memcpy(iter, c_mng->exp, row * col * sizeof(uint64_t));

	//for (i = 0; i < row * col; i++)
	//{
	//	printf("[dump] exp[%d] = %ld \n", i, c_mng->exp[i]);
	//}

	/*printf("[dump] exp[%d] = %ld \n", 3, c_mng->exp[3]);
	printf("[dump] iter = %ld \n", iter[3]);*/

	return PyBytes_FromStringAndSize((char*)dumpMap, sizeof(uint64_t) * dumpSize);
}

static PyObject* py_paillier_melt(PyObject* self, PyObject* args)
{
	int i, j;
	uint64_t* l_data;

	int rank;
	int yDim, yNum;
	
	Py_ssize_t size;
	PyObject* py_pub;
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#iii", &l_data, &size, &yDim, &yNum, &rank))
		return NULL;


	printf("yDim = %d yNum = %d \n", yDim, yNum);

	c_mng->col = yNum;
	c_mng->row = 1;

	c_mng->ifenc = 1;
	c_mng->ifmal = 1;
	c_mng->field = (mpz_t*)malloc(sizeof(mpz_t) * c_mng->col * c_mng->row);
	for ( i = 0; i < c_mng->col * c_mng->row; i++)
	{
		mpz_init_set_ui(c_mng->field[i], 0);
		//mpz_setbit(c_mng->field[i], 2048);
	}
	
	mpz_t pickle, zero, res;
	mpz_init2(pickle, 2048);
	mpz_init(res);
	mpz_init_set_ui(zero, 10);
	//mpz_setbit(zero, 2048);
	uint64_t* iter;
	for (i = 0; i < yNum; i++) //4
	{
 		for (j = 0; j < yDim; j++) //2
		{
			iter = l_data + 32 * (i * yDim + j);
			mpz_import(pickle, 32, -1, sizeof(uint64_t), 0, 0, iter);
			//gmp_printf("**********gmp %Zd\n", pickle);
			mpz_add(c_mng->field[(i/2)*yDim + j],
				c_mng->field[(i / 2) * yDim + j], pickle);
		}

	}
	for ( i = 0; i < 4; i++)
	{
		gmp_printf("@@@@@@@@@@@gmp %Zd\n", c_mng->field[i]);
	}

	return PyMpzField_FromMpzField(c_mng, 0);
}

static PyObject* py_paillier_melt_one(PyObject* self, PyObject* args)
{
	int i, j;
	uint64_t* l_data;

	int rank;
	int yDim, yNum;

	Py_ssize_t size;
	PyObject* py_pub;
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#", &l_data, &size))
		return NULL;

	
	return PyMpzField_FromMpzField(c_mng, 0);
}

static PyObject* py_paillier_load(PyObject* self, PyObject* args)
{
	int i, j;
	uint64_t* l_data;

	int row, col, bit;
	short headerSize = 4;
	Py_ssize_t size;
	PyObject* py_pub;
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#", &l_data, &size))
		return NULL;

	uint64_t* iter;

	/* load header */
	row = (int)l_data[0];
	col = (int)l_data[1];
	bit = (int)l_data[2];


	/* init mng */
	initManager(c_mng, col, row, bit);
	c_mng->ifenc = (int)l_data[3];

	int nlimb = BIT2BYTE(bit);
	int N64 = nlimb / 8;

	//printf("[load] col = %d  row = %d bit = %d N64 = %d\n", col, row, bit, N64);


	/* load field */
	for (i = 0; i < row * col; i++)
	{
		iter = l_data + headerSize + N64 * i;
		mpz_import(c_mng->field[i], N64, -1, sizeof(uint64_t), 0, 0, iter);

	/*	printf("[load] exp : %ld\n", c_mng->exp[i]);
		printf("[load] size : %d\n", c_mng->field[i]->_mp_size);
		printf("[load] alloc : %d\n", c_mng->field[i]->_mp_alloc);
		gmp_printf("[load] : %Zd\n", c_mng->field[i]);*/

		//if (rank == 3 && i == 1 && row==1)
		//{
		//	printf("[sub rank 1]:row = %d col = %d\n", c_mng->row, c_mng->col);
		//	printf("[sub rank 1]:c_mng->field[0] = %ld\n", c_mng->field[i]->_mp_d[0]);
		//	printf("[sub rank 1]:l_data = %ld\n", iter[0]);
		//	gmp_printf("[gmpprintf]: +++++++++++++++%Zd\n", c_mng->field[i]);
		//	/*for (i = 0; i < 37; i++)
		//	{
		//		printf("#####################%ld\n", l_data[i]);
		//	}*/
		//}
	}

	/* load exp */
	iter = l_data + headerSize + N64 * col * row;
	memcpy(c_mng->exp, iter, col * row * sizeof(uint64_t));

	for (i = 0; i < row * col; i++)
	{
		//printf("[cpu load] exp[%d] = %ld \n", i, c_mng->exp[i]);
		//gmp_printf("[cpu load] exp[%d] = %Zd \n", i, c_mng->field[i]);
	}


	return PyMpzField_FromMpzField(c_mng, 0);
}


/* new function */

static PyObject* py_paillier_make_header(PyObject* self, PyObject* args)
{
	int row, col;
	int bit, tag;

	/* python get size */
	if (!PyArg_ParseTuple(args, "iiii", &row, &col, &bit, &tag))
		return NULL;

	//malloc 
	void* _arr = malloc(sizeof(int64_t) * 4);

	((uint64_t*)_arr)[0] = (int64_t)row;
	((uint64_t*)_arr)[1] = (int64_t)col;
	((uint64_t*)_arr)[2] = (int64_t)bit;
	((uint64_t*)_arr)[3] = (int64_t)tag;

	return PyBytes_FromStringAndSize((char*)_arr, 4 * sizeof(uint64_t));
}

static PyObject* py_paillier_slice_csv(PyObject* self, PyObject* args)
{
	int i, j;
	int k, l;
	int row, col;
	int blockDimx, blockDimy;
	int blockNumx, blockNumy;
	int tag;
	int headerSize = 4;

	PyObject* py_csv;
	Py_buffer py_view;

	/* python get size */
	if (!PyArg_ParseTuple(args, "Oii", &py_csv, &blockDimx, &blockDimy))
		return NULL;
	if (PyObject_GetBuffer(py_csv, &py_view,
		PyBUF_ANY_CONTIGUOUS | PyBUF_FORMAT) == -1)
		return NULL;

	/* switch fp64 or int64*/
	tag = (!strcmp(py_view.format, "l") || !strcmp(py_view.format, "d")) ? 1 : 0;
	tag = !strcmp(py_view.format, "d") ? 2 : tag;

	if (!tag)
	{
		PyErr_SetString(PyExc_TypeError, "Expected an array of long int or float");
		PyBuffer_Release(&py_view);
		return NULL;
	}

	row = py_view.shape[0];
	col = py_view.shape[1];

	blockNumx = (col + blockDimx - 1) / blockDimx;
	blockNumy = (row + blockDimy - 1) / blockDimy;

	printf("csz_row = %d csv_col = %d\n", row, col);
	printf("blockDimx = %d blockDimy = %d\n", blockDimx, blockDimy);
	printf("blockNumx = %d blockNumy = %d\n", blockNumx, blockNumy);


	int newCol = blockDimx * blockNumx;
	int newRow = blockDimy * blockNumy;

	//malloc 
	void* Byte_arr = malloc(sizeof(int64_t) * (newCol * newRow + headerSize));

	((uint64_t*)Byte_arr)[0] = row;
	((uint64_t*)Byte_arr)[1] = col;
	((uint64_t*)Byte_arr)[2] = 3072;
	((uint64_t*)Byte_arr)[3] = tag;

	int gx, gy;
	int arrpos;

	if (tag == 1)
	{
		int64_t* c_buf = (int64_t*)py_view.buf;
		int64_t* c_iter = ((int64_t*)Byte_arr) + headerSize;

		for (i = 0; i < blockNumy; i++)
		{
			for (j = 0; j < blockNumx; j++)
			{
				for (k = 0; k < blockDimy; k++)
				{
					for (l = 0; l < blockDimx; l++)
					{
						gx = blockDimx * j + l;
						gy = blockDimy * i + k;

						if (gx < col && gy < row)
						{
							arrpos = (k * blockDimx + l) + (j + i * blockNumx)
								* (blockDimx * blockDimy);

							c_iter[arrpos] = c_buf[gy * col + gx];

						
							//printf("[slice csv int]: [%d] [%d] : %ld\n", arrpos, gy * col + gx,
							//	c_buf[gy * col + gx]);

						}
						else
						{
							c_iter[arrpos] = 0;
						}
					}
				}
			}
		}


	}
	else
	{
		float64_t* c_buf = (float64_t*)py_view.buf;
		float64_t* c_iter = ((float64_t*)Byte_arr) + +headerSize;

		for (i = 0; i < blockNumy; i++)
		{
			for (j = 0; j < blockNumx; j++)
			{
				for (k = 0; k < blockDimy; k++)
				{
					for (l = 0; l < blockDimx; l++)
					{
						gx = blockDimx * j + l;
						gy = blockDimy * i + k;

						if (gx < col && gy < row)
						{
							arrpos = (k * blockDimx + l) + (j + i * blockNumx)
								* (blockDimx * blockDimy);

							c_iter[arrpos] = c_buf[gy * col + gx];

							/*printf("[slice csv float]: [%d] [%d] : %lf\n", arrpos, gy * col + gx,
							c_buf[gy * col + gx]);*/

						}
						else
						{
							c_iter[arrpos] = 0;
						}
					}
				}
			}
		}

	}

	PyObject* pArgs = PyTuple_New(4);

	PyTuple_SetItem(pArgs, 0, PyBytes_FromStringAndSize((char*)Byte_arr,
		sizeof(uint64_t)* (newCol* newRow + headerSize)));
	PyTuple_SetItem(pArgs, 1, Py_BuildValue("i", tag));
	PyTuple_SetItem(pArgs, 2, Py_BuildValue("i", row));
	PyTuple_SetItem(pArgs, 3, Py_BuildValue("i", col));

	return pArgs;
}

static PyObject* py_paillier_init_byte_csv(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	int bit, tag;
	short headerSize = 4;

	void* l_csv;
	Py_ssize_t size;
	PyObject* py_pub;

	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#O", &l_csv, &size, &py_pub))
		return NULL;

	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	

	/* dump header*/
	row = (int)((uint64_t*)l_csv)[0];
	col = (int)((uint64_t*)l_csv)[1];
	bit = (int)((uint64_t*)l_csv)[2];
	tag = (int)((uint64_t*)l_csv)[3];

	void* c_buf = (void*)((uint64_t*)l_csv + headerSize);

	///* malloc */
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));

	initManager(c_mng, col, row, bit);

	///* init int */
	if (tag == 1)
	{
		uint64_t* c_uint64 = (uint64_t*)c_buf;
		for (i = 0; i < col * row; i++)
		{
			c_mng->exp[i] = encode_int64(c_pub,
				c_mng->field[i], c_uint64[i], -1, -1);


			//printf("value = %ld \n", c_uint64[i]);

			/*gmp_printf("[cpu coding ]: %Zd\n", c_mng->field[i]);
			printf("[+++cpu c_buf]: %ld\n", c_buf[i]);
			printf("[+++cpu coding exp ]: %d\n", c_mng->exp[i]);*/
		}

		/*for (i = 0; i < row ; i++)
		{
			for ( j = 0; j < col; j++)
			{
				printf("%d ", c_buf[i * col + j]);
			}
			printf("\n");
		}
		printf("\n");
		printf("\n");*/

	}
	else
	{
		float64_t* c_float64 = (float64_t*)c_buf;

		for (i = 0; i < col * row; i++)
		{
			c_mng->exp[i] = encode_float64(c_pub,
				c_mng->field[i], c_float64[i], -1, -1);

			printf("value = %lf \n", c_float64[i]);

			/*if (i == 0)
			{
				printf("[encode]src: %lf \n", c_buf[i]);
				printf("[encode]: %d \n", c_mng->exp[i]);
				gmp_printf("[encode]: %Zd \n", c_mng->field[i]);
			}*/

			/*gmp_printf("[+++cpu coding ]: %Zd\n", c_mng->field[i]);
			printf("[+++cpu c_buf]: %ld\n", c_buf[i]);

			printf("[+++cpu coding exp ]: %d\n", c_mng->exp[i]);*/
		}
		/*for (i = 0; i < col * row; i++)
		{
			printf("%lf\n", c_buf[i]);
		}*/

	}

	return PyMpzField_FromMpzField(c_mng, 0);
}



