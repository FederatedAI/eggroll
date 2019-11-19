#pragma once
#include "Python.h"
#include "../paillier.h"
#include "../tools.h"
#include "../toPyInterface.h"
#include "../pypaillierStct.h"

#include "eggPaillier.h"

static PyObject* pyg_paillier_show(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	int tag;
	PyObject* _pyg;

	void** _gpu;
	/* python get size */
	if (!PyArg_ParseTuple(args, "Oi", &_pyg, &tag))
		return NULL;
	_gpu = PyCapsule_GetPointer(_pyg, "void");

	gpu_show(_gpu, tag);

	Point a;
	a.x = 1;
	a.y = 2;

	return PyPoint_FromPoint(&a, 0);

}

/* key gen */
static PyObject* pyg_paillier_keygen(PyObject* self, PyObject* args) {

	//cudaSetDevice(1);


	int len;
	eggroll_public_key* pub =
		(eggroll_public_key*)malloc(sizeof(eggroll_public_key));

	eggroll_private_key* priv =
		(eggroll_private_key*)malloc(sizeof(eggroll_private_key));

	eggroll_public_init(pub);
	eggroll_private_init(priv);

	eggroll_keygen_stable(pub, priv, 1024);
	//gmp_printf("pub key :%Zd\n", pub->n);

	/* ==============test==============*/
	void** d_pub = malloc(sizeof(void*));
	void** d_priv = malloc(sizeof(void*));

	//cudaMalloc((void**)& d_pub, sizeof(int));
	//cudaMalloc((void**)& d_priv, sizeof(int));

	//printf("[python 1]: d_pub = %0x\n", *d_pub);

	gpu_init_pub(pub, d_pub);
	gpu_init_priv(priv, d_priv);

	//printf("[python 2]: d_pub = %0x\n", *d_pub);

	PyObject* pArgs = PyTuple_New(2);
	PyTuple_SetItem(pArgs, 0, PyCapsule_New(d_pub, "void", NULL));
	PyTuple_SetItem(pArgs, 1, PyCapsule_New(d_priv, "void", NULL));

	return pArgs;

	/*Point a;
	a.x = 1;
	a.y = 2;

	return PyPoint_FromPoint(&a, 0);*/

}

/* encrpyt gpu*/
static PyObject* pyg_paillier_init(PyObject* self, PyObject* args) {

	//cudaSetDevice(1);

	int i, j;
	int row, col;
	int tag;
	PyObject* py_csv;
	Py_buffer py_view;
	PyObject* pyg_pub;

	void** gpu_pub;

	/* python get size */
	if (!PyArg_ParseTuple(args, "OO", &py_csv, &pyg_pub))
		return NULL;
	if (PyObject_GetBuffer(py_csv, &py_view,
		PyBUF_ANY_CONTIGUOUS | PyBUF_FORMAT) == -1)
		return NULL;

	/* load c_pub from dev */
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	gpu_pub = PyCapsule_GetPointer(pyg_pub, "void");
	eggroll_public_init(c_pub);
	load_devPub(c_pub, gpu_pub);

	//gmp_printf("c_pub.g = %Zd\n", c_pub->g);
	//gmp_printf("c_pub.n = %Zd\n", c_pub->n);


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

	/////* malloc */
	//printf("[gpu init]:col = %d, row = %d\n", col, row);

	void** d_mpz = (void**)malloc(sizeof(void*));
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));
	
	initManager(c_mng, col, row, 3072);

	/* init int */
	if (tag == 1)
	{
		int64_t* c_buf = (int64_t*)py_view.buf;
		for (i = 0; i < col * row; i++)
		{
			c_mng->exp[i] = encode_int64(c_pub,
				c_mng->field[i], c_buf[i], -1, -1);
		}

		//for (i = 0; i < col * row; i++)
		//{
		//	float tmp = decode_int64(c_pub, c_mng->field[i], c_mng->exp[i]);
		//	printf("[gpu]%f\n", tmp);
		//}

		gpu_init_mpz(col, row, c_mng, d_mpz);
	}
	else
	{
		float64_t* c_buf = (float64_t*)py_view.buf;
		for (i = 0; i < col * row; i++)
		{

			c_mng->exp[i] = encode_float64(c_pub,
				c_mng->field[i], c_buf[i], -1, -1);
			/*gmp_printf("[+++gpu coding ]: %Zd\n", c_mng->field[i]);
			printf("[+++gpu coding exp ]: %ld\n", c_mng->exp[i]);*/

		}
		gpu_init_mpz(col, row, c_mng, d_mpz);

	}

	return PyCapsule_New(d_mpz, "void", NULL);


}

static PyObject* pyg_paillier_byte_init(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;

	uint64_t* l_data;
	Py_ssize_t size;

	PyObject* pyg_pub;
	void** gpu_pub;

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#iiO", &l_data, &size, &col, &row, &pyg_pub))
		return NULL;

	/* load c_pub from dev */
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	gpu_pub = PyCapsule_GetPointer(pyg_pub, "void");
	eggroll_public_init(c_pub);
	load_devPub(c_pub, gpu_pub);

	float64_t* c_buf = (float64_t*)l_data;
	
	/* mpz_mng */
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));
	initManager(c_mng, col, row, 3072);
	
	/* gpu_mng */
	void** d_mpz = (void**)malloc(sizeof(void*));

	for (i = 0; i < row * col; i++)
	{
		c_mng->exp[i] = encode_float64(c_pub,
			c_mng->field[i], c_buf[i], -1, -1);
	}
	gpu_init_mpz(col, row, c_mng, d_mpz);

	return PyCapsule_New(d_mpz, "void", NULL);
}

static PyObject* pyg_paillier_byte_init_csv(PyObject* self, PyObject* args)
{
	int i, j;
	short row, col;
	int bit, tag;
	short headerSize = 4;

	void* l_csv;
	Py_ssize_t size;
	PyObject* pyg_pub;
	void** gpu_pub;

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#OO", &l_csv, &size, &pyg_pub))
		return NULL;

	/* load c_pub from dev */
	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	gpu_pub = PyCapsule_GetPointer(pyg_pub, "void");
	eggroll_public_init(c_pub);
	load_devPub(c_pub, gpu_pub);

	/* dump header*/
	row = ((uint64_t*)l_csv)[0];
	col = ((uint64_t*)l_csv)[1];
	bit = ((uint64_t*)l_csv)[2];
	tag = ((uint64_t*)l_csv)[3];

	

	/* mpz_mng */
	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));
	initManager(c_mng, col, row, 3072);
	
	/* gpu_mng */
	void** d_mpz = (void**)malloc(sizeof(void*));

	if (tag == 1)
	{
		uint64_t* c_buf = ((uint64_t*)l_csv + headerSize);
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
		float64_t* c_buf = ((float64_t*)l_csv + headerSize);
		printf("[init byte csv]: %d\n", tag);

		for (i = 0; i < col * row; i++)
		{
			c_mng->exp[i] = encode_float64(c_pub,
				c_mng->field[i], c_buf[i], -1, -1);
			/*gmp_printf("[+++cpu coding ]: %Zd\n", c_mng->field[i]);
			printf("[+++cpu coding exp ]: %d\n", c_mng->exp[i]);*/
		}

	}
	gpu_init_mpz(col, row, c_mng, d_mpz);

	return PyCapsule_New(d_mpz, "void", NULL);
}


static PyObject* pyg_paillier_encrypt(PyObject* self, PyObject* args) {

	int i, j;
	int row, col;
	int tag;
	PyObject* py_pub;
	PyObject* py_mng;

	void** g_pub;
	void** g_mat;

	/* python get size */
	if (!PyArg_ParseTuple(args, "OO", &py_mng, &py_pub))
		return NULL;

	g_pub = PyCapsule_GetPointer(py_pub, "void");
	g_mat = PyCapsule_GetPointer(py_mng, "void");

	///* malloc */
	void** g_res = (void**)malloc(sizeof(void*));
	
	///* init int */
	gpu_encrypt(g_pub, g_mat, g_res);

	return PyCapsule_New(g_res, "void", NULL);
	/*Point a;
	a.x = 1;
	a.y = 2;
	return PyPoint_FromPoint(&a, 0);
*/
}

static PyObject* pyg_paillier_decrypt(PyObject* self, PyObject* args) {

	int i, j;
	int row, col;
	int tag;
	PyObject* py_pub;
	PyObject* py_priv;
	PyObject* py_ciper;

	void** g_pub;
	void** g_priv;
	void** g_ciper;

	/* python get size */
	if (!PyArg_ParseTuple(args, "OOO", &py_ciper, &py_pub, &py_priv))
		return NULL;

	g_pub = (void**)PyCapsule_GetPointer(py_pub, "void");
	g_priv = (void**)PyCapsule_GetPointer(py_priv, "void");
	g_ciper = (void**)PyCapsule_GetPointer(py_ciper, "void");

	///* malloc */
	void** g_plain = (void**)malloc(sizeof(void*));

	/* init int */
	gpu_decrypt(g_pub, g_priv, g_ciper, g_plain);

	return PyCapsule_New(g_plain, "void", NULL);
}


/* calc */
static PyObject* pyg_paillier_dotadd(PyObject* self, PyObject* args) {

	int i, j;
	int row, col;
	int tag, ret;
	PyObject* py_pub;
	PyObject* py_mat_l;
	PyObject* py_mat_r;

	void** g_pub;
	void** g_mat_l;
	void** g_mat_r;

	/* python get size */
	if (!PyArg_ParseTuple(args, "OOO", &py_mat_l, &py_mat_r, &py_pub))
		return NULL;

	g_mat_l = PyCapsule_GetPointer(py_mat_l, "void");
	g_mat_r = PyCapsule_GetPointer(py_mat_r, "void");
	g_pub = PyCapsule_GetPointer(py_pub, "void");

	///////* malloc */
	void** g_res = (void**)malloc(sizeof(void*));
	////
	///* init int */
	ret = gpu_dotadd(g_pub, g_mat_l, g_mat_r, g_res);

	//printf("[gpu dot add].........%d\n");
	////printf("[gpu dot add].........%d\n", ret);

	return PyCapsule_New(g_res, "void", NULL);

}

static PyObject* pyg_paillier_dotmul(PyObject* self, PyObject* args) {

	int i, j;
	int row, col;
	int tag;
	PyObject* py_pub;
	PyObject* py_mat_l;
	PyObject* py_mat_r;

	void** g_pub;
	void** g_mat_l;
	void** g_mat_r;

	/* python get size */
	if (!PyArg_ParseTuple(args, "OOO", &py_mat_l, &py_mat_r, &py_pub))
		return NULL;

	g_pub = PyCapsule_GetPointer(py_pub, "void");
	g_mat_l = PyCapsule_GetPointer(py_mat_l, "void");
	g_mat_r = PyCapsule_GetPointer(py_mat_r, "void");

	///* malloc */
	void** g_res = (void**)malloc(sizeof(void*));

	/* init int */
	gpu_dotmul(g_pub, g_mat_l, g_mat_r, g_res);

	return PyCapsule_New(g_res, "void", NULL);
}


static PyObject* pyg_paillier_matmul_c_eql(PyObject* self, PyObject* args) {

	int i, j;
	int row, col;
	int tag;
	PyObject* py_pub;
	PyObject* py_mat_l;
	PyObject* py_mat_r;

	void** g_pub;
	void** g_mat_l;
	void** g_mat_r;

	/* python get size */
	if (!PyArg_ParseTuple(args, "OOO", &py_mat_l, &py_mat_r, &py_pub))
		return NULL;

	g_pub = PyCapsule_GetPointer(py_pub, "void");
	g_mat_l = PyCapsule_GetPointer(py_mat_l, "void");
	g_mat_r = PyCapsule_GetPointer(py_mat_r, "void");

	///* malloc */
	void** g_res = (void**)malloc(sizeof(void*));

	/* init int */
	gpu_matmul_c_eql(g_pub, g_mat_l, g_mat_r, g_res);

	return PyCapsule_New(g_res, "void", NULL);
}


static PyObject* pyg_paillier_decode(PyObject* self, PyObject* args) {

	int i, j;
	int row, col;
	int tag;
	PyObject* py_pub;
	PyObject* py_priv;
	PyObject* py_plain;

	void** g_pub;
	void** g_plain;

	/* python get size */
	if (!PyArg_ParseTuple(args, "OO", &py_plain, &py_pub))
		return NULL;

	g_pub = (void**)PyCapsule_GetPointer(py_pub, "void");
	g_plain = (void**)PyCapsule_GetPointer(py_plain, "void");

	///* malloc mng */
	mpz_manager* h_mng = (mpz_manager*)malloc(sizeof(mpz_manager));

	/* init int */
	gpu_decode(g_pub, g_plain, h_mng);

	/* load c_pub from dev */
	eggroll_public_key* c_pub = (eggroll_public_key*)malloc(sizeof(eggroll_public_key));
	eggroll_public_init(c_pub);
	load_devPub(c_pub, g_pub);

	/* test */
	if (1)
	{
		double val = 0.f;
		for (i = 0; i < h_mng->col * h_mng->row; i++)
		{
			val = decode_float64(c_pub, h_mng->field[i], h_mng->exp[i]);
			printf("[gpu decode] val : %lf\n", val);
		}
	}

	return PyMpzField_FromMpzField(&h_mng, 0);
}

/* multi process */
static PyObject* pyg_paillier_dump(PyObject* self, PyObject* args) {

	int i, j;
	int row, col;
	PyObject* py_mng;

	void** g_mng;

	/* python get size */
	if (!PyArg_ParseTuple(args, "O", &py_mng))
		return NULL;

	g_mng = (void**)PyCapsule_GetPointer(py_mng, "void");

	///* malloc */
	void** g_dump= (void**)malloc(sizeof(void*));

	/* init int */
	long int dumplen = gpu_dump(g_mng, g_dump);

	return PyBytes_FromStringAndSize((char*)(*g_dump), sizeof(uint64_t) * dumplen);
}

static PyObject* pyg_paillier_load(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col, bit;
	int ret;
	short headerSize = 4;

	uint64_t* l_data;
	Py_ssize_t size;

	mpz_manager* c_mng = (mpz_manager*)malloc(sizeof(mpz_manager));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "s#", &l_data, &size))
		return NULL;

	///* malloc gpu_mng */
	void** g_res = (void**)malloc(sizeof(void*));

	ret = gpu_load(g_res, l_data);

	return PyCapsule_New(g_res, "void", NULL);
}

static PyObject* pyg_paillier_key2dev(PyObject* self, PyObject* args)
{
	int i, j;
	int row, col;
	PyObject* py_pub;
	PyObject* py_priv;

	eggroll_public_key* c_pub = malloc(sizeof(eggroll_public_key));
	eggroll_private_key* c_priv = malloc(sizeof(eggroll_private_key));

	/* Get the passed Python object */
	if (!PyArg_ParseTuple(args, "OO", &py_pub, &py_priv))
		return NULL;
	if (!(c_pub = PyPublicKey_AsPublicKey(py_pub)))
		return NULL;
	if (!(c_priv = PyPrivateKey_AsPrivateKey(py_priv)))
		return NULL;

	void** d_pub = (void**)malloc(sizeof(void*));
	void** d_priv = (void**)malloc(sizeof(void*));

	gpu_init_pub(c_pub, d_pub);
	gpu_init_priv(c_priv, d_priv);

	PyObject* pArgs = PyTuple_New(2);
	PyTuple_SetItem(pArgs, 0, PyCapsule_New(d_pub, "void", NULL));
	PyTuple_SetItem(pArgs, 1, PyCapsule_New(d_priv, "void", NULL));
}

