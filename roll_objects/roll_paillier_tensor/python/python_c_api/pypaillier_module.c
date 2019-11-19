
#include "Python.h"
#include "toPyInterface.h"
#include "pypaillier.h"
#include "pypaillierStct.h"
#include "cuPaillier/cupypaillier.h"

/* Module method table */
static PyMethodDef SampleMethods[] = {

	/* mpz function*/
	{"mpz", py_mpz, METH_VARARGS},
	{"gmpPrint", py_mpzPrint, METH_VARARGS, "gmp printf"},
  
	/* test point function*/
	{"point", py_Point, METH_VARARGS},
	{"pointPrint", py_pointPrint, METH_VARARGS, "gmp printf"},
  
	/* paillier function*/
	{"keygen", py_paillier_keygen, METH_NOARGS, "generates default public and private keys"},
	{"use_fixed_key", py_paillier_use_fixkey, METH_NOARGS, "use predefined public and private key, test only"},
	{"init", py_paillier_init, METH_VARARGS, "module init"},
	{"encrypt_and_obfuscate", (PyCFunction)py_paillier_encrpyt, METH_VARARGS, "encrypt a number and obsfucate"},
	{"encrypt", (PyCFunction)py_paillier_raw_encrpyt, METH_VARARGS, "data encryption"},
	{"matmul_c_eql", (PyCFunction)py_paillier_mul_c_eql, METH_VARARGS, "matrix multiply"},
	{"matmul_r_eql", (PyCFunction)py_paillier_mul_r_eql, METH_VARARGS, "matrix multiply"},
	{"obfuscate", (PyCFunction)py_paillier_obf, METH_VARARGS, "obfuscate a number"},
	{"vdot", (PyCFunction)py_paillier_dotmul, METH_VARARGS, "vdot multiply"},
	{"add", (PyCFunction)py_paillier_dotadd, METH_VARARGS, "matrix add"},
	{"scalar_mul", (PyCFunction)py_paillier_slcmul, METH_VARARGS, "scalar multiplies a matrix"},
	{"decrypt", (PyCFunction)py_paillier_decrpyt, METH_VARARGS, "data decryption"},
	{"decode", (PyCFunction)py_paillier_decode, METH_VARARGS, "data decryption"},
	{"del", (PyCFunction)py_paillier_free, METH_VARARGS, "free mpz memory"},

	/* about tool*/
	{"print", (PyCFunction)py_paillier_print, METH_VARARGS, "print a cipher"},
	
	/* about multi process*/
	{"dump_pub_key", (PyCFunction)py_paillier_dump_pub, METH_VARARGS, "serialize public key"},
	{"dump_prv_key", (PyCFunction)py_paillier_dump_priv, METH_VARARGS, "serialize private key"},

	{"load_pub_key", (PyCFunction)py_paillier_load_pub, METH_VARARGS, "derialize public key"},
	{"load_prv_key", (PyCFunction)py_paillier_load_priv, METH_VARARGS, "derialize private key"},
	
	{"slice_n_dump", (PyCFunction)py_paillier_slice_n_dump, METH_VARARGS, "gmp -> slice(byte)"},
	{"slice", (PyCFunction)py_paillier_slice, METH_VARARGS, "gmp -> slice(byte)"},
	{"dump", (PyCFunction)py_paillier_dump, METH_VARARGS, "gmp -> slice(byte)"},
	{"melt", (PyCFunction)py_paillier_melt, METH_VARARGS, "melt(byte) -> gmp"},
	{"melt_one", (PyCFunction)py_paillier_melt_one, METH_VARARGS, "melt(byte) -> gmp"},
	{"load", (PyCFunction)py_paillier_load, METH_VARARGS, " byte -> gmp"},
	
	/* new multi processs function */
	{"make_header", (PyCFunction)py_paillier_make_header, METH_VARARGS, "make dumpline header"},
	{"slice_csv", (PyCFunction)py_paillier_slice_csv, METH_VARARGS, "slice csv"},
	{"init_byte_csv", (PyCFunction)py_paillier_init_byte_csv, METH_VARARGS, "csv(byte) -> gmp"},

	/* about gpu */
	{"gpu_genkey", (PyCFunction)pyg_paillier_keygen, METH_VARARGS, "gpu key gen"},
	{"gpu_init", (PyCFunction)pyg_paillier_init, METH_VARARGS, "gpu init"},
	{"gpu_init_byte", (PyCFunction)pyg_paillier_byte_init, METH_VARARGS, "byte -> cgdn(gpu)"},
	{"gpu_init_byte_csv", (PyCFunction)pyg_paillier_byte_init_csv, METH_VARARGS, "csv -> cdgn(gpu)"},
	{"gpu_encrypt", (PyCFunction)pyg_paillier_encrypt, METH_VARARGS, "gpu encrpyt"},
	{"gpu_dotadd", (PyCFunction)pyg_paillier_dotadd, METH_VARARGS, "gpu dot add"},
	{"gpu_dotmul", (PyCFunction)pyg_paillier_dotmul, METH_VARARGS, "gpu dot mul"},
	{"gpu_matmul", (PyCFunction)pyg_paillier_matmul_c_eql, METH_VARARGS, "gpu dot mul"},
	{"gpu_decrypt", (PyCFunction)pyg_paillier_decrypt, METH_VARARGS, "gpu decrypt"},
	{"gpu_decode", (PyCFunction)pyg_paillier_decode, METH_VARARGS, "gpu decrypt"},

	/* aboue gpu multi processing */
	{"gpu_dump", (PyCFunction)pyg_paillier_dump, METH_VARARGS, "devmem 2 host BYTE"},
	{"gpu_load", (PyCFunction)pyg_paillier_load, METH_VARARGS, "host BYTE 2 devmem"},

	{"gpu_key2dev", (PyCFunction)pyg_paillier_key2dev, METH_VARARGS, "key host2dev"},

	{"gpu_show", (PyCFunction)pyg_paillier_show, METH_VARARGS, "gpu show"},
	{ NULL, NULL, 0, NULL}
};

/* Module structure */
static struct PyModuleDef roll_paillier_tensormodule = {
  PyModuleDef_HEAD_INIT,
  "roll_paillier_tensor",           /* name of module */
  "Paillier-encrypt tensor implementation for Eggroll",  /* Doc string (may be NULL) */
  -1,                 /* Size of per-interpreter state or -1 */
  SampleMethods       /* Method table */
};

/* Module initialization function */
PyMODINIT_FUNC
PyInit_roll_paillier_tensor(void) {
	PyObject* m;
	PyObject* py_roll_paillier_tensor_api;

	m = PyModule_Create(&roll_paillier_tensormodule);
	if (m == NULL)
		return NULL;

	if (PyType_Ready(&ManagerType) < 0)
	{
		return NULL;
	}
	Py_INCREF(&ManagerType);
	PyModule_AddObject(m, "manager", (PyObject*)&ManagerType);

	/* Add the Point C API functions */
	py_roll_paillier_tensor_api = PyCapsule_New((void*)& _roll_paillier_tensor_api, "sample._roll_paillier_tensor_api", NULL);  //<---pysample.h:23
	if (py_roll_paillier_tensor_api) {
		PyModule_AddObject(m, "_roll_paillier_tensor_api", py_roll_paillier_tensor_api);
	}
	return m;
}
