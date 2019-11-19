#pragma once
#include "Python.h"
#include "paillier.h"
#define PYSAMPLE_MODULE  //<---pysample.h:16
#include "pypaillier_module.h"

#include "gmp.h"


/* test point */
static void del_Point(PyObject* obj) {
	//printf("Deleting point\n");
	free(PyCapsule_GetPointer(obj, "Point"));
}

static PyObject* PyPoint_FromPoint(Point* p, int must_free) {
	return PyCapsule_New(p, "Point", must_free ? del_Point : NULL);
}
static Point* PyPoint_AsPoint(PyObject* obj) {
	return (Point*)PyCapsule_GetPointer(obj, "Point");
}

/* mpz */
static void del_Mpz(PyObject* obj) {
	printf("Deleting mpz\n");
	free(PyCapsule_GetPointer(obj, "mpz_t"));
}

static PyObject* PyMpz_FromMpz(mpz_t* p, int must_free) {
	return PyCapsule_New(p, "mpz_t", must_free ? del_Mpz : NULL);
}
/* Utility functions */
static mpz_t* PyMpz_AsMpz(PyObject* obj) {
	return (mpz_t*)PyCapsule_GetPointer(obj, "mpz_t");
}


/* public key*/
static void del_public_key(PyObject* obj) {
	printf("Deleting eggroll_public_key\n");
	free(PyCapsule_GetPointer(obj, "eggroll_public_key"));
}
static PyObject* PyPublicKey_FromPublicKey(eggroll_public_key* p, int must_free) {
	return PyCapsule_New(p, "eggroll_public_key", must_free ? del_public_key : NULL);
}
static eggroll_public_key* PyPublicKey_AsPublicKey(PyObject* obj) {
	return (eggroll_public_key*)PyCapsule_GetPointer(obj, "eggroll_public_key");
}

/* private key */
static void del_private_key(PyObject* obj) {
	printf("Deleting paillier_private_key\n");
	free(PyCapsule_GetPointer(obj, "eggroll_private_key"));
}

static PyObject* PyPrivateKey_FromPrivateKey(eggroll_private_key* p, int must_free) {
	return PyCapsule_New(p, "eggroll_private_key", must_free ? del_private_key : NULL);
}
static eggroll_private_key* PyPrivateKey_AsPrivateKey(PyObject* obj) {
	return (eggroll_private_key*)PyCapsule_GetPointer(obj, "eggroll_private_key");
}


/* mpz_field key */
static void del_mpz_field(PyObject* obj) {
	printf("Deleting mpz_field\n");
	free(PyCapsule_GetPointer(obj, "mpz_manager"));
}

static PyObject* PyMpzField_FromMpzField(mpz_manager* p, int must_free) {
	return PyCapsule_New(p, "mpz_manager", must_free ? del_mpz_field : NULL);
}
static mpz_manager* PyMpzField_AsMpzField(PyObject* obj) {
	return (mpz_manager*)PyCapsule_GetPointer(obj, "mpz_manager");
}


/* api register */
static _PaillierAPIMethods _roll_paillier_tensor_api = {
  PyPoint_AsPoint,
  PyPoint_FromPoint,
  
  /* gmp */
  PyMpz_AsMpz,
  PyMpz_FromMpz,

  PyPublicKey_AsPublicKey,
  PyPublicKey_FromPublicKey,

  PyPrivateKey_AsPrivateKey,
  PyPrivateKey_FromPrivateKey,

  PyMpzField_AsMpzField,
  PyMpzField_FromMpzField
};
