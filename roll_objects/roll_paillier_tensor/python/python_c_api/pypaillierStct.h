#pragma once
#include "Python.h"
#include "structmember.h"
#include "gmp.h"
#include "toPyInterface.h"

typedef struct {
	PyObject_HEAD
	int ifmal;
	int ifenc;
	int row;
	int col;
	int64_t* exp;
	PyObject* field;
} ManagerObject;

static PyTypeObject ManagerType;

static void
Manager_dealloc(ManagerObject* self)
{
	//printf("[manager del here] 1\n");
	//puts("deallocating weird pointer");
	Py_XDECREF(self->field);
	Py_TYPE(self)->tp_free((PyObject*)self);

}

static PyObject*
Manager_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
	/* must malloc first ! */
	ManagerObject* self;
	self = PyObject_New(ManagerObject, &ManagerType);
	
	mpz_t* tmp = (mpz_t*)malloc(sizeof(mpz_t));
	if (self != NULL) {
		self->row = 1;
		self->col = 1;
		self->ifenc = 0;
		self->ifmal = 1;
		self->field = PyMpz_FromMpz(tmp, 0);
		if (self->field == NULL) {
			Py_DECREF(self);
			return NULL;
		}
	}

	return (PyObject*)self;
}

static int
Manager_init(ManagerObject* self, PyObject* args, PyObject* kwds)
{
	//printf("[manager init here]\n");

	static char* kwlist[] = { "col", "row", NULL };
	PyObject* field = NULL, * tmp;

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|ii", kwlist,
		&self->col, &self->row))
		return -1;

	/* free inited memory before */
	mpz_t* c_field = PyMpz_AsMpz(self->field);
	free(c_field);

	/* realloc field refer to col row*/
	c_field = (mpz_t*)malloc(sizeof(mpz_t) * self->col * self->row);
	
	self->field = PyMpz_FromMpz(c_field, 0);
	self->exp = (int64_t*)malloc(sizeof(int64_t) * self->col * self->row);
	self->ifenc = 0;
	self->ifmal = 1;

	//printf("%d %d\n", self->col, self->row);

	if (field) {
		tmp = self->field;
		Py_INCREF(field);
		self->field = field;
		Py_XDECREF(tmp);
	}

	return 0;
}

static PyMemberDef Manager_members[] = {
	{"col", T_INT, offsetof(ManagerObject, col), 1, 
	"col size"},
	{"row", T_INT, offsetof(ManagerObject, row), 1,
	 "row size"},
	{"ifmal", T_INT, offsetof(ManagerObject, ifmal), 1,
	 "if malloced"},
	{"ifenc", T_INT, offsetof(ManagerObject, ifenc), 1,
	 "if encrypt"},
	{"field", T_OBJECT_EX, offsetof(ManagerObject, field), 1,
	 "field addr"},
	{NULL}  /* Sentinel */
};

static PyTypeObject ManagerType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "roll_paillier_tensor.Manager",
	.tp_doc = "manager objects",
	.tp_basicsize = sizeof(ManagerObject),
	.tp_itemsize = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_new = Manager_new,
	.tp_init = (initproc)Manager_init,
	.tp_dealloc = (destructor)Manager_dealloc,
	.tp_members = Manager_members,
	//.tp_methods = Manager_methods,
};

