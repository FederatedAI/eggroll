#pragma once
/* pysample.h */
#include "Python.h"
#include "paillier.h"
#include "tools.h"

#include "gmp.h"
#include "structmember.h"


#ifdef __cplusplus
extern "C" {
#endif

	
	/* Public API Table */
	/* ��������Ҫ�Ĳ����Ǻ���ָ��� _PointAPIMethods.
	   �����ڵ���ģ��ʱ����ʼ����Ȼ����ģ��ʱ�����ҵ��� */

	typedef struct {
		/* test */
		Point* (*aspoint)(PyObject*);
		PyObject* (*frompoint)(Point*, int);

		mpz_t* (*asmpz)(PyObject*);
		PyObject* (*frommpz)(mpz_t*, int);

		__mpz_struct* (*asmpzptr)(PyObject*);
		PyObject* (*frommpzptr)(__mpz_struct*, int);

		/* key */
		eggroll_public_key* (*aspublic)(PyObject*);
		PyObject* (*frompublic)(eggroll_public_key*, int);

		eggroll_private_key* (*asprivate)(PyObject*);
		PyObject* (*fromprivate)(eggroll_private_key*, int);

		mpz_manager* (*asmzpfield)(PyObject*);
		PyObject* (*frommpzfield)(mpz_manager, int);

	} _PaillierAPIMethods;


#ifndef PYSAMPLE_MODULE  //<---pysample.c:4
	/* Method table in external module */
	static _PointAPIMethods* _eggroll_api = 0;

	/* Import the API table from sample, import_sample() ������ָ���ҵ��벢��ʼ�����ָ�� */
	static int import_sample(void) {  //<---ptexample.c:46
		// ���ṩ������������sample._eggroll_api������һ�����ҵ����Ҷ�����ȡ��ָ������
		_eggroll_api = (_PointAPIMethods*)PyCapsule_Import("sample._eggroll_api", 0);  //<---pysample.c:250
		return (_eggroll_api != NULL) ? 1 : 0;
	}

	/* Macros to implement the programming interface */
#define PyPoint_AsPoint(obj) (_eggroll_api->aspoint)(obj)
#define PyPoint_FromPoint(obj) (_eggroll_api->frompoint)(obj)
#endif

#ifdef __cplusplus
}
#endif
