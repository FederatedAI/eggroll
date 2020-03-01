#include <torch/torch.h>
#include <torch/script.h>
#include <iostream>
#include <memory.h>
#include <eggroll/roll_frame/pytorch/commons.h>
#include <eggroll/roll_frame/pytorch/roll_frame_torch.h>


JNIEXPORT jfloat JNICALL Java_com_webank_eggroll_rollframe_pytorch_Torch_primaryDot
(JNIEnv* env, jclass jcls, jfloatArray jvals_left, jfloatArray jvals_right) {
	jsize input_size = env->GetArrayLength(jvals_left);
	// convert to native type
	auto* vals_left = env->GetPrimitiveArrayCritical(jvals_left, NULL);
	auto* vals_right = env->GetPrimitiveArrayCritical(jvals_right, NULL);
	// convert to Tensor

	torch::Tensor input_left = torch::from_blob(vals_left, { input_size }, TORCH_OPTION_FLOAT);
	torch::Tensor input_right = torch::from_blob(vals_right, { input_size }, TORCH_OPTION_FLOAT);
	double res = input_left.dot(input_right).item<double>();
	env->ReleasePrimitiveArrayCritical(jvals_left, vals_left, 0);		// release
	env->ReleasePrimitiveArrayCritical(jvals_right, vals_right, 0);		// release
	return res;
}

JNIEXPORT jdoubleArray JNICALL Java_com_webank_eggroll_rollframe_pytorch_Torch_mm__JJ_3D
(JNIEnv* env, jclass jcls, jlong address, jlong size, jdoubleArray jvals) {
	jsize dim = env->GetArrayLength(jvals);
	auto* matrix_ptr = reinterpret_cast<double*>(address);
	// convert to Tensor
	torch::Tensor matrix = torch::from_blob(matrix_ptr, { size }, TORCH_OPTION_DOUBLE);
	matrix = matrix.view({ -1, dim });
	auto* vector_ptr = env->GetPrimitiveArrayCritical(jvals, NULL);
	torch::Tensor vector = torch::from_blob(vector_ptr, { dim }, TORCH_OPTION_DOUBLE);
	vector = vector.view({ dim,1 });
	torch::Tensor res = torch::mm(matrix, vector);
	jlong length = res.numel();
	double* res_ptr = res.data_ptr<double>();
	jdoubleArray out = env->NewDoubleArray(length);
	env->SetDoubleArrayRegion(out, 0, length, res_ptr);
	env->ReleasePrimitiveArrayCritical(jvals, vector_ptr, 0);		// release
	return out;
}

JNIEXPORT jdoubleArray JNICALL Java_com_webank_eggroll_rollframe_pytorch_Torch_mm__JJ_3DJJ
(JNIEnv* env, jclass jcls, jlong address, jlong size, jdoubleArray jvals, jlong rows, jlong cols) {
	auto* matrix_ptr = reinterpret_cast<double*>(address);
	torch::Tensor matrix = torch::from_blob(matrix_ptr, { size }, TORCH_OPTION_DOUBLE);
	matrix = matrix.view({ -1, rows });
	auto* vector_ptr = env->GetPrimitiveArrayCritical(jvals, NULL);
	torch::Tensor vector = torch::from_blob(vector_ptr, { rows,cols }, TORCH_OPTION_DOUBLE);
	torch::Tensor res = torch::mm(matrix, vector);
	jlong length = res.numel();
	double* res_ptr = res.data_ptr<double>();
	jdoubleArray out = env->NewDoubleArray(length);
	env->SetDoubleArrayRegion(out, 0, length, res_ptr);
	env->ReleasePrimitiveArrayCritical(jvals, vector_ptr, 0);		// release
	return out;
}