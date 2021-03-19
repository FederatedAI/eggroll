#include <torch/torch.h>
#include <torch/script.h>
#include <iostream>
#include <memory.h>
#include <eggroll/roll_frame/pytorch/commons.h>
#include <eggroll/roll_frame/pytorch/roll_frame_torch.h>

torch::jit::script::Module module;

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
/*
 * Class:     com_webank_eggroll_rollframe_pytorch_Torch
 * Method:    getTorchScript
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_webank_eggroll_rollframe_pytorch_Torch_getTorchScript
(JNIEnv* env, jclass jcls, jstring jpath) {
	const char* path = env->GetStringUTFChars(jpath, NULL);
	//static torch::jit::script::Module module;
	module = torch::jit::load(path);
	auto jptr = reinterpret_cast<int64_t>(&module);
	env->ReleaseStringUTFChars(jpath, path);
	return jptr;
}

/*
 * Class:     com_webank_eggroll_rollframe_pytorch_Torch
 * Method:    run
 * Signature: (J[Lcom/webank/eggroll/rollframe/pytorch/TorchTensor;[D)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_com_webank_eggroll_rollframe_pytorch_Torch_run
 (JNIEnv* env, jclass jcls, jlong jptr, jobjectArray jarray, jdoubleArray jparameters) {
	//torch::set_num_threads(7);
	std::cout << at::get_num_threads() << std::endl;
	torch::jit::script::Module* ptr = reinterpret_cast<torch::jit::script::Module*>(jptr);
	assert(ptr != nullptr);
	std::vector<torch::jit::IValue> inputs;
	c10::List<at::Tensor> inputs_element;
	jsize tensor_count = env->GetArrayLength(jarray);
	//printf("tensor_count = %d", tensor_count);
	for (int i = 0; i < tensor_count; i++) {
		// get each FrameBatch members
		jobject tensor_obj = env->GetObjectArrayElement(jarray, i);
		if (NULL == tensor_obj) return NULL;
		jclass tensor_clz = env->GetObjectClass(tensor_obj);
		//std::cout<<"get tensor_clz"<<std::endl;
		jmethodID m_address_id = env->GetMethodID(tensor_clz, "getAddress", "()J");
		//std::cout << "get getAddress methodID" << std::endl;
		jlong address = env->CallLongMethod(tensor_obj, m_address_id);
		//std::cout << "get address..." << std::endl;
		jmethodID m_size_id = env->GetMethodID(tensor_clz, "getSize", "()J");
		jlong size = env->CallLongMethod(tensor_obj, m_size_id);
		auto* tensor_ptr = reinterpret_cast<double*>(address);
		torch::Tensor tensor = torch::from_blob(tensor_ptr, { size }, TORCH_OPTION_DOUBLE);
		inputs_element.emplace_back(tensor);
	}
	std::cout << "finish tensor input" << std::endl;
	jsize parameters_size = env->GetArrayLength(jparameters);
	auto* parameters = env->GetPrimitiveArrayCritical(jparameters, NULL);
	torch::Tensor parameters_tensor = torch::from_blob(parameters, { parameters_size }, TORCH_OPTION_DOUBLE);
	inputs_element.emplace_back(parameters_tensor);
	inputs.emplace_back(inputs_element);
	// std::cout << "finish parameters input" << std::endl;
	// std::cout << parameters_tensor << std::endl;
	torch::Tensor res = ptr->forward(inputs).toTensor();				// run model
	std::cout << "finish run model..." << std::endl;
	env->ReleasePrimitiveArrayCritical(jparameters, parameters, 0);		// release
	jlong length = res.numel();
	double* res_ptr = res.data_ptr<double>();
	jdoubleArray out = env->NewDoubleArray(length);
	env->SetDoubleArrayRegion(out, 0, length, res_ptr);
	std::cout << "run pytorch done" << std::endl;
	return out;
}