#ifndef GRAPH_INTERFACE_COMMONS_H
#define GRAPH_INTERFACE_COMMONS_H

#define TORCH_OPTION(type, is_grad) \
  (torch::TensorOptions().dtype(type).requires_grad(is_grad))

#define TORCH_OPTION_FLOAT \
  TORCH_OPTION(torch::kFloat, false)

#define TORCH_OPTION_FLOAT_GRAD \
  TORCH_OPTION(torch::kFloat, true)

#define TORCH_OPTION_DOUBLE \
  TORCH_OPTION(torch::kDouble, false)

#define TORCH_OPTION_DOUBLE_GRAD \
  TORCH_OPTION(torch::kDouble, true)
#endif