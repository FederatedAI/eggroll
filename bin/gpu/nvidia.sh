result=$(nvidia-smi --query-gpu=name --format=csv, noheader|grep 'NVIDIA'|wc -l)
echo  $result

