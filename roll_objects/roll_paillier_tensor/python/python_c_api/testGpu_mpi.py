#pragma once
#from mpi4py import MPI
import socket
import roll_paillier_tensor as eggroll
import numpy as np
import pandas as pd
import datetime

r = 2
myname = socket.getfqdn(socket.gethostname())
myaddr = socket.gethostbyname(myname)
node1 = 'gpua-node1'
node4 = 'node4'
if myname.strip()==node1.strip():
    if r == 100:
        mat = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/bigMat1.csv").values
        vec = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/bigVec1.csv").values
    elif r == 2:
        mat = pd.read_csv("/data/czn/data/testMat1.csv").values
        vec = pd.read_csv("/data/czn/data/testVec1.csv").values
        mat1 = pd.read_csv("/data/czn/data/testMat11.csv").values
        mat2 = pd.read_csv("/data/czn/data/testMat11.csv").values


if  myname.strip()==node4.strip():
    if r == 100:
        mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/bigMat1.csv").values
        vec = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/bigVec1.csv").values
    elif r == 2:
        mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat2.csv").values
        vec = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testVec2.csv").values
 
##clock
str = datetime.datetime.now()
pub, priv = eggroll.gpu_genkey()
end = datetime.datetime.now()
delta = end - str
print("cipher.key gen cost time",int(delta.total_seconds() * 1000), "ms")


str = datetime.datetime.now()
gpu_mat1 = eggroll.gpu_init(mat1, pub)
gpu_mat2 = eggroll.gpu_init(mat2, pub)
end = datetime.datetime.now()
delta = end - str
print("cipher.decrypt cost time",int(delta.total_seconds() * 1000), "ms")



str = datetime.datetime.now()
dump_mat1 = eggroll.gpu_dump(gpu_mat1)
dump_mat2 = eggroll.gpu_dump(gpu_mat2)
end = datetime.datetime.now()
delta = end - str
print("cipher.decrypt cost time",int(delta.total_seconds() * 1000), "ms")




str = datetime.datetime.now()
load_mat1 = eggroll.gpu_load(dump_mat1)
load_mat2 = eggroll.gpu_load(dump_mat2)
end = datetime.datetime.now()
delta = end - str
print("cipher.decrypt cost time",int(delta.total_seconds() * 1000), "ms")


str = datetime.datetime.now()
enc_mat1 = eggroll.gpu_encrypt(load_mat1, pub)
enc_mat2 = eggroll.gpu_encrypt(load_mat2, pub)
end = datetime.datetime.now()
delta = end - str
print("cipher.decrypt cost time",int(delta.total_seconds() * 1000), "ms")



a = eggroll.gpu_dotadd(enc_mat1, enc_mat2, pub)
##slice csv ---> [head: byte], 
#b_mat1, tag1, row1, col1 = eggroll.slice_csv(mat1, blockDimx, blockDimy)
#b_mat2, tag2, row2, col2 = eggroll.slice_csv(mat2, blockDimx, blockDimy)

#sub_head1 = eggroll.make_header(blockDimx, blockDimy, 2048, tag1)
#sub_head2 = eggroll.make_header(blockDimx, blockDimy, 2048, tag2)

#sendMat1 = []
#sendMat2 = []

#subsize = 2 * 2 * 8;
#hsize = 4 * 8

#for i in range(4):
#    sendMat1.append(sub_head1 + b_mat1[(hsize + i * subsize) : ((i + 1) * subsize + hsize)])
#    sendMat2.append(sub_head2 + b_mat2[(hsize + i * subsize) : ((i + 1) * subsize + hsize)])


