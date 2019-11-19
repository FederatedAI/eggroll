#pragma once
import socket
import roll_paillier_tensor as fa
import pandas as pd
import time 
import datetime

a = fa.point(3, 4)
#print(a)
fa.pointPrint(a)

b = fa.mpz(300)
fa.gmpPrint(b)

r = 2

f = fa.manager(2,3)
f.col

myname = socket.getfqdn(socket.gethostname())
myaddr = socket.gethostbyname(myname)
node1 = 'gpua-node1'
node4 = 'node4'
if myname.strip()==node1.strip():
    if r == 100:
        mat = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/bigMat1.csv").values
        vec = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/bigVec1.csv").values
    elif r == 2:
        mat2X2 = pd.read_csv("/data/czn/data/testMat2X2_int.csv").values
        #mat2X2 = pd.read_csv("/data/czn/data/testMat2X2_int.csv").values
        mat2X3 = pd.read_csv("/data/czn/data/testMat2X3_float.csv").values
        mat3X1 = pd.read_csv("/data/czn/data/testMat3X1_float.csv").values


if  myname.strip()==node4.strip():
    if r == 100:
        mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/bigMat1.csv").values
        vec = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/bigVec1.csv").values
    elif r == 2:
        mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat2.csv").values
        vec = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testVec2.csv").values
  

####cpu
#c_pub, c_priv = fa.keygen()
##print(c_pub)
#c_Mat = fa.init(mat2X3, c_pub)
#c_Vec = fa.init(mat3X1, c_pub)

#e_Mat = fa.encrypt_and_obfuscate(c_Mat, c_pub)
##e_Vec = fa.encrypt_and_obfuscate(c_Vec, c_pub)

#r_Mat = fa.matmul_c_eql(e_Mat, c_Vec, c_pub, c_priv)

#res = fa.decrypt(r_Mat, c_pub, c_priv)
#val = fa.decode(res, c_pub, c_priv)


##clock
str = datetime.datetime.now()
pub, priv = fa.gpu_genkey()
end = datetime.datetime.now()
delta = end - str
print("cipher.key gen cost time",int(delta.total_seconds() * 1000), "ms")

##kk = fa.gpu_show(pub, 0)

str = datetime.datetime.now()
gpu_mat1 = fa.gpu_init(mat2X3, pub)
gpu_mat2 = fa.gpu_init(mat3X1, pub)
end = datetime.datetime.now()
delta = end - str
print("cipher.decrypt cost time",int(delta.total_seconds() * 1000), "ms")

###kk = fa.gpu_show(gpu_mat, 2)

str = datetime.datetime.now()
enc_gpu_mat1 = fa.gpu_encrypt(gpu_mat1, pub)
#enc_gpu_vec = fa.gpu_encrypt(gpu_vec, pub)
end = datetime.datetime.now()
delta = end - str
print("gpu_encrypt cost time",int(delta.total_seconds() * 1000), "ms")


####################################################
##str = datetime.datetime.now()
##dumpMap = fa.gpu_dump(enc_gpu_mat)
##end = datetime.datetime.now()
##delta = end - str
##print("gpu_dotadd cost time",int(delta.total_seconds() * 1000), "ms")


##str = datetime.datetime.now()
##aa = fa.gpu_load(dumpMap)
##end = datetime.datetime.now()
##delta = end - str
##print("gpu_dotadd cost time",int(delta.total_seconds() * 1000), "ms")

####################################################


str = datetime.datetime.now()
#enc_add_res = fa.gpu_dotadd(enc_gpu_mat, enc_gpu_vec, pub)
#enc_add_res = fa.gpu_dotmul(enc_gpu_mat1, gpu_mat2, pub)

enc_add_res = fa.gpu_matmul(enc_gpu_mat1, gpu_mat2, pub)

end = datetime.datetime.now()
delta = end - str
print("gpu_dotadd cost time",int(delta.total_seconds() * 1000), "ms")


str = datetime.datetime.now()
res = fa.gpu_decrypt(enc_add_res, pub, priv)
end = datetime.datetime.now()
delta = end - str
print("gpu_dotadd cost time",int(delta.total_seconds() * 1000), "ms")

fa.gpu_decode(res, pub)