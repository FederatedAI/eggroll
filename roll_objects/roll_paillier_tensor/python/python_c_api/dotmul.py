#pragma once
import socket
import eggroll as fa
import pandas as pd
import time 
import datetime
#a = pa.point(3, 4)
#pa.pointPrint(a)

b = fa.mpz(300)
fa.gmpPrint(b)

myname = socket.getfqdn(socket.gethostname(  ))
myaddr = socket.gethostbyname(myname)
node1 = 'gpua-node1'
node4 = 'node4'

R = 2

if myname.strip()==node1.strip():
    if R==2:
        mat1 = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/testMat1.csv").values
        mat11 = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/testMat11.csv").values
    elif R==100:
        mat1 = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/bigMat1.csv").values
        mat11 = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/bigMat11.csv").values


if  myname.strip()==node4.strip():
    if R==2:
        mat1 = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat1_float.csv").values
        mat11 = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat11.csv").values
    elif R==100:
        mat1 = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/bigMat1.csv").values
        mat11 = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/bigMat11.csv").values
        
##clock
str = datetime.datetime.now()
if R==2:
    pub, priv = fa.useFixKey()
else:
    pub, priv = fa.keygen(1024)

end = datetime.datetime.now()
delta = end - str
print("cipher.key gen cost time",int(delta.total_seconds() * 1000), "ms")

print("mat : row = {} col = {} ".format(mat1.shape[0] , mat1.shape[1]))
print("mat : row = {} col = {} ".format(mat11.shape[0] , mat11.shape[1]))


##clock
str = datetime.datetime.now()
c_mat1 = fa.mpzinit(mat1, pub)
c_mat11 = fa.mpzinit(mat11, pub)
end = datetime.datetime.now()
delta = end - str
print("cipher.init cost time",int(delta.total_seconds() * 1000), "ms")

str = datetime.datetime.now()
enc_mat1 = fa.encrypt(c_mat1, pub)
end = datetime.datetime.now()
delta = end - str
print("cipher.encrypt cost time",int(delta.total_seconds() * 1000), "ms")

str = datetime.datetime.now()
res = fa.dotmul(enc_mat1, c_mat11, pub, priv)
end = datetime.datetime.now()
delta = end - str
print("cipher.mul cost time",int(delta.total_seconds() * 1000), "ms")

str = datetime.datetime.now()
kk = fa.decrypt(res,pub, priv)
end = datetime.datetime.now()
delta = end - str
print("cipher.decrypt cost time",int(delta.total_seconds() * 1000), "ms")
#a , b  = pa.keygen(1024)
#a