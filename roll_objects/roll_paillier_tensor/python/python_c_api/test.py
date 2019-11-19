#pragma once
import socket
import roll_paillier_tensor as fa
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
        mat = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/testMat1.csv").values
        vec = pd.read_csv("/data/projects/eggroll_czn/Python_C_Paillier/pData/testVec1.csv").values


if  myname.strip()==node4.strip():
    if r == 100:
        mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/bigMat1.csv").values
        vec = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/bigVec1.csv").values
    elif r == 2:
        mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat1.csv").values
        vec = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testVec1.csv").values
   

##clock
str = datetime.datetime.now()
pub, priv = fa.keygen()
end = datetime.datetime.now()
delta = end - str
print("cipher.key gen cost time",int(delta.total_seconds() * 1000), "ms")

##clock
str = datetime.datetime.now()
zmat = fa.init(mat, pub)
zvec = fa.init(vec, pub)

end = datetime.datetime.now()
delta = end - str
print("cipher.init cost time",int(delta.total_seconds() * 1000), "ms")

str = datetime.datetime.now()
enc_mat = fa.encrypt_and_obfuscate(zmat, pub)
end = datetime.datetime.now()
delta = end - str
print("cipher.init cost time",int(delta.total_seconds() * 1000), "ms")

str = datetime.datetime.now()
ans = fa.matmul(enc_mat, zvec, pub, priv)
end = datetime.datetime.now()
delta = end - str
print("cipher.init cost time",int(delta.total_seconds() * 1000), "ms")


str = datetime.datetime.now()
code = fa.decrypt(ans, pub, priv)
end = datetime.datetime.now()
delta = end - str
print("cipher.init cost time",int(delta.total_seconds() * 1000), "ms")

by = fa.dump(code)
ba = fa.load(by)

fa.print(zvec, pub, priv)
fa.print(zmat, pub, priv)
fa.print(code, pub, priv)

#bigby = fa.slice_n_dump(enc_mat, 2, 2, 1)
#bigmng = fa.load(bigby)
#fa.print(bigmng, pub, priv)

#fa.print(ba, pub, priv)
#fa.print(code, pub, priv)
#fa.print(ans, pub, priv)
#fa.print(enc_mat, pub, priv)
