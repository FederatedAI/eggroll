from mpi4py import MPI
import numpy as np
import eggroll as fa
import pandas as pd

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

##print("size = ", size)
#if rank == 0:
#    data = range(10)
#    pub, priv = fa.keygen()
#    d_pub = fa.dumppub(pub)
#    d_priv = fa.dumppriv(priv)
    
#    mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat_mpi.csv").values
#    vec = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testVec_mpi.csv").values
#    zmat = fa.mpzinit(mat)
#    zvec = fa.mpzinit(vec)
#    mpz_mat = fa.encrypt(zmat, pub)

#    #print(mpz_mat)
#    sliceMat = fa.slice(mpz_mat, 2, 2, 4)
#    sliceVec = fa.slice(zvec, 2, 1, 4)

#    sendMat = []
#    sendVec = []
#    for i in range(4):
#        sendMat.append(sliceMat[(i * 256 * 4) : ((i + 1) * 256 * 4)])
#    for i in range(4):
#        sendVec.append(sliceVec[((int(i % 2)) * 256 * 2) : (((int(i % 2)) + 1) * 256 * 2)])
#   #print(sendVec[0])
#else:
#    d_pub = None
#    d_priv = None
#    sendMat = None
#    sendVec = None

#d_pub = comm.bcast(d_pub, root=0)
#d_priv = comm.bcast(d_priv, root=0)

#pub = fa.loadpub(d_pub)
#priv = fa.loadpriv(d_priv)

#recvMat = comm.scatter(sendMat, root=0)
#recvVec = comm.scatter(sendVec, root=0)

#subMat = fa.load(recvMat, 2, 2, rank)
#subVec = fa.load(recvVec, 2, 1, rank)

#subMul = fa.mul(subMat, subVec, pub, priv)
#subRes = fa.decrypt(subMul, pub, priv)

#senRes = fa.slice(subRes, 2, 1, 1)
#recRes = comm.gather(senRes, root=0)

#if rank == 0:
#    Res = bytes()
#    for i in range(4):
#        Res += recRes[i]

#    a = fa.melt(Res, 2, 4, size)
   

#print(recvbuf)
#print("rank ",rank , "recebuf", recvbuf)
