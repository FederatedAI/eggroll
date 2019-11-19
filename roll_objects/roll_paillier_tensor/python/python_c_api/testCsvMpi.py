from mpi4py import MPI
import numpy as np
import roll_paillier_tensor as eggroll
import pandas as pd

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

#print("size = ", size)
if rank == 0:
    data = range(10)
    pub, priv = eggroll.keygen()
    d_pub = eggroll.dump_pub_key(pub)
    d_priv = eggroll.dump_prv_key(priv)
    
    mat1 = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat_mpi.csv").values
    mat2 = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat_mpi.csv").values
    
    blockDimx = 2
    blockDimy = 2
    
    #slice csv ---> [head: byte], 
    b_mat1, tag1, row1, col1 = eggroll.slice_csv(mat1, blockDimx, blockDimy)
    b_mat2, tag2, row2, col2 = eggroll.slice_csv(mat2, blockDimx, blockDimy)

    sub_head1 = eggroll.make_header(blockDimx, blockDimy, 2048, tag1)
    sub_head2 = eggroll.make_header(blockDimx, blockDimy, 2048, tag2)

    sendMat1 = []
    sendMat2 = []

    subsize = 2 * 2 * 8;
    hsize = 4 * 8

    for i in range(4):
        sendMat1.append(sub_head1 + b_mat1[(hsize + i * subsize) : ((i + 1) * subsize + hsize)])
        sendMat2.append(sub_head2 + b_mat2[(hsize + i * subsize) : ((i + 1) * subsize + hsize)])

else:
    d_pub = None
    d_priv = None
    sendMat1 = None
    sendMat2 = None

d_pub = comm.bcast(d_pub, root=0)
d_priv = comm.bcast(d_priv, root=0)

pub = eggroll.load_pub_key(d_pub)
priv = eggroll.load_prv_key(d_priv)

recvMat1 = comm.scatter(sendMat1, root=0)
recvMat2 = comm.scatter(sendMat2, root=0)

subMat1 = eggroll.init_byte_csv(recvMat1, pub)
subMat2 = eggroll.init_byte_csv(recvMat2, pub)

encMa1 = eggroll.encrypt_and_obfuscate(subMat1, pub)
encMa2 = eggroll.encrypt_and_obfuscate(subMat2, pub)

dumpMa1 = eggroll.dump(subMat1)
dumpMa2 = eggroll.dump(subMat2)

loadMa1 = eggroll.load(dumpMa1)
loadMa2 = eggroll.load(dumpMa2)

subAdd= eggroll.add(loadMa1, loadMa2, pub, priv)

subcode = eggroll.decrypt(subAdd, pub, priv)
subdata = eggroll.decode(subcode, pub, priv)

#dumpRes = eggroll.dump(subdata)

#recRes = comm.gather(dumpRes, root=0)

if rank == 0:
    Res = bytes()
    for i in range(4):
        Res += recRes[i]

    a = fa.melt(Res, 2, 4, size)
   

#print(recvbuf)
#print("rank ",rank , "recebuf", recvbuf)
