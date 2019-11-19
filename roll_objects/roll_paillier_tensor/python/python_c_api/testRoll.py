from Pailliercyper import *
from Grill import * 
import pandas as pd
import Fate as fa

def test_pengueinInRoll():
    cipher = PaillierEncrypt()
    cipher.generate_key()

    mat = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testMat1.csv").values
    vec = pd.read_csv("/data/home/qijunhuang/czn/code/Python_C_Paillier/pData/testVec1.csv").values

    pg_mat = penguinInRoll(cipher, mat)
    pg_vec = penguinInRoll(cipher, vec)


    enc_mat = pg_mat.encrypt()
    enc_res = enc_mat * pg_vec
    


if __name__ == '__main__':
    test_sugarInRoll()