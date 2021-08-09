# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 14:18:34 2020

@author: M172468
"""
import happybase
#from elasticsearch import Elasticsearch
#import numpy as np
import pandas as pd

def MG08_getDataFromBigtablemain(connection,wafer):
    keylist=[]
    valuelist=[]
    table = connection.table('mes_bigtable')
    data11 = table.scan(row_prefix=wafer,columns=[
        "cf:HEAD_SN","cf:[DP].SSEB_EWI(nm)-R2",
        "cf:[LAPPING.SLIDER].DELTA_HR_WIR_LOCK_VOL",
        "cf:[LAPPING.SLIDER].FINAL_DIRECT_APPEND_DH38_WIR_HW_MES"])
    for key,value in data11:
        keylist.append(key)
        valuelist.append(value)
    result = pd.DataFrame()
    if(len(valuelist)>0):
        df_dp1=pd.DataFrame(valuelist)
        df_dp1.columns = [name1.replace("cf:", "") for name1 in df_dp1.columns]
        result = df_dp1[["HEAD_SN",
                        "[DP].SSEB_EWI(nm)-R2",
                        "[LAPPING.SLIDER].DELTA_HR_WIR_LOCK_VOL",
                        "[LAPPING.SLIDER].FINAL_DIRECT_APPEND_DH38_WIR_HW_MES"]]
    else:
        result=pd.DataFrame()
    return(result)

def main(input):
    wafer=input['wafer_no'][0]
    try:
        connection = happybase.Connection(host='DN2MESHD04.sae.com.hk', port=9090)
        print(connection.tables())
        connection.open()
        #----------------------external_wafer_data_mp----------------------
        output = MG08_getDataFromBigtablemain(connection,wafer)
    finally:
        if connection:
            connection.close()
    return(output)

    
import os
os.chdir("D:/data_Engineer/walter/MG08_ML_AI/New_AI/delta_PCM_2/dp")
input=pd.DataFrame([{'wafer_no':'0A70D'}])
wafer_no=list(input['wafer_no'])[0]
output = main(input)
output.to_csv('MG08_ML_AI_model2_DP_'+wafer_no+'_20210525.csv',index = False)
