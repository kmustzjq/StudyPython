# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 14:18:34 2020

@author: M172468
"""
import happybase
#from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd

def ffCSVData(connection,wafer):
    keylist=[]
    valuelist=[]
    rp=str(wafer)
    table = connection.table('wafer_incoming_ff')
    data11 = table.scan(row_prefix=rp,columns=[
        "cf:WAFER","cf:BLOCK","cf:BLK_SS1_Y",
        "cf:LET_CD","cf:LET_Y","cf:LS_DP","cf:PT2C_UZ55",
        "cf:R2_AVG","cf:SS1_CD","cf:TWG_Y","cf:WF_MRR","cf:WFPT_PCM"])
    result = pd.DataFrame()
    for key,value in data11:
        keylist.append(key)
        valuelist.append(value)
    if(len(valuelist)>0):
        result=pd.DataFrame(valuelist)
        result.columns = [name1.replace("cf:", "") for name1 in result.columns]
#        if("cf:BLK_SS1_Y"in result.columns and 
#            "cf:LET_CD"in result.columns and "cf:LET_Y"in result.columns and 
#            "cf:LS_DP"in result.columns and "cf:PT2C_UZ55"in result.columns and
#            "cf:R2_AVG"in result.columns and "cf:SS1_CD"in result.columns and
#            "cf:TWG_Y"in result.columns and "cf:WF_MRR"in result.columns and
#            "cf:WFPT_PCM"in result.columns):
#            result_df = result[["cf:WAFER","cf:BLOCK","cf:BLK_SS1_Y","cf:LET_CD","cf:LET_Y","cf:LS_DP","cf:PT2C_UZ55",
#                                "cf:R2_AVG","cf:SS1_CD","cf:TWG_Y","cf:WF_MRR","cf:WFPT_PCM"]]
#            result_df.columns=["WAFER","BLOCK","BLK_SS1_Y","LET_CD","LET_Y","LS_DP",
#                               "PT2C_UZ55","R2_AVG","SS1_CD","TWG_Y","WF_MRR","WFPT_PCM"]
        result[["BLK_SS1_Y",
        "LET_CD","LET_Y","LS_DP","PT2C_UZ55",
        "R2_AVG","SS1_CD","TWG_Y","WF_MRR","WFPT_PCM"]
    ] = result[["BLK_SS1_Y",
        "LET_CD","LET_Y","LS_DP","PT2C_UZ55",
        "R2_AVG","SS1_CD","TWG_Y","WF_MRR","WFPT_PCM"]
    ].apply(lambda x: x.fillna(x.astype(float).mean()),axis=0)
        result= result[["WAFER","BLOCK","BLK_SS1_Y","LET_CD","LET_Y","LS_DP",
                         "PT2C_UZ55","R2_AVG","SS1_CD","TWG_Y","WF_MRR","WFPT_PCM"]]
    else:
        result=pd.DataFrame()
    return(result)
    
    
def external_wafer_data_mp(connection,wafer):
    keylist=[]
    valuelist=[]
    table = connection.table('external_wafer_data_mp')
    data11 = table.scan(row_prefix=wafer,columns=[
        "cf:Wafer","cf:Bevel_Angle DTT","cf:Coil_R DTT","cf:Coil_R_Yield",
         "cf:DFH_R1 DTT","cf:DFH_R2 DTT","cf:DFH1_Yield","cf:DFH2_Yield",
         "cf:FLW DTT", "cf:HDI_R DTT","cf:HDI_Yield","cf:P2T DTT", "cf:Pin_Thickness DTT",
         "cf:PWA DTT","cf:Read_Gap_bottom DTT","cf:TRA DTT",
         "cf:UC_Thickness DTT", "cf:Wafer_IR_Yield","cf:Write_Gap DTT"])
    for key,value in data11:
        keylist.append(key)
        valuelist.append(value)
    result = pd.DataFrame()
    if(len(valuelist)>0):
        result=pd.DataFrame(valuelist)
        result.columns = [name1.replace("cf:", "") for name1 in result.columns]
        result[["Bevel_Angle DTT","Coil_R DTT","Coil_R_Yield",
         "DFH_R1 DTT","DFH_R2 DTT","DFH1_Yield","DFH2_Yield",
         "FLW DTT", "HDI_R DTT","HDI_Yield","P2T DTT", "Pin_Thickness DTT",
         "PWA DTT","Read_Gap_bottom DTT","TRA DTT",
         "UC_Thickness DTT", "Wafer_IR_Yield","Write_Gap DTT"]
    ] = result[["Bevel_Angle DTT","Coil_R DTT","Coil_R_Yield",
         "DFH_R1 DTT","DFH_R2 DTT","DFH1_Yield","DFH2_Yield",
         "FLW DTT", "HDI_R DTT","HDI_Yield","P2T DTT", "Pin_Thickness DTT",
         "PWA DTT","Read_Gap_bottom DTT","TRA DTT",
         "UC_Thickness DTT", "Wafer_IR_Yield","Write_Gap DTT"]
    ].apply(lambda x: x.fillna(x.astype(float).mean()),axis=0)
    
        result = result[["Wafer","Bevel_Angle DTT","Coil_R DTT","Coil_R_Yield",
         "DFH_R1 DTT","DFH_R2 DTT","DFH1_Yield","DFH2_Yield",
         "FLW DTT", "HDI_R DTT","HDI_Yield","P2T DTT", "Pin_Thickness DTT",
         "PWA DTT","Read_Gap_bottom DTT","TRA DTT",
         "UC_Thickness DTT", "Wafer_IR_Yield","Write_Gap DTT"]]
        result = result.rename(columns={'Wafer': 'WAFER'})
    else:
        result=pd.DataFrame()
    return(result)
    

def spc_wafer_data_mp(connection,wafer):
    keylist=[]
    valuelist=[]
    table = connection.table('spc_wafer_data_mp')
    data11 = table.scan(row_prefix=wafer,columns=[
        "cf:WAFER","cf:TWG2_X39X_LS_DP_AVG","cf:TWG2_X39X_TWGD_P_AVE",
        "cf:X48K_TSSL_CDM_AVG","cf:X48L_TSSL_OVL_Y_AVG"])
    for key,value in data11:
        keylist.append(key)
        valuelist.append(value)
    result = pd.DataFrame()
    if(len(valuelist)>0):
        result=pd.DataFrame(valuelist)
        result.columns = [name1.replace("cf:", "") for name1 in result.columns]
        result[["TWG2_X39X_LS_DP_AVG","TWG2_X39X_TWGD_P_AVE",
        "X48K_TSSL_CDM_AVG","X48L_TSSL_OVL_Y_AVG"]
    ] = result[["TWG2_X39X_LS_DP_AVG","TWG2_X39X_TWGD_P_AVE",
        "X48K_TSSL_CDM_AVG","X48L_TSSL_OVL_Y_AVG"]
    ].apply(lambda x: x.fillna(x.astype(float).mean()),axis=0)
    else:
        result=pd.DataFrame()
    return(result)



def MG08_getDataFromBigtablemain(connection,wafer,MARKING):
    keylist=[]
    valuelist=[]
    table = connection.table('mes_bigtable')
    data11 = table.scan(row_prefix=wafer,columns=[
        "cf:HEAD_SN","cf:WAFER","cf:[DP].MARKING","cf:[DP].SSEB_EWI(nm)-R2",
        "cf:[DP].Sqz.BER(dec)-R2", "cf:[DP].Sqz.BER(dec)-R1","cf:[DP].OW2(dB)-R2",
        "cf:[DP].OW2(dB)-R1","cf:[DP].BER(dec)-R2","cf:[DP].BER(dec)-R1"])
    for key,value in data11:
        keylist.append(key)
        valuelist.append(value)
    result = pd.DataFrame()
    if(len(valuelist)>0):
        df_dp1=pd.DataFrame(valuelist)
        df_dp1.columns = [name1.replace("cf:", "") for name1 in df_dp1.columns]
        df_dp1 = df_dp1[["HEAD_SN",
                        "WAFER",
                        "[DP].MARKING",
                        "[DP].SSEB_EWI(nm)-R2",
                        "[DP].Sqz.BER(dec)-R2",
                        "[DP].Sqz.BER(dec)-R1",
                        "[DP].OW2(dB)-R2",
                        "[DP].OW2(dB)-R1",
                        "[DP].BER(dec)-R2",
                        "[DP].BER(dec)-R1"]]
        df_dp1[df_dp1.columns[3:]] = df_dp1[df_dp1.columns[3:]].apply(pd.to_numeric)
        df_dp1 = df_dp1.dropna()
        df_dp1.columns=["HEAD_SN",
                        "WAFER",
                        "MARKING",
                        "SSEB_EWI",
                        "Sqz.BER(dec)-R2",
                        "Sqz.BER(dec)-R1",
                        "OW2(dB)-R2",
                        "OW2(dB)-R1",
                        "BER(dec)-R2",
                        "BER(dec)-R1"]
        df_dp1 = df_dp1.query('SSEB_EWI > 51').query('SSEB_EWI < 53')
        df_dp = df_dp1[["HEAD_SN","WAFER","MARKING","Sqz.BER(dec)-R1","Sqz.BER(dec)-R2",
                        "OW2(dB)-R1","OW2(dB)-R2","BER(dec)-R1","BER(dec)-R2"]]
        result = df_dp.groupby(['WAFER','MARKING'],as_index=False).mean()
    else:
        result=pd.DataFrame()
    return(result)


#def MG08_getDataFromBigtablemain(wafer,MARKING):
#    try:
#        es_username = 'pe'
#        es_passwd = '!*1Zx9'
#        if (len(wafer) == 6):
#            WAFER = wafer[1:6]
#        else:
#            WAFER = wafer
#        doc = {
#            "query": {
#                "bool": {
#                    "filter": [
#                        {"term": {
#                            "WAFER": WAFER
#                        }},
#                        {"term": {
#                            "[DP].MARKING": MARKING
#                        }}
#                    ]
#
#                }
#            },
#            "_source": ["HEAD_SN",
#                        "WAFER",
#                        "[DP].MARKING",
#                        "[DP].SSEB_EWI(nm)-R2",
#                        "[DP].Sqz.BER(dec)-R2",
#                        "[DP].Sqz.BER(dec)-R1",
#                        "[DP].OW2(dB)-R2",
#                        "[DP].OW2(dB)-R1",
#                        "[DP].BER(dec)-R2",
#                        "[DP].BER(dec)-R1"],
#
#            "size": 100000
#        }
#
#        es = Elasticsearch(
#            hosts=[{'host': 'dn2meshd01.sae.com.hk', 'port': 19200},
#                   {'host': 'dn2meshd02.sae.com.hk', 'port': 19200},
#                   {'host': 'dn2meshd04.sae.com.hk', 'port': 19200},
#                   {'host': 'dn2meshd07.sae.com.hk', 'port': 19200},
#                   {'host': 'dn2meshd08.sae.com.hk', 'port': 19200},
#                   {'host': 'dn2meshd09.sae.com.hk', 'port': 19200}], http_auth=(es_username, es_passwd),
#            timeout=60,
#            max_retries=10, retry_on_timeout=True)
#        res = es.search(index="bigtable", body=doc, request_timeout=60)
#
#        res_bucket = res['hits']['hits']
#        gp_num = len(res_bucket)
#
#        list1 = []
#
#        if gp_num != 0:
#            for j in range(gp_num):
#                list1.append(res_bucket[j]['_source'])
#        result = pd.DataFrame(list1)
#        if("[DP].MARKING" in result.columns and "[DP].SSEB_EWI(nm)-R2" in result.columns and
#       "[DP].Sqz.BER(dec)-R2" in result.columns and "[DP].Sqz.BER(dec)-R1" in result.columns and
#       "[DP].OW2(dB)-R2" in result.columns and "[DP].OW2(dB)-R1" in result.columns and
#       "[DP].BER(dec)-R2" in result.columns and "[DP].BER(dec)-R1" in result.columns):
#            df_dp1 = result[["HEAD_SN",
#                            "WAFER",
#                            "[DP].MARKING",
#                            "[DP].SSEB_EWI(nm)-R2",
#                            "[DP].Sqz.BER(dec)-R2",
#                            "[DP].Sqz.BER(dec)-R1",
#                            "[DP].OW2(dB)-R2",
#                            "[DP].OW2(dB)-R1",
#                            "[DP].BER(dec)-R2",
#                            "[DP].BER(dec)-R1"]] 
#            df_dp1[df_dp1.columns[3:]] = df_dp1[df_dp1.columns[3:]].apply(pd.to_numeric)
#            df_dp1 = df_dp1.dropna()
#            
#            
#            df_dp1.columns=["HEAD_SN",
#                            "WAFER",
#                            "MARKING",
#                            "SSEB_EWI",
#                            "Sqz.BER(dec)-R2",
#                            "Sqz.BER(dec)-R1",
#                            "OW2(dB)-R2",
#                            "OW2(dB)-R1",
#                            "BER(dec)-R2",
#                            "BER(dec)-R1"]
#            df_dp1 = df_dp1.query('SSEB_EWI > 51').query('SSEB_EWI < 53')
#            df_dp = df_dp1[["HEAD_SN","WAFER","MARKING","Sqz.BER(dec)-R1","Sqz.BER(dec)-R2","OW2(dB)-R1","OW2(dB)-R2","BER(dec)-R1","BER(dec)-R2"]]
#            
#            dp_summary = df_dp.groupby(['WAFER','MARKING'],as_index=False).mean()
#        else:
#            dp_summary = pd.DataFrame() 
#    finally:
#        print 'finish'
#    return(dp_summary)
    
def main(input):
    wafer=input['WAFER'][0]
    project_code=input['project_code'][0]
    wafer_no_project_code=pd.DataFrame([[project_code,wafer]],columns=['Project_Code','WAFER'])
    try:
        connection = happybase.Connection(host='DN2MESHD04.sae.com.hk', port=9090)
        print(connection.tables())
        connection.open()
        #----------------------external_wafer_data_mp----------------------
        wafer_data = external_wafer_data_mp(connection,wafer)
        #----------------------ff_CSV-------------------
        ffCSV_data = ffCSVData(connection,wafer)
        ffCSV_data[ffCSV_data.columns[2:]] = ffCSV_data[ffCSV_data.columns[2:]].apply(pd.to_numeric)
        if (len(ffCSV_data) >0 ):
            ffCSV_data_by_wafer = pd.DataFrame(ffCSV_data.groupby(['WAFER'],as_index=False).mean())
            names_FF = ffCSV_data_by_wafer.columns.tolist()
            names_FF[1:] = ("mean_"+ffCSV_data_by_wafer.columns[1:]).tolist()
            ffCSV_data_by_wafer.columns = names_FF
        else:
            ffCSV_data_by_wafer =pd.DataFrame()
        #----------------------spc_wafer_data_mp----------------------
        spc_wafer_data = spc_wafer_data_mp(connection,wafer)
        if spc_wafer_data['X48K_TSSL_CDM_AVG'][0]=='' or spc_wafer_data['X48L_TSSL_OVL_Y_AVG'][0]=='':
            spc_wafer_data = pd.DataFrame()
        #----------------------1P---------------------------------------
        ow_ber_1p = MG08_getDataFromBigtablemain(connection,wafer,MARKING="1P")
        if len(ow_ber_1p) > 0 :
            ow_ber_1p1 = pd.melt(ow_ber_1p, id_vars=['WAFER','MARKING'], 
                                 value_vars=['Sqz.BER(dec)-R1', 'Sqz.BER(dec)-R2','OW2(dB)-R1', 
                                             'OW2(dB)-R2', 'BER(dec)-R1', 'BER(dec)-R2'])
            ow_ber_1p1['key'] = ow_ber_1p1['MARKING']+'_'+ow_ber_1p1['variable']
            ow_ber_1p2 = pd.DataFrame(ow_ber_1p1.pivot_table(index=['WAFER'],columns=['key'],values="value"))
        else:
            ow_ber_1p2 = pd.DataFrame()
        if (len(wafer_data)>0  and len(ffCSV_data_by_wafer)>0 and len(spc_wafer_data) >0 and len(ow_ber_1p2) >0):
            d0 = pd.merge(wafer_no_project_code,wafer_data,on='WAFER',how="left")
            d1=pd.merge(d0,ffCSV_data_by_wafer,on=['WAFER'],how="left")
            d2=pd.merge(d1,spc_wafer_data,on=['WAFER'],how="left")
            output=pd.merge(d2,ow_ber_1p2,on=['WAFER'],how="left")
            output['MissingAlarm'] = 0
        else:
            x=[wafer_data.size,ffCSV_data_by_wafer.size,spc_wafer_data.size,ow_ber_1p2.size]
            ErrorFile = str(['external_wafer_data_mp','ffCSV','spc_wafer_data_mp','ow_ber_1p_data'][np.min(np.where(x==np.min(x)))])
            wafer_no_project_code=pd.DataFrame([[project_code,wafer,ErrorFile]],columns=['project_code','wafer','ErrorFile'])
            output=wafer_no_project_code
            output['MissingAlarm'] = 1
    finally:
        if connection:
            connection.close()
    return(output)
    
    
    
    
input = pd.DataFrame([{'WAFER':'0AAFD','project_code' :'BTSAD0'}])
output = main(input)
