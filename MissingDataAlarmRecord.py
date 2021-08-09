# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 15:39:42 2020

@author: M172468
"""

import happybase
import numpy as np
import pandas as pd
import time
import pymysql
from sqlalchemy import create_engine

a = ['10A', '52A', '70A', '26B', '44B', '78B', '10C', '52C', '70C', '26D', '44D', '78D',
     '20E', '18F', '20G', '18H', '20J', '40J', '60J', '20K', '40K', '60K', '18P', '26Q', '18R', '26S']
slider_stamp_project_code = [
    "BTSQ80",
    "BTST90",
    "BTSAU0",
    "BTSY10",
    "BTSAD0",
    "TSDJ40",
    "TSDF30",
    "TSDK80",
    "BTSAN0",
    "BTSAT0",
    "BTSAL0",
    "BTSBF0",
    "TSDL30"
]


def addwafer(wafer, string1):
    return (str(wafer) + string1)


# ----------------------pcm----------------------
def pcmData(connection, wafer):
    keylist = []
    valuelist = []
    rplist = map(addwafer, [wafer] * len(a), a)
    for i in range(len(rplist)):
        rp = rplist[i]
        table = connection.table('wafer_incoming_pcm')
        data11 = table.scan(row_prefix=rp)
        for key, value in data11:
            keylist.append(key)
            valuelist.append(value)
    result = pd.DataFrame()
    if (len(valuelist) > 0):
        result = pd.DataFrame(valuelist)
        if ('cf:PCM' in result.columns):
            result_df = result[['cf:WAFER', 'cf:BLOCK', 'cf:ROW', 'cf:COLUMN', 'cf:PCM']]
            result_df.columns = ['WAFER', 'BLOCK', 'ROW', 'COLUMN', 'PCM']
        else:
            result_df = pd.DataFrame()
            return (result_df)
    else:
        result_df = pd.DataFrame()
    return (result_df)


# ----------------------fpd----------------------
def fpdData(connection, wafer):
    keylist = []
    valuelist = []
    rplist = map(addwafer, [wafer] * len(a), a)
    for i in range(len(rplist)):
        rp = rplist[i]
        table = connection.table('wafer_incoming_fpd')
        data11 = table.scan(row_prefix=rp)
        for key, value in data11:
            keylist.append(key)
            valuelist.append(value)
    result = pd.DataFrame()
    if (len(valuelist) > 0):
        result = pd.DataFrame(valuelist)
        if ("cf:ELG_STATUS" in result.columns and "cf:ELG_RES" in result.columns and "cf:WRT_RES" in result.columns and
                "cf:WRT_IND" in result.columns and "cf:MR" in result.columns and "cf:HR" in result.columns and
                "cf:HDI" in result.columns and "cf:HRW" in result.columns and "cf:ELG_CODE" in result.columns and
                "cf:ELG_FLAG" in result.columns and "cf:WELG_STATUS" in result.columns and
                "cf:WELG_RES" in result.columns and "cf:WELG_FLAG" in result.columns):
            result_df = result[["cf:WAFER", "cf:BLOCK", "cf:ROW", "cf:COLUMN", "cf:ELG_STATUS",
                                "cf:ELG_RES", "cf:WRT_RES", "cf:WRT_IND", "cf:MR", "cf:HR", "cf:HDI",
                                "cf:HRW", "cf:ELG_CODE", "cf:ELG_FLAG", "cf:WELG_STATUS", "cf:WELG_RES",
                                "cf:WELG_FLAG"]]
            result_df.columns = ["WAFER", "BLOCK", "ROW", "COLUMN", "ELG_STATUS",
                                 "ELG_RES", "WRT_RES", "WRT_IND", "MR", "HR", "HDI",
                                 "HRW", "ELG_CODE", "ELG_FLAG", "WELG_STATUS", "WELG_RES", "WELG_FLAG"]
        else:
            result_df = pd.DataFrame()
            return (result_df)
    else:
        result_df = pd.DataFrame()
    return (result_df)


# ----------------------ff_CSV----------------------
def ffCSVData(connection, wafer):
    keylist = []
    valuelist = []
    rp = str(wafer)
    table = connection.table('wafer_incoming_ff')
    data11 = table.scan(row_prefix=rp)
    result = pd.DataFrame()
    for key, value in data11:
        keylist.append(key)
        valuelist.append(value)
    if (len(valuelist) > 0):
        result = pd.DataFrame(valuelist)
        if ('cf:R4_AVG' in result.columns and 'cf:TWG_Y' in result.columns and
                'cf:WF_MRR' in result.columns and 'cf:BLK_SS1_Y' in result.columns and
                'cf:PT2C_UZ55' in result.columns and
                'cf:FLW' in result.columns and
                'cf:LET_CD' in result.columns and
                'cf:LET_Y' in result.columns and
                'cf:LS_DP' in result.columns and
                'cf:R2_AVG' in result.columns and
                'cf:RUGAP' in result.columns and
                'cf:SS1_CD' in result.columns and
                'cf:TRA' in result.columns and
                'cf:TWGD_P' in result.columns and
                'cf:WF_SS1_Y' in result.columns and
                'cf:WFPT_PCM' in result.columns and
                'cf:X0D2_PWA_F' in result.columns
            ):
            result_df = result[
                ['cf:WAFER', 'cf:BLOCK', 'cf:R4_AVG', 'cf:TWG_Y', 'cf:WF_MRR', 'cf:BLK_SS1_Y', 'cf:PT2C_UZ55',
                 'cf:FLW','cf:LET_CD' ,'cf:LET_Y','cf:LS_DP','cf:R2_AVG','cf:RUGAP','cf:SS1_CD','cf:TRA','cf:TWGD_P','cf:WF_SS1_Y','cf:WFPT_PCM','cf:X0D2_PWA_F']]
            result_df.columns = ['WAFER', 'BLOCK', 'FF_R4_AVG', 'FF_TWG_Y', 'FF_WF_MRR', 'FF_BLK_SS1_Y', 'FF_PT2C_UZ55',
                                 'FLW','LET_CD' ,'LET_Y','LS_DP','R2_AVG','RUGAP','SS1_CD','TRA','TWGD_P','WF_SS1_Y','WFPT_PCM',
                                 'X0D2_PWA_F']
        else:
            result_df = pd.DataFrame()
            return (result_df)
    else:
        result_df = pd.DataFrame()
    return (result_df)


# ----------------------teg----------------------
def TEG(connection, wafer):
    keylist = []
    valuelist = []
    table = connection.table('wafer_incoming_teg')
    data11 = table.scan(row_prefix=wafer)
    result = pd.DataFrame()
    for key, value in data11:
        keylist.append(key)
        valuelist.append(value)
    if (len(valuelist) > 0):
        result = pd.DataFrame(valuelist)
        if ('cf:ELG' in result.columns and 'cf:WELG' in result.columns):
            ELG_data = result['cf:ELG'].astype('float')
            WELG_data = result['cf:WELG'].astype('float')
            ELG_data[(ELG_data > 25.0) | (ELG_data < 5.0)] = np.NaN
            WELG_data[(WELG_data > 25.0) | (WELG_data < 5.0)] = np.NaN
        else:
            result_2 = pd.DataFrame()
            return (result_2)
        result_1 = pd.concat([result[['cf:WAFER']], ELG_data, WELG_data], axis=1)
        result_1 = result_1.dropna()
        result_2 = pd.DataFrame(result_1.groupby(['cf:WAFER'], as_index=False).mean())
        result_2.columns = ['WAFER', 'R4_MEAN', 'W1_MEAN']
    else:
        result_2 = pd.DataFrame()
    return (result_2)


def main(input):
    wafer_no = input['WAFER_NO'][0]
    project_code = input['PROJECT'][0]
    wafer = wafer_no[1:6]
    flag = 0
    wafer_no_project_code = pd.DataFrame([[wafer_no, project_code, wafer, flag]],
                                         columns=['wafer_no', 'project_code', 'wafer', 'flag'])
    if project_code in slider_stamp_project_code:
        time.sleep(1600)
        try:
            connection = happybase.Connection(host='DN2MESHD06.sae.com.hk', port=9090)
            print(connection.tables())
            connection.open()
            # ----------------------pcm----------------------
            pcmresult = pcmData(connection, wafer)
            # ----------------------fpd----------------------
            fpdresult = fpdData(connection, wafer)
            # ----------------------ff_CSV-------------------
            ffCSVresult = ffCSVData(connection, wafer)
            # ----------------------teg----------------------
            TEGresult = TEG(connection, wafer)
            if (len(pcmresult) > 0 and len(fpdresult) > 0 and len(ffCSVresult) > 0 and len(TEGresult) > 0):
                print("good")
            else:
                x = [pcmresult.size, fpdresult.size, ffCSVresult.size, TEGresult.size]
                ErrorFile_initial = np.array(['pcm', 'fpd', 'ffCSV', 'TEG'])[(np.where(x == np.min(x))[0])].tolist()
                str = '%20'
                ErrorFile = str.join(ErrorFile_initial)
                wafer_no_project_code = pd.DataFrame([[wafer_no, project_code, wafer, ErrorFile, flag]],
                                                     columns=['wafer_no', 'project_code', 'wafer', 'ErrorFile', 'flag'])
                wafer_no_project_code['flag'] = 1
                
                ErrorFile_to_sql = ','.join(ErrorFile_initial)
                wafer_no_project_code_to_sql = pd.DataFrame([[wafer_no, project_code, wafer, ErrorFile_to_sql, flag]],
                                                     columns=['wafer_no', 'project_code', 'wafer', 'ErrorFile', 'flag'])
                wafer_no_project_code_to_sql['flag'] = 1
                engine = create_engine('mysql+pymysql://de:$Dk9Ls66@10.10.1.28:3306/mes_de?charset=utf8')
                wafer_no_project_code_to_sql.to_sql('J0001_Data_Missing_Alarm_record', con=engine, if_exists='append',index=False)
        finally:
            if connection:
                connection.close()
    output = wafer_no_project_code
    return (output)


input = pd.DataFrame([['H9B5H2','BTSAD0']],columns = ['WAFER_NO','PROJECT'])
output=main(input)

