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
        result[["BLK_SS1_Y",
        "LET_Y","LS_DP","PT2C_UZ55",
        "R2_AVG","SS1_CD","TWG_Y","WF_MRR","WFPT_PCM"]
    ] = result[["BLK_SS1_Y",
        "LET_Y","LS_DP","PT2C_UZ55",
        "R2_AVG","SS1_CD","TWG_Y","WF_MRR","WFPT_PCM"]
    ].apply(lambda x: x.fillna(x.astype(float).mean()),axis=0)
        result= result[["WAFER","BLOCK","BLK_SS1_Y","LET_Y","LS_DP",
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

def AL15SE_getDataFromBigtablemain(connection,wafer,MARKING):
    keylist=[]
    valuelist=[]
    table = connection.table('mes_bigtable')
    data11 = table.scan(row_prefix=wafer,columns=[
        "cf:HEAD_SN","cf:WAFER","cf:[DP].MARKING","cf:[DP].EWAC(nm)",
                        "cf:[DP].OW1(dB)",
                        "cf:[DP].BER(dec)",
                        "cf:[DP].Sqz.BER(dec)"])
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
                        "[DP].EWAC(nm)",
                        "[DP].OW1(dB)",
                        "[DP].BER(dec)",
                        "[DP].Sqz.BER(dec)"]]
        df_dp1[df_dp1.columns[3:]] = df_dp1[df_dp1.columns[3:]].apply(pd.to_numeric)
        df_dp1 = df_dp1.dropna()
        df_dp1.columns=["HEAD_SN",
                        "WAFER",
                        "MARKING",
                        "EWAC",
                        "OW1",
                        "BER",
                        "Sqz.BER"]
        df_dp1 = df_dp1.query('EWAC > 66').query('EWAC < 69')
        df_dp = df_dp1[["HEAD_SN","WAFER","MARKING","OW1","BER","Sqz.BER"]]
        result = df_dp.groupby(['WAFER','MARKING'],as_index=False).mean()
    else:
        result=pd.DataFrame()
    return(result)

    
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
        #----------------------1P---------------------------------------
        ow_ber_1p = AL15SE_getDataFromBigtablemain(connection,wafer,MARKING="P")
        if len(ow_ber_1p) > 0 :
            ow_ber_1p1 = pd.melt(ow_ber_1p, id_vars=['WAFER','MARKING'], 
                                 value_vars=['OW1', 'BER','Sqz.BER'])
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
    
    
    
input = pd.DataFrame([{'WAFER':'9B5D5','project_code' :'BTSQ80'}])
output = main(input)