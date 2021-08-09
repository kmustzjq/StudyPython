import happybase
import pandas as pd
#from elasticsearch import Elasticsearch


def MG08_ADC_WaferData(connection,WAFER):
    keylist=[]
    valuelist=[]
    table = connection.table('mes_bigtable')
    data11 = table.scan(row_prefix=WAFER,columns=[
        "cf:HEAD_SN","cf:WAFER","cf:[DP].RESULT","cf:[DP].MARKING","cf:[QST].MRR","cf:[QST].R2_MRR",
        "cf:[QST].AMP","cf:[DP].SSEB_EBI(nm)-R2","cf:[DP].SSEB_EWI(nm)-R2",
        "cf:[DP].MT50(nm)-R2","cf:[DP].OW2(dB)-R1","cf:[DP].OW2(dB)-R2","cf:[DP].TAAL(mV)-R1",
        "cf:[DP].BER(dec)-R1","cf:[DP].Sqz.BER(dec)-R1","cf:[DP].Sqz.BER(dec)-R2",
        "cf:[QST].NRMS","cf:[DP].MT50(nm)-R1","cf:[QST].R2_AMP","cf:[DP].Pasym(%)-R2",
        "cf:[DP].TAAL(mV)-R2","cf:[QST].R2_NRMS","cf:[DP].BER(dec)-R2","cf:[DP].RESM(%)-R2",
        "cf:[DP].RESM(%)-R1","cf:[DP].Pasym(%)-R1","cf:[QST].DMRR","cf:[DP].TAAM(mV)-R2",
        "cf:[DP].NLD(%)-R2","cf:[QST].NPEAK","cf:[DP].NLD(%)-R1","cf:[QST].R2_TCR2",
        "cf:[DP].MT10(nm)-R1","cf:[QST].R2_DMRR","cf:[DP].MT10(nm)-R2"])
    for key,value in data11:
        keylist.append(key)
        valuelist.append(value)
    if(len(valuelist)>0):
        df_dp1=pd.DataFrame(valuelist)
        df_dp1.columns = [name1.replace("cf:", "") for name1 in df_dp1.columns]
        df_dp1 = df_dp1[["HEAD_SN","WAFER","[DP].RESULT","[DP].MARKING","[QST].MRR","[QST].R2_MRR",
                                "[QST].AMP","[DP].SSEB_EBI(nm)-R2","[DP].SSEB_EWI(nm)-R2",
                                "[DP].MT50(nm)-R2","[DP].OW2(dB)-R1","[DP].OW2(dB)-R2","[DP].TAAL(mV)-R1",
                                "[DP].BER(dec)-R1","[DP].Sqz.BER(dec)-R1","[DP].Sqz.BER(dec)-R2",
                                "[QST].NRMS","[DP].MT50(nm)-R1","[QST].R2_AMP","[DP].Pasym(%)-R2",
                                "[DP].TAAL(mV)-R2","[QST].R2_NRMS","[DP].BER(dec)-R2","[DP].RESM(%)-R2",
                                "[DP].RESM(%)-R1","[DP].Pasym(%)-R1","[QST].DMRR","[DP].TAAM(mV)-R2",
                                "[DP].NLD(%)-R2","[QST].NPEAK","[DP].NLD(%)-R1","[QST].R2_TCR2",
                                "[DP].MT10(nm)-R1","[QST].R2_DMRR","[DP].MT10(nm)-R2"]]
        WaferData = df_dp1.dropna()
    else:
        WaferData = pd.DataFrame()
    return(WaferData)



def external_wafer_data_mp(connection,WAFER):
    keylist = []
    valuelist = []
    table = connection.table('external_wafer_data_mp')
    data11 = table.scan(row_prefix=WAFER,columns=[
            "cf:Wafer",
            "cf:PWA DTT",
            "cf:PT2 DTT",
            "cf:Bevel_Angle DTT",
            "cf:Neck_Height DTT",
            "cf:Side_Gap DTT",
            "cf:P2T DTT",
            "cf:FLW DTT",
            "cf:FLW_R2 DTT",
            "cf:IMOS_R1 DTT",
            "cf:IMOS_R2 DTT",
            "cf:TRA_R2 DTT",
            "cf:MRT_JS_NF_R2 DTT",
            "cf:Pin_Thickness_R2 DTT",
            "cf:RWS DTT"])
    result = pd.DataFrame()
    for key,value in data11:
        keylist.append(key)
        valuelist.append(value)
    if(len(valuelist)>0):
        result=pd.DataFrame(valuelist)
        result.columns = [name1.replace("cf:", "") for name1 in result.columns]    
        wafer_data = result[[
                    "Wafer",
                    "PWA DTT",
                    "PT2 DTT",
                    "Bevel_Angle DTT",
                    "Neck_Height DTT",
                    "Side_Gap DTT",
                    "P2T DTT",
                    "FLW DTT",
                    "FLW_R2 DTT",
                    "IMOS_R1 DTT",
                    "IMOS_R2 DTT",
                    "TRA_R2 DTT",
                    "MRT_JS_NF_R2 DTT",
                    "Pin_Thickness_R2 DTT",
                    "RWS DTT"]]
        wafer_data = wafer_data.rename(columns={'Wafer': 'WAFER'})
         
    else:
        wafer_data = pd.DataFrame()
    return(wafer_data)


def main(input):
    WAFER = str(input['WAFER'][0])
    proj_code = str(input['proj_code'][0])
    #external_wafer_data_mp
    connection = happybase.Connection(host='DN2MESHD06.sae.com.hk', port=9090)
    connection.open()
    
    WaferData = MG08_ADC_WaferData(connection,WAFER)
    wafer_data = external_wafer_data_mp(connection,WAFER)
    if (len(WaferData) > 0 and len(wafer_data) > 0):
        output = WaferData.merge(wafer_data, on='WAFER', how='left')
        output = output.assign(PROJECT_CODE = proj_code)
        output = output[[
                    "PROJECT_CODE",
                    "HEAD_SN",
                    "WAFER",
                    "[DP].RESULT",
                    "[DP].MARKING",
                    "[QST].MRR",
                    "[QST].R2_MRR",
                    "[QST].AMP",
                    "[DP].SSEB_EBI(nm)-R2",
                    "[DP].SSEB_EWI(nm)-R2",
                    "[DP].MT50(nm)-R2",
                    "[DP].OW2(dB)-R1",
                    "[DP].OW2(dB)-R2",
                    "[DP].TAAL(mV)-R1",
                    "[DP].BER(dec)-R1",
                    "[DP].Sqz.BER(dec)-R1",
                    "[DP].Sqz.BER(dec)-R2",
                    "[QST].NRMS",
                    "[DP].MT50(nm)-R1",
                    "[QST].R2_AMP",
                    "[DP].Pasym(%)-R2",
                    "[DP].TAAL(mV)-R2",
                    "[QST].R2_NRMS",
                    "[DP].BER(dec)-R2",
                    "[DP].MT10(nm)-R2",
                    "[DP].RESM(%)-R2",
                    "[DP].RESM(%)-R1",
                    "[DP].Pasym(%)-R1",
                    "[QST].DMRR",
                    "[DP].TAAM(mV)-R2",
                    "[DP].NLD(%)-R2",
                    "[QST].NPEAK",
                    "[DP].NLD(%)-R1",
                    "[QST].R2_TCR2",
                    "[DP].MT10(nm)-R1",
                    "[QST].R2_DMRR",
                    "PWA DTT",
                    "PT2 DTT",
                    "Bevel_Angle DTT",
                    "Neck_Height DTT",
                    "Side_Gap DTT",
                    "P2T DTT",
                    "FLW DTT",
                    "FLW_R2 DTT",
                    "IMOS_R1 DTT",
                    "IMOS_R2 DTT",
                    "TRA_R2 DTT",
                    "MRT_JS_NF_R2 DTT",
                    "Pin_Thickness_R2 DTT",
                    "RWS DTT"]]
        output.columns = [
                    "PROJECT_CODE",
                    "HEAD_SN",
                    "WAFER",
                    "D_RESULT",
                    "D_MARKING",
                    "BQ_MRR_2",
                    "BQ_MRR_2_R2",
                    "BQ_AMP_2",
                    "SSEB_EB_R2",
                    "SSEB_EW_R2",
                    "UMRW_R2",
                    "OW",
                    "OW_R2",
                    "LF_HGA_TAA",
                    "SOVABER",
                    "SQZ_SOVABER",
                    "SQZ_SOVABER_R2",
                    "BQ_NRMS_2",
                    "UMRW",
                    "BQ_AMP_2_R2",
                    "TAA_ASYM_R2",
                    "LF_HGA_TAA_R2",
                    "BQ_NRMS_2_R2",
                    "SOVABER_R2",
                    "UMRW2_R2",
                    "RESM_R2",
                    "RESM",
                    "TAA_ASYM",
                    "BQ_DMRR2",
                    "MF_TAA_MV__R2",
                    "P_SPECTNLD_R2",
                    "BQ_NPEAK_2",
                    "P_SPECTNLD",
                    "BQ_TCR2_2_R2",
                    "UMRW2",
                    "BQ_DMRR2_R2",
                    "PWA DTT",
                    "PT2 DTT",
                    "Bevel_Angle DTT",
                    "Neck_Height DTT",
                    "Side_Gap DTT",
                    "P2T DTT",
                    "FLW DTT",
                    "FLW_R2 DTT",
                    "IMOS_R1 DTT",
                    "IMOS_R2 DTT",
                    "TRA_R2 DTT",
                    "MRT_JS_NF_R2 DTT",
                    "Pin_Thickness_R2 DTT",
                    "RWS DTT"]
    else:
        output = input
        output['MissingAlarm'] = 1
    return (output)



input = pd.DataFrame([{'WAFER':'0A7H2','proj_code' :'BTSAD0'}])
output = main(input)