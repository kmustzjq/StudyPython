import happybase
#import numpy as np
import pandas as pd 

a=[  
#   "10A", "52A", "70A","26B","44B","78B","10C",
#  "52C","70C","26D","44D","78D","20E","18F","20G","18H","20J","40J","60J",
#  "20K","40K","60K","18P","26Q","18R","26S"

   "08A","18A","26A","34A","42A","50A","60A","68A","78A","86A","94A",
  "08C","18C","26C","34C","42C","50C","60C", "68C","78C", "86C","94C","08B",
  "16B","24B","34B","42B","52B","60B","68B","76B","86B","94B","08D","16D",
  "24D","34D","42D","52D","60D","68D","76D","86D","94D","10J","30J","50J", 
  "70J","10K","30K","50K","70K","08E","18E", "28E","38E","08G","18G","28G",
  "38G","08F","16F","26F","36F","08H","16H","26H","36H","08R","16R","26R",
  "34R","42R","08P","16P","26P","34P","42P","08Q","16Q","24Q","34Q","42Q",
  "08S","16S","24S","34S","42S"
   
]
def addwafer(wafer,string1):
    return (str(wafer)+string1)
#---------------wafer_shipment_data-------------
def mes_bigtable(connection,wafer):
    keylist=[]
    valuelist=[]
    rplist = list(map(addwafer,[wafer]*len(a),a))
    for i in range(len(rplist)):
        rp = bytes(rplist[i],encoding='utf-8')
        table = connection.table('mes_bigtable')
        data11 = table.scan(row_prefix=rp,columns=[
                'cf:HEAD_SN',
                'cf:[DP].SSEB_EW(nm)-R1'
                ])
        for key,value in data11:
            keylist.append(key)
            valuelist.append(value)
    result = pd.DataFrame()   
    if(len(valuelist)>0):    
        df=pd.DataFrame(valuelist)  
        result = df.applymap(lambda x: x.decode() if isinstance(x, bytes) else x)
        result.columns = result.columns.astype(str)
        result.columns = [(name1).replace("cf:[DP].", "") for name1 in result.columns]
        result.columns = [(name1).replace("cf:", "") for name1 in result.columns]
        return(result)
    else:
        result=pd.DataFrame()
    return(result)
    
def main(input):
    wafer_no=list(input['wafer_no'])[0]
    wafer=wafer_no[1:6]
    try:
        connection = happybase.Connection(host='DN2MESHD04.sae.com.hk', port=9090)
        print(connection.tables())
        connection.open()
        #---------------wafer_shipment_data-------------
        wafShipd = mes_bigtable(connection,wafer)
    finally:
        if connection:
            connection.close()
    return(wafShipd)
    
    
    
    
import os
os.chdir("D:/data_Engineer/walter/Paris_C_1P_OSR_AI")

inputlist=pd.read_csv("Paris_C_1P_OSR_AI_wafer_list.csv")
os.chdir("D:/data_Engineer/walter/Paris_C_TL_PCM_and_OSR_AI/DP/")
# for i in range(len(inputlist)):
i=1
input = inputlist.iloc[[i]]
wafer_no=list(input['wafer_no'])[0]
waferdata = main(input)
    # waferdata.to_csv('20210728/Paris_C_TL_OSR_AI_DP_'+wafer_no+'_20210727.csv',index=False)

