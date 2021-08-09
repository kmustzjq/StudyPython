
import pandas as pd
from elasticsearch import Elasticsearch


import os




def main(input):
    if (input.shape[0] >= 1):
        try:
            for i in range(0,len(input['Wafer'])):
                es_username = 'pe'
                es_passwd = '!*1Zx9'
                WAFER = input['Wafer'][i]
                # WAFER = '9A90C'
                doc = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {
                                    "WAFER": WAFER
                                }}
                            ]
    
                        }
                    },
                    "_source": [
                                'HEAD_SN',
                                'PROJECT_CODE',
                                'BLOCK',
                                'ROW',
                                'SLIDER_NO',
                                '[QST].DEFECT_MAP',
                                '[QST].RH_MAP',
                                '[LAPPING.SLIDER].CLO1A_WRLGHA_AFT',
                                '[LAPPING.SLIDER].INC_PCM',
                                '[LAPPING.SLIDER].RBDP_CLO1A_MWW',
                                '[QST].R2_MRR',
                                '[QST].MRR',
                                '[DP].RESULT',
                                '[LAPPING.SLIDER].CLO1A_WRLGH_FLAG'
                                '[LAPPING.SLIDER].FINAL_TARGET_SLIDER_HW_TARGET_AFT_LOCK',
                                '[LAPPING.SLIDER].FINAL_TARGET_SLIDER_HW_TARGET_BEF_LOCK',
                                '[LAPPING.SLIDER].FINAL_TARGET_DELTA_HW_WIR_AFT_LOCK',
                                '[LAPPING.SLIDER].FINAL_TARGET_DELTA_HW_WIR_BEF_LOCK',
#                                '[LAPPING.SLIDER].DELTA_HR_WIR_LOCK_VOL',
#                                '[LAPPING.SLIDER].FINAL_DIRECT_APPEND_DH38_WIR_HW_MES',
                                '[DP].SSEB_EWI(nm)-R2'],
                    "size": 100000
                }
    
                es = Elasticsearch(
                    hosts=[{'host': 'dn2meshd01.sae.com.hk', 'port': 19200},
                           {'host': 'dn2meshd02.sae.com.hk', 'port': 19200},
                           {'host': 'dn2meshd04.sae.com.hk', 'port': 19200},
                           {'host': 'dn2meshd06.sae.com.hk', 'port': 19200},
                           {'host': 'dn2meshd07.sae.com.hk', 'port': 19200},
                           {'host': 'dn2meshd08.sae.com.hk', 'port': 19200},
                           {'host': 'dn2meshd09.sae.com.hk', 'port': 19200}], http_auth=(es_username, es_passwd),
                    timeout=60,
                    max_retries=10, retry_on_timeout=True)
                res = es.search(index="bigtable", body=doc, request_timeout=60)
    
                res_bucket = res['hits']['hits']
                gp_num = len(res_bucket)
    
                list1 = []
    
                if gp_num != 0:
                    for j in range(gp_num):
                        list1.append(res_bucket[j]['_source'])
                    output = pd.DataFrame(list1)
                    output1 = output[[
                            'HEAD_SN',
                            'PROJECT_CODE',
                            'BLOCK',
                            'ROW',
                            'SLIDER_NO',
                            '[QST].DEFECT_MAP',
                            '[QST].RH_MAP',
                            '[LAPPING.SLIDER].CLO1A_WRLGHA_AFT',
                            '[LAPPING.SLIDER].INC_PCM',
                            '[LAPPING.SLIDER].RBDP_CLO1A_MWW',
                            '[QST].R2_MRR',
                            '[QST].MRR',
                            '[DP].RESULT',
                            '[LAPPING.SLIDER].CLO1A_WRLGH_FLAG',
                            '[LAPPING.SLIDER].FINAL_TARGET_SLIDER_HW_TARGET_AFT_LOCK',
                            '[LAPPING.SLIDER].FINAL_TARGET_SLIDER_HW_TARGET_BEF_LOCK',
                            '[LAPPING.SLIDER].FINAL_TARGET_DELTA_HW_WIR_AFT_LOCK',
                            '[LAPPING.SLIDER].FINAL_TARGET_DELTA_HW_WIR_BEF_LOCK',
#                            '[LAPPING.SLIDER].DELTA_HR_WIR_LOCK_VOL',
#                            '[LAPPING.SLIDER].FINAL_DIRECT_APPEND_DH38_WIR_HW_MES',
                            '[DP].SSEB_EWI(nm)-R2']]

                else:
                    output1 = pd.DataFrame()
            return output1
        finally:
            print 'finish'
           
     
if __name__ == '__main__':
    input = pd.DataFrame([{'Wafer':'0A942' , 'project_code':'BTSAD0'}])
    output = main(input)
    
    os.chdir('D:/data_Engineer/walter/MG08_ML_AI/ROW_SLIDER_STAMP_AI/analysis/H0A942_3_BLOCK')
    output.to_csv('20210413/H0A942_3_BLOCK_20210413.csv',index = False)