import pandas as pd
from elasticsearch import Elasticsearch

def main():
    #user name of elasticsearch for end user
    es_username = '{{username}} '
    es_passwd = '{{password}}'
    
    es = Elasticsearch(hosts=[{'host':'dn2meshd01.sae.com.hk','port':19200},{'host':'dn2meshd02.sae.com.hk','port':19200},{'host':'dn2meshd03.sae.com.hk','port':19200},{'host':'dn2meshd04.sae.com.hk','port':19200},{'host':'dn2meshd06.sae.com.hk','port':19200},{'host':'dn2meshd07.sae.com.hk','port':19200},{'host':'dn2meshd08.sae.com.hk','port':19200},{'host':'dn2meshd09.sae.com.hk','port':19200}],http_auth=(es_username, es_passwd),timeout=60, max_retries=10, retry_on_timeout=True)
    doc ={
      "size":30,
      "query": {
        "bool": {
          "filter": {
            "bool": {
              "must": [
                {
                  "term": {
                    "WAFER": "9AH36"
                  }
                },
                {
                  "bool": {
                    "must_not": {
                      "term": {
                        "[DP].GRADE": "ERROR"
                      }
                    }
                  }
                }
              ]
            }
          }
        }
      },
     "_source": ["[DP].EWAC(nm)","[DP].BER(dec)","[DP].TESTER"]
    }
    res = es.search(index="bigtable", body=doc,request_timeout=60,scroll = '3m',size =2000)
    
    #get the scroll ID from the response by accessing its _scroll_id key
    scroll_id = res['_scroll_id']
    
    results = res["hits"]["hits"]
    while (1):
        res = es.scroll(scroll_id=scroll_id, scroll='10s')
        results += res['hits']['hits']
        if len(res['hits']['hits'])==0:
            break
    list1=[]
    list2=[]
    list3=[]
    gp_num = len(results)
    for i in range(gp_num):
        if results[i]['_source'].get("[DP].EWAC(nm)",None)!=None:
            list1.append(float(results[i]['_source']["[DP].EWAC(nm)"]))
        else:
            list1.append(None)
        if results[i]['_source'].get("[DP].BER(dec)",None)!=None:
            list2.append(float(results[i]['_source']["[DP].BER(dec)"]))
        else:
            list2.append(None)
        if results[i]['_source'].get("[DP].TESTER",None)!=None:
            list3.append(results[i]['_source']["[DP].TESTER"])
        else:
            list3.append(None)
    
    output=pd.DataFrame({'[DP].EWAC(nm)': list1,'[DP].BER(dec)': list2,'[DP].TESTER': list3})
    print(output)
if __name__ == '__main__':
  main()