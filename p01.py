from elasticsearch import Elasticsearch
import elasticsearch.exceptions as elasticExcept
from datetime import datetime
import pprint as ppr
import sys
from urllib3.exceptions import ConnectTimeoutError
# =============================================
class Elastic:
    es = None

    @classmethod
    def ElasticSrvConnect(cls):
        try:
            cls.es = Elasticsearch(hosts="192.168.240.129", port=9200)
        except ConnectTimeoutError as e:
            print (e)
            sys.exit(1)
        else:
            print ("connect success !!!")

    @classmethod
    def ElasticsHealthCheck(cls):
        try:
            health = cls.es.cluster.health()["status"]
        except elasticExcept.ConnectionError as e:
            print (e)
            sys.exit(1)
        else:
            if health == "red":
                print ("health check : {}".format(health))
                sys.exit(1) # program 종료
            elif health == "yellow":
                print ("health check : {}".format(health))
                cls.CreateIndex()
            elif health == "green":
                print ("health check : {}".format(health))
                cls.CreateIndex()

    # 인덱스 생성
    @classmethod
    def CreateIndex(cls):
        requests_body = {
            "settings" : {
                "number_of_shards":5
            },
            "mappings": {
                "doc": {
                    "properties": {
                        "name": {"type": "text"},
                        "numb": {"type": "integer"},
                        "showtime": {"type": "text"},
                        "showday": {"type": "text"},
                        "nation": {"type": "text"}
                    }
                }
            }
        }
        res = cls.es.indices.create(index="navermovie", body=requests_body)
        print ("response : %s"%(res))

    # 인덱스 조회
    @classmethod
    def DocumentRead(cls, param="navermovie"):
        cls.es.indices.refresh(index=param)
        res = cls.es.search(
            index=param,
            body= {
                "query":{
                    # 특정 필드만
                    # "match":{"name":"이티"}
                    "match_all":{}
                }
            }
        )
        ppr.pprint (res['hits'])

    # 다큐먼트 (Document) 삽입
    @classmethod
    def InsertDocument(cls, param="navermovie", x=None):
        # request_body = {"name":"이티", "numb":3}
        for i in x:
            cls.es.index(index=param,doc_type="doc", body=i)
            print ("success")

# def main():
#     Elastic.ElasticSrvConnect()
#     Elastic.ElasticsHealthCheck()
#     # Elastic.CreateIndex()   - 인덱스 생성
#     Elastic.DocumentRead()
#     # Elastic.InsertDocument()
# if __name__ == "__main__":
#     main()