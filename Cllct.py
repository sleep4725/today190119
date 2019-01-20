from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.parse import urlparse
import requests as req
from yaml import load
import p01
class Cllct:
    def __init__(self):
        self.elastic = p01.Elastic
        self.url = None
        self.path = None
        self.bsObj = None
        self.element = []

    # elasticsearch (1)  srv connect
    def ElasticSrvConnect(self):
        self.elastic.ElasticSrvConnect()

    # elasticsearch (2)  healthCheck
    def ElasticsHealthCheck(self):
        self.elastic.ElasticsHealthCheck()

    # elasticsearch (3) Data insert
    def ElasticsInsertDocument(self):
        self.elastic.InsertDocument(x=self.element)

    # Instance method (1)
    def urlSetting(self):
        with open("./CONFIG/info.yaml", "r") as f:
            txt = load(f.read())
            self.url = txt["url"]
            self.path = txt["path"]
            f.close()

    # Instance method (2)
    def requestURL(self):
        html = req.get(url=self.url+self.path)
        if html.status_code == 200:
            self.bsObj = BeautifulSoup(html.text, "html.parser")
            mvLst = self.bsObj.find_all("div", {"class":"tit3"})
            for indx, vale in enumerate(mvLst):
                insertData = {"name":None, "numb":None, "showtime":None, "showday":None, "nation":None}
                showt, showd, nation = self.SubInfo(vale.a.attrs["href"]) # Function call
                insertData["name"] = vale.a.attrs["title"]
                insertData["numb"] = indx+1
                insertData["showtime"] = showt
                insertData["showday"] = showd
                insertData["nation"] = nation
                Result = "영화 이름 : {n}, 영화 순위 : {o}, 영화 상영시간 : {t}, 영화 상영날짜 : {d}, 제작 국가 : {s}".\
                    format(n = insertData["name"], o = insertData["numb"], t = insertData["showtime"],
                           d = insertData["showday"], s = insertData["nation"])
                print (Result)
                self.element.append(insertData)

    def SubInfo(self, subpath):
        nation = None   # 제작국가
        showtime = None # 상영시간
        showday = None  # 상영날짜
        html = req.get(self.url + subpath)
        if html.status_code == 200:
            bsObject = BeautifulSoup(html.text, "html.parser")
            mvInfo = bsObject.select_one("div.mv_info > dl.info_spec > dd > p")
            try:
                # 국가
                nation = mvInfo.select_one("span:nth-of-type(2) > a").string
            except:
                return showtime, showday, nation
            else:
                try:
                    # 상영시간
                    showtime = mvInfo.select_one("span:nth-of-type(3)").string
                except:
                    return showtime, showday, nation
                else:
                    try:
                        # 상영날짜
                        showday = mvInfo.select_one("span:nth-of-type(4) > a:nth-of-type(2)").attrs["href"]
                    except:
                        try:
                            showday = mvInfo.select_one("span:nth-of-type(3) > a:nth-of-type(2)").attrs["href"]
                        except:
                            return showtime, showday, nation
                        else:
                            showday = urlparse(showday).query
                            showday = str(showday).split("=")[1]
                            # return 순서 : 상영시간, 상영날짜, 국가
                            return showtime, showday, nation
                    else:
                        showday = urlparse(showday).query
                        showday = str(showday).split("=")[1]
                        # return 순서 : 상영시간, 상영날짜, 국가
                        return showtime, showday, nation

def main():
    cnode = Cllct() # 객체 생성
    cnode.ElasticSrvConnect()
    # cnode.ElasticsHealthCheck()
    # ------------------------------
    cnode.urlSetting()
    cnode.requestURL()
    cnode.ElasticsInsertDocument()
if __name__ == "__main__":
    main()

