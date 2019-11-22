# coding:utf8
# import datetime
import time
import re
import urllib
from bs4 import BeautifulSoup
# from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

def huabaoWPFund():
    data_dict ={}
    url = "http://www.fsfund.com/funds/162411/index.shtml"
    html = urllib.request.urlopen(url).read()
    sp = BeautifulSoup(html, 'html.parser', from_encoding="GB18030")
    data = sp.select(".fundid_jz_table")
    reget = re.compile('[0-9.-]+')
    result = reget.findall(data[0].text)
    data_dict['date'] = result[0]
    data_dict['corrent_data'] = result[1]
    data_dict['corrent_change'] = result[3]
    return data_dict
def spsiop_index():
    data_dict = {}
    url2 = "https://www.marketwatch.com/investing/index/spsiop?countrycode=xx"
    html2 = urllib.request.urlopen(url2).read()
    sp2 = BeautifulSoup(html2, 'html.parser', from_encoding="utf-8")
    price = sp2.find("meta", {"name": "price"})['content']
    data_dict['price']=float(price.replace(',',''))
    priceChange = sp2.find("meta", {"name": "priceChange"})['content']
    data_dict['priceChange'] = float(priceChange.replace(',', ''))
    priceChangePercent = sp2.find("meta", {"name": "priceChangePercent"})['content']
    quoteTime = sp2.find("meta", {"name": "quoteTime"})['content']
    data_dict['date'] = quoteTime
    data_dict['priceChange'] = priceChange
    data_dict['priceChangePercent'] = priceChangePercent
    data_dict['price'] = price
    print(price)
    print(priceChange)
    print(priceChangePercent)
    print(quoteTime)
    return data_dict
def dollars_rate():
    url3 =  "http://www.boc.cn/sourcedb/whpj/"
    html3 = urllib.request.urlopen(url3).read()
    url4 = "https://finance.sina.com.cn/money/forex/hq/USDCNH.shtml"
    html4 = urllib.request.urlopen(url4).read()
    sp3 = BeautifulSoup(html3, 'html.parser', from_encoding="utf-8")
    sp4 = BeautifulSoup(html4, 'html.parser', from_encoding="utf-8")
    dollar = sp3.find_all("tr")[27].text
    dollar2 = sp3.find_all("昨收")

    print(dollar)
    print('test')


if __name__ == '__main__':
    # main()
    data=huabaoWPFund()
    data2 = spsiop_index()
    dollars_rate()
    print()

    # def job():
    #     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #     date, corrent_data, corrent_change = huabaoWPFund()
    #     spsiop_index()
    #     dollars_rate()
    #
    #
    # # BlockingScheduler
    # scheduler = BlockingScheduler()
    # scheduler.add_job(job, 'cron', day_of_week='1-5', hour=8, minute=38)
    # scheduler.start()
    #