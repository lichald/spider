__author__ = 'Lining'
# -*- coding:utf-8 -*-
import requests,json,re,time,datetime,socket,pyodbc
from urllib import request
import pandas as pd
from pandas import DataFrame,Series
from bs4 import BeautifulSoup

def lagou_spider_keyword(keyword):
    #将搜索字符串转换为utf-8编码，之后进行lagou.com搜索url构造
    keywordbyte=keyword.encode('utf-8')
    keywordindex=str(keywordbyte).replace(r'\x','%').replace(r"'","")
    keywordindex=re.sub('^b','',keywordindex)

    #计算总共有多少搜索结果页
    i =0
    type='true'
    url='http://www.lagou.com/jobs/positionAjax.json?px=default&first='+type+'&kd='+keywordindex+'&pn='+str(i+1)
    with request.urlopen(url) as f:
        data=f.read()
        urlcount=int(json.loads(str(data,encoding='utf-8',errors='ignore'))["content"]["totalPageCount"])
        print('本次搜索页面共计%d'%urlcount)

    #开始正式抓取
    for i in list(range(0,urlcount)):

        #构造页面
        if i ==0 :
            type='true'
        else:
            type='false'
        url='http://www.lagou.com/jobs/positionAjax.json?px=default&first='+type+'&kd='+keywordindex+'&pn='+str(i+1)
        with request.urlopen(url) as f:
            data=f.read()

        #读取json数据，开始解析
        try:
            jsondata=json.loads(str(data,encoding='utf-8',errors='ignore'))["content"]['result']

            for t in list(range(len(jsondata))):
                #把company描述的列表合并为一个字符串
                jsondata[t]['companyLabelList2']='-'.join(jsondata[t]['companyLabelList'])
                jsondata[t].pop('companyLabelList')

                #将每一行数据做成Series，之后再合并
                if t == 0:
                    rdata=DataFrame(Series(data=jsondata[t])).T
                else:
                    rdata=pd.concat([rdata,DataFrame(Series(data=jsondata[t])).T])
            #重新给rdata编码
            rdata.index=range(1,len(rdata)+1)
            rdata['keyword']=keyword
            rdata['salarymin']=0
            rdata['salarymax']=0
            rdata['url']=''
            rdata['jd']=''#职位描述
            rdata['handle_perc']=''#简历及时处理率，在七天内处理完简历占所有简历的比例
            rdata['handle_day']=''#完成简历处理平均天数
            for klen in list(range(len(rdata['salary']))):
                rdata.ix[klen+1,'salarymin'] = re.search('^(\d*?)k',rdata['salary'].iloc[klen]).group(1)
                #如果工资的最大值没有写，如（8k以上），则列为空值
                if re.search('-(\d*?)k$',rdata['salary'].iloc[klen]) != None:
                    rdata.ix[klen+1,'salarymax'] = re.search('-(\d*?)k$',rdata['salary'].iloc[klen]).group(1)
                else:
                    rdata.ix[klen+1,'salarymax'] = ''
                #增加url一列，便于后续抓取jd内容
                rdata.ix[klen+1,'url'] =  'http://www.lagou.com/jobs/%s.html'% rdata.ix[klen+1,'positionId']

                #对url进行二次抓取，把jd抓进来
                with request.urlopen(rdata.ix[klen+1,'url']) as f:
                    data_url=f.read()
                    soup_url=BeautifulSoup(data_url,'html5lib')
                    strings_url=soup_url.find('dd',class_='job_bt').strings
                    rdata.ix[klen+1,'jd']=''.join(strings_url).encode('gbk','ignore').decode('gbk','ignore').replace(' ','')
                    temp=soup_url.find_all('span',class_='data')
                    if re.search('>(\w*%)<',str(temp[0])) == None:
                        rdata.ix[klen+1,'handle_perc']=''
                    else:
                        rdata.ix[klen+1,'handle_perc']=re.search('>(\w*%)<',str(temp[0])).group(1)
                    rdata.ix[klen+1,'handle_day']=re.search('>(\w*)<',str(temp[1])).group(1).replace('天','')

        except Exception:
            print(Exception)
            continue
        #构造totaldata，是所有页面的集合，rdata是这一个页面的集合
        if i == 0:
             totaldata=rdata
        else:
             totaldata=pd.concat([totaldata,rdata])

        totaldata.index=range(1,len(totaldata)+1)
        print('正在抓取搜索页面第%d页,时间是%s，还剩下%d页'%(i+1,datetime.datetime.now(),urlcount-i-1))


    #开始写入数据库
    totaldata.to_excel('lagou.xls',sheet_name='sheet1')


if __name__=='__main__':
    keyword = input("请输入搜索词(回车进入下一步): ")
    #keyword='数据挖掘' #可以随意定义搜索词
    lagou_spider_keyword(keyword)
