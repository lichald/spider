__author__ = 'Lining'
# -*- coding: utf8 -*-
import jieba,os,pyodbc
import pandas as pd
from pandas import DataFrame,Series

def wordcut_fun(wordcut,excelsum,exceldetail):
    result=[]
    #将原始数据导入进来，形成list
    wordcuts=wordcut.split("\n")
    #进行分词操作
    for i in wordcuts:
        try:
            seg_list = jieba.cut(i) 
            for j in seg_list:
                if len(j)>=2:
                    result.append(j.lower())
        except: print("some wrong")

    dic_result={}
    #进行计数
    for i in result:
        if i in dic_result:
            dd=dic_result.get(i)
            dic_result[i]=dd+1
        else:
            dic_result[i]=1
    dic_result=sorted(dic_result.items(),key=lambda asd:asd[1],reverse=True)
    dic_data=DataFrame(dic_result,columns=['keyword','frequency'])
    dic_data['tag']=''
    #将这个表根据我自己做的东东来打标签
    source=pd.read_sql_query(r'select fenci as keyword,tag_keyword as tag from [zln_data].[dbo].[lagou_fenci_jd]',con=sqlconn)
    for i in list(range(len(dic_data['keyword']))):
        for t in list(range(len(source['keyword']))):
            if source.ix[t,'keyword'].lower() in dic_data.ix[i,'keyword']:
                dic_data.ix[i,'tag']=source.ix[t,'tag']
    dic_data=dic_data[dic_data['tag']!='']
    sum_data=dic_data['frequency'].groupby(dic_data['tag']).sum()
    
    #写入excel中
    DataFrame(dic_data).to_excel(exceldetail+'.xls',sheet_name='detail')
    DataFrame(sum_data).to_excel(excelsum+'.xls',sheet_name='sum')

if __name__== '__main__':
    sql=input('请输入sql查询语句（输入之后请回车）：')
    excel1=input('请输入--详细列表--的文件名(不含后缀名)：')
    excel2=input('请输入--汇总列表--的文件名（不含后缀名）：')
    #***部分为数据库的地址、账号和密码
    sqlconn=pyodbc.connect("DRIVER={SQL SERVER};SERVER=***\\sql;DATABASE=zln_data;UID=***;PWD=***")#从数据库中提取最大最小值
    sqlcursor=sqlconn.cursor()
    jd=pd.read_sql_query(sql,con=sqlconn)
    t=wordcut_fun(wordcut=r'\n'.join(jd['jd']),exceldetail=excel1,excelsum=excel2)

