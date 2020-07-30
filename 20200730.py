# 1.今日收盘
# 2.找到今日涨停股票
# 3.找到该股票相同板块所有股票的代码
# 4.将同板块所有股票加入同一个dataframe
# 5.设置时间进行spearman检验
# 6.相关性排序
#
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from jqdatasdk import *
import statsmodels.api as sm
from statsmodels.tsa.stattools import grangercausalitytests
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import coint
import tushare as ts
import seaborn as sns

Stocks =['600072','300527']
df1 = ts.get_k_data('600685',start='2020-03-01',end='2020-03-31')
df1 = df1[['date', 'close']]
for stock in Stocks:
    df2 = ts.get_k_data(stock,start='2020-03-01',end='2020-03-31')
    df1[stock] = df2['close']
    df1 = df1.rename(columns={'close': stock})
df1.to_csv('123.csv')
df = pd.read_csv('123.csv',index_col='date',parse_dates=['date'])
# df.loc[ : , ~df.columns.str.contains('Unnamed')]
# df = df.drop(columns=0,axis=1)
print(df)
print(df.corr('spearman'))
sns.heatmap(df.corr('spearman'))
plt.show()

# df_1.to_csv('600072.csv')
# df_2.to_csv('600685.csv')
# df_1 = pd.read_csv('600072.csv',index_col=0,parse_dates=True,header=None,names=['1'])['close']
# df_2 = pd.read_csv('600685.csv',index_col=0,parse_dates=True)
# print(df_1.index)
# print(df_1)
# print(df_2)