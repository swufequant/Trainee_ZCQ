import jqdata #导入数据库
#初始化函数，设定基准等等
def initialize(context):
    g.security = get_index_stocks('000300.XSHG')
    set_option('use_real_price',True)
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
def handle_data(context,data):  
    #print(get_current_data()['601318.XSHG'].day_open) 获取开盘价
    print(attribute_history('002400',30)) #获取历史数据函数
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from jqdatasdk import *
import statsmodels.api as sm
from statsmodels.tsa.stattools import grangercausalitytests
auth('13410041218','zcq13730878146A')
is_auth = is_auth()

Stocks = get_index_stocks('000300.XSHG')#股票池为沪深300成分股
df_x = get_price('002400.XSHE',start_date='2016-01-01 9:30:00',end_date='2016-12-31 15:00:00',frequency='1min',fq='pre')['close']#获取‘002400’分钟'close'价格的Series对象


panel = get_price(get_index_stocks('000300.XSHG'),start_date='2016-01-01 9:30:00',end_date='2016-12-31 15:00:00',frequency='1min',fq='pre',panel=Ture)
df_close = panel['close']#获取收盘价的[pandas.DataFrame],行索引为datatime对象,列索引为股票代号
print(df_close)
relevant_stocks = []
for stock in Stocks:
    df_y = df_close[stock]
    df_x[stock] = df_y[stock]
    grangercausalitytests(df[['002400.XSHE','stock']],maxlag=4)           #滞后阶数为4的格兰杰因果检验
    if p < 0.05:
        relevant_stocks.append(stock)                                     #格兰杰因果检验,如果p值满足条件则为相关股票

print(relevant_stocks)





#print(Stocks)
    # set_option('use_real_price',True)
    # set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    # print(g.security)
# def handle(context):

# ts.set_token('4e1d7b5ec065a702594a38f37c6e03d17549946b7b2f0e5191283926')
# pro = ts.pro_api()
# df = ts.pro_bar(ts_code="002400.SZ",start_date='2016-01-01',end_date='2016-12-31',freq='D')
# df.to_csv('002400.csv')
# df = pd.read_csv('002400.csv',index_col='trade_date',parse_dates=['trade_date'])[['open','close','high','low']]
# print(df)



# df = ts.get_k_data('002400',start='2016-01-01',end='2016-12-31')
# df.to_csv('002400000.csv')
# df = pd.read_csv('002400000.csv',index_col='date',parse_dates=['date'])[['open','close','high','low']]
# print(df)