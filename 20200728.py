import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from jqdatasdk import *
import statsmodels.api as sm
from statsmodels.tsa.stattools import grangercausalitytests
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import coint
auth('13410041218','zcq13730878146A')
is_auth = is_auth()

Stocks = get_index_stocks('000300.XSHG')                                                         #股票池为沪深300成分股
df_x = get_price('002400.XSHE',start_date='2016-01-01 9:30:00',end_date='2016-12-31 15:00:00',frequency='1m',fq='pre')['close']#获取‘002400’分钟'close'价格的Series对象
result1 = adfuller(df_x)[1]
print(result1)
if result1 >= 0.05:
    df_x_diff = np.diff(df_x)
    result2 = adfuller(df_x_diff)[1]
    print(result2)
panel = get_price(get_index_stocks('000300.XSHG'),start_date='2016-01-01 9:30:00',end_date='2016-12-31 15:00:00',frequency='1min',fq='pre',panel=Ture)
df_close = panel['close']#获取收盘价的[pandas.DataFrame],行索引为datatime对象,列索引为股票代号
print(df_close)
cointegration_stocks = pd.Series()
granger_stocks = pd.Series()
def calSim():
    symbols = ['600831', '603000', '603888', '300431', '002238', '600037']
    used_cols = ['code', 'close']
    df = ts.get_hists(symbols, start='2018-12-01', end='2018-12-30')
    df = df[used_cols]
    df_Close = pd.DataFrame()

    pos = [321, 322, 323, 324, 325, 326, 327, 328]
    i = 0
    fig = plt.figure()
    for symbol in symbols:
        ax = fig.add_subplot(pos[i])
        ax.plot(range(len(df_Close[symbol])), df_Close[symbol])
        ax.set_title(symbol)
        i += 1
    plt.tight_layout()  # change the distance of subplots
    plt.show()
for symbol in symbols:
        value = df.loc[df['code'] == symbol, 'close'].values
        df_Close[symbol] = value
    print(df_Close)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    sns.heatmap(df_Close.corr(), ax=ax, annot=True)
    plt.show()
for stock in Stocks :
    df_y = df_close[stock]
    result3 = adfuller(df_y)[1]
    print(result3)
    if result3 >= 0.05 :
        df_y_diff = np.diff(df_y)
        result4 = adfuller(df_y_diff)[1]
        if result4 < 0.05 and result2 <0.05 :
            p = coint(df_x, df_y)[1]
            if p < 0.05:
                index0 = str(stock)+'/'+'002400.XSHE'
                cointegration_stocks0 = pd.Series(p,index=[index0])
                cointegration_stocks = cointegration_stocks.append(coint_list)                 #协整检验
    if result3 < 0.05 and result1 < 0.05 :
        df_x[stock] = df_y[stock]                                                            #格兰杰因果检验
        grangercausalitytests(df[['002400.XSHE', 'stock']], maxlag=4)
        if p < 0.05 :
            index0 = str(stock) + '/' + '002400.XSHE'
            granger_stocks1 = pd.Series(p, index=[index0])
            granger_stocks = granger_stocks.append(granger_stocks1)
