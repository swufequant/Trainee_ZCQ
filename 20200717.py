auth('13410041218','zcq13730878146A')
is_auth = is_auth()
df1 = get_price('600760.XSHG',start_date='2020-03-15',end_date='2020-07-15',fq='pre',frequency='60m')['close']
print(df1)
df2 = get_price('600765.XSHG',start_date='2020-03-15',end_date='2020-07-15',fq='pre',frequency='60m')['close']
print(df2)
relevant_stocks=[]
if adfuller(df1)[1] >= 0.05:
    df1_diff = np.diff(df1)
    print(adfuller(df1_diff))
    if adfuller(df2)[1] >= 0.05:
        df2_diff = np.diff(df2)
        print(adfuller(df2_diff))
        p = coint(df1,df2)[1]
        print(coint(df1,df2))
        if p <  0.2:
            print('OK')# relevant_stocks.append()
        else:
            print('都不平稳不相关')
    else:
        print(('notrelevant'))
else:
    print('ADDED')

    #获取每日涨停股票数据
    #查找涨停股票对应涨停次数
    #若第一次涨停则获取其板块信息，设置参数，排列相关性关系