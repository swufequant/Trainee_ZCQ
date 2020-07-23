import numpy as np
import matplotlib.pyplot as plt
from jqdatasdk import *
import statsmodels.api as sm
import tushare as ts
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