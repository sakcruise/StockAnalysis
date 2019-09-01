from pandas_datareader import data
import matplotlib.pyplot as plt
plt.style.use('seaborn-whitegrid')

import pandas as pd
import sys
import os
import datetime as date
sys.path.append("..")
print(sys.path)
from src import connect_db as db

class GetStockdata():
    def __init__(self):
        self.conn = db.connect_db()


    def get_stock_price(self):
        sql = """select x.date, x.close as abb from 
                    stocks_price x where x.symbol = 'abb'
                    and current_date - x.date <= 260
                    """
        stockprice_df = pd.read_sql(sql, con=self.conn)
        # print(stockprice_df)

        plt.scatter(date, [600.01, 112.78], marker='o')

        stockprice_df.plot(x='date', y='abb')
        # ax.scatter(xlist, ylist, zlist, color=color)
        # plt.plot(0, 0+1)
        plt.show()


def main():
    s = GetStockdata()
    stocklist = s.get_stock_price()

if __name__ == "__main__":
    main()

