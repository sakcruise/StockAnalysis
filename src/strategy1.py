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
        self.output_path = sys.path[1] + "\data\output\profits\\"


    def get_stock_price(self):
        sql = """select x2.symbol, x2.date,
                    round(cast((case when x2.buy_rate != 0 then x2.buy_rate else x2.sell_rate end) as numeric), 2) as rate,
                    case when x2.buy_rate != 0 then 'buy' else 'sell' end as rate_type
                    from
                    (select *,
                     lag(x1.buy_rate) OVER
                    (ORDER BY x1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as buy_lag,
                    lag(x1.sell_rate) OVER
                    (ORDER BY x1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sell_lag
                     from (
                    select x.symbol,
                    x.date, x.open, x.high, x.low, x.hhv, x.llv, x.rownum,
                    (case when x.open >= hhv or high >= hhv then hhv else 0 end) as buy_rate,
                    (case when x.open <= llv or low<=llv then llv else 0 end) as sell_rate
                    from
                    (select s.symbol, s.date,s.open, s.high, s.low,
                    max(high)
                    over (partition by s.symbol order by s.date ROWS BETWEEN 250 PRECEDING AND CURRENT ROW) as hhv,
                    min(low)
                    over (partition by s.symbol order by s.date ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) as llv,
                    row_number() over (partition by s.symbol order by s.date) as rownum
                    from stocks_price s
                    where s.symbol = 'abb'
                    order by s.date) x
                    where x.rownum >= 250) x1
                    where x1.buy_rate != 0 or x1.sell_rate !=0) x2
                    where (x2.buy_rate != 0 and (x2.buy_lag is null or x2.buy_lag = 0)) or
                     (x2.sell_rate != 0 and (x2.sell_lag is null or x2.sell_lag = 0))
                    """
        stockprice_df = pd.read_sql(sql, con=self.conn)
        symbol = stockprice_df['symbol'].values[0]
        print(stockprice_df)

        stocks_df = pd.DataFrame(columns=['symbol', 'buy_date', 'sell_date', 'buy_rate', 'sell_rate', 'profit'])
        i = 0
        for index, row in stockprice_df.iterrows():
            stocks_df.at[i, 'symbol'] = symbol
            if row['rate_type'] == 'buy':
                stocks_df.at[i, 'buy_date'] = row['date']
                stocks_df.at[i, 'buy_rate'] = row['rate']

            if row['rate_type'] == 'sell':
                stocks_df.at[i, 'sell_date'] = row['date']
                stocks_df.at[i, 'sell_rate'] = row['rate']
                i+=1
        stocks_df['profit'] = stocks_df['buy_rate'] - stocks_df['sell_rate']

        print(stocks_df.to_csv(self.output_path + symbol + '_' + 'profit' + '.csv', index=False))
        print(stocks_df['profit'].sum())
        # plt.scatter(date, [600.01, 112.78], marker='o')
        #
        # stockprice_df.plot(x='date', y='abb')
        # # ax.scatter(xlist, ylist, zlist, color=color)
        # # plt.plot(0, 0+1)
        # plt.show()


def main():
    s = GetStockdata()
    stocklist = s.get_stock_price()

if __name__ == "__main__":
    main()

