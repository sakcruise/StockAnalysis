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

    def get_stock_list(self):
        # stocklist_df = pd.read_csv(sys.path[1] + "\data\input\stock500list.csv")
        stocklist_df = pd.read_sql("select lower(symbol) as symbol from stocks".format(), con=self.conn)
        # print(stocklist_df)
        stocklist =  stocklist_df['symbol'].tolist()
        return stocklist

    def calculate_stocks_profit(self, _stocklist):
        sql = """select x2.symbol, x2.date,
                    round(cast((case when x2.buy_rate != 0 then x2.buy_rate else x2.sell_rate end) as numeric), 2) as rate,
                    case when x2.buy_rate != 0 then 'buy' else 'sell' end as rate_type
                    from
                    (select *,
                     lag(x1.buy_rate) OVER
                    (PARTITION BY X1.SYMBOL ORDER BY x1.date) as buy_lag,
                    lag(x1.sell_rate) OVER
                    (PARTITION BY X1.SYMBOL ORDER BY x1.date) as sell_lag
                     from (
                    select x.symbol,
                    x.date, x.open, x.high, x.low, x.hhv, x.llv, x.rownum,
                    (case when x.open >= hhv or high >= hhv then hhv else 0 end) as buy_rate,
                    (case when x.open <= llv or low<=llv then llv else 0 end) as sell_rate
                    from
                    (select s.symbol, s.date,s.open, s.high, s.low,
                    max(high)
                    over (partition by s.symbol order by s.date ROWS BETWEEN 250 PRECEDING AND 1 PRECEDING) as hhv,
                    min(low)
                    over (partition by s.symbol order by s.date ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) as llv,
                    row_number() over (partition by s.symbol order by s.date) as rownum
                    from stocks_price s
                    where s.symbol = '{0}'
                    order by s.symbol, s.date) x
                    where x.rownum >= 250) x1
                    where x1.buy_rate != 0 or x1.sell_rate !=0) x2
                    where (x2.buy_rate != 0 and (x2.buy_lag is null or x2.buy_lag = 0)) or
                     (x2.sell_rate != 0 and (x2.sell_lag is null or x2.sell_lag = 0))
                    """
        for symbol in _stocklist:
            print(symbol)
            stockprice_df = pd.read_sql(sql.format(symbol), con=self.conn)
            stocks_df = pd.DataFrame(columns=['symbol', 'buy_date', 'sell_date', 'buy_rate', 'sell_rate', 'profit'])
            i = 0
            buy_rate = False
            for index, row in stockprice_df.iterrows():
                stocks_df.at[i, 'symbol'] = symbol
                if row['rate_type'] == 'buy':
                    stocks_df.at[i, 'buy_date'] = row['date']
                    stocks_df.at[i, 'buy_rate'] = row['rate']
                    buy_rate = True

                if row['rate_type'] == 'sell' and buy_rate:
                    stocks_df.at[i, 'sell_date'] = row['date']
                    stocks_df.at[i, 'sell_rate'] = row['rate']
                    buy_rate = False
                    i+=1
            stocks_df['profit'] = stocks_df['sell_rate'] - stocks_df['buy_rate']
            # stocks_df.to_csv(self.output_path + symbol + '_' + 'profit' + '.csv', index=False)

            stocks_df.to_sql(name="stocks_profit", con=self.conn, index=False, if_exists='append')

    def download_stocks_profit(self):
        sql = """select extract(year from buy_date) as year,extract(month from buy_date) as month , * from stocks_profit
                 order by buy_date"""
        stockprice_extract_df = pd.read_sql(sql, con=self.conn)
        stockprice_extract_df.to_csv(self.output_path + 'stocks_profit' + '.csv', index=False)


def main():

    s = GetStockdata()
    stocklist_df = s.get_stock_list()
    s.calculate_stocks_profit(stocklist_df)
    s.download_stocks_profit()


if __name__ == "__main__":
    main()

