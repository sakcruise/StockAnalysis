from pandas_datareader import data
# import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import datetime as date
sys.path.append("..")
print(sys.path)
from src import connect_db as db

class GetStockdata():
    def __init__(self):
        self.start_date = '2014-01-01'
        self.end_date = date.datetime.now().strftime("%Y-%m-%d")
        self.output_path = sys.path[0] + "\data\output\stocks\\"

        self.conn = db.connect_db()

    def get_stock_list(self):
        # stocklist_df = pd.read_csv(sys.path[1] + "\data\input\stock500list.csv")
        stocklist_df = pd.read_sql("select symbol from stocks".format(), con=self.conn)
        print(stocklist_df)
        stocklist =  stocklist_df['symbol'].tolist()
        return stocklist

    def download_stock_data(self, _stocklist):
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        for symbol in _stocklist:
            panel_data = data.DataReader(symbol+'.NS', 'yahoo', self.start_date, self.end_date)
            panel_data.reset_index(inplace=True)

            # panel_data.to_csv(self.output_path + symbol.lower() + "_" + self.start_date.replace('-','') + "_" + self.end_date.replace('-','') + ".csv")
            panel_data['symbol'] = symbol.lower()
            cols = ['symbol','Date', 'High', 'Low', 'Open', 'Close', 'Volume', 'Adj Close']
            panel_data_ordered = panel_data[cols]
            panel_data_ordered.columns = map(str.lower, panel_data_ordered.columns)
            panel_data_ordered_renamed = panel_data_ordered.rename(columns={'adj close': 'close_adj'})
            print(symbol)
            panel_data_ordered_renamed.to_sql(name="stocks_price", con=self.conn, index=False, if_exists='append')

def main():
    s = GetStockdata()
    stocklist = s.get_stock_list()
    s.download_stock_data(stocklist)

if __name__ == "__main__":
    main()

