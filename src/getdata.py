from pandas_datareader import data
# import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import datetime as date
sys.path.append("..")
print(sys.path)



class GetStockdata():
    def __init__(self):
        self.start_date = '2014-01-01'
        self.end_date = date.datetime.now().strftime("%Y-%m-%d")
        self.output_path = sys.path[1] + "\data\output\stocks\\"

    def get_stock_list(self):
        stocklist_df = pd.read_csv(sys.path[1] + "\data\input\stock500list.csv")
        stocklist =  stocklist_df['Symbol'].tolist()
        print(stocklist)
        return stocklist

    def download_stock_data(self, _stocklist):
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        for symbol in _stocklist:
            panel_data = data.DataReader(symbol+'.NS', 'yahoo', self.start_date, self.end_date)
            panel_data.to_csv(self.output_path + symbol.lower() + "_" + self.start_date.replace('-','') + "_" + self.end_date.replace('-','') + ".csv")


def main():
    s = GetStockdata()
    stocklist = s.get_stock_list()
    s.download_stock_data(stocklist)

if __name__ == "__main__":
    main()
    # We would like all available data from 01/01/2000 until 12/31/2016.

