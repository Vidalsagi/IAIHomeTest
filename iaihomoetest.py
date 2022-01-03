# Sagi Vidal 205657042

import pathlib

import yfinance as yf       #Yahoo finance library
import datetime             #Used for checking current stock values
import csv                  #For csv files
import pandas as pd         #Pandas dataframe stuff
import threading            #For ex.3
import time                 #For setting time ex.3
import os                   #For making a new folder ex.4
from pathlib import Path    #To get file path ex.4

tickers_main = []    #Save the current values of the symbols and the amount of sample
my_dict = {}         #Dictonary to save all the collected data investments

#This function will get the data from yfinance ex.3
def yfinancedata(tickersymbol,delay):
    tickerdata = yf.Ticker(tickersymbol)
    tickerinfo = tickerdata.info #prints all the info
    investment = tickerinfo['shortName'] #print only the name of the company
    today = datetime.datetime.today().isoformat()
    tickerDF = tickerdata.history(period='1d',start='2020-1-1',end=today[:10])
    priceLast = tickerDF['Close'].iloc[-1]
    my_dict.setdefault(investment, []).append(priceLast)
    #print('Investment = ' + investment)
    #print('Today = ' + today)
    #print(investment + 'price last = ' + str(priceLast))
    #print("investment is: " + str(investment))


#write all the stocks from dictonary to csv file
def to_csv(stocks):
    with open('below_mean.csv', 'w') as output:
        writer = csv.writer(output)
        writer.writerow(stocks[0].keys())
        for stock in stocks:
            writer.writerow(stock.value())

#getting tickers from csv file ex.2
def get_csv():
    tickers = []
    with open('readtickers.csv', newline='') as csvfile:
        tickers = list(csv.reader(csvfile))
        return tickers

#ex.3
def active_threads(tickers_main):
    for i in range(len(tickers_main)):
        myThread(tickers_main[i][1], tickers_main[i][0]).start()

class myThread (threading.Thread):
   def __init__(self, time, name):
      threading.Thread.__init__(self)
      self.time = time
      self.name = name
   def run(self):
       time.sleep(int(self.time))  # set sleep time
       yfinancedata(self.name, self.time)


def thread_sample(delay, name):
    yfinancedata(name,delay)  # print + add to dictonary the symbol value


#ex.4 save fastparquet file
def save_fastparquet():
    time.sleep(1)

    #set the name of directory
    today_day = datetime.datetime.today().date().day
    today_month = datetime.datetime.today().date().month
    today_year = datetime.datetime.today().date().year
    today_hour = datetime.datetime.today().time().hour
    today_min = datetime.datetime.today().time().minute
    today_sec = datetime.datetime.today().time().second
    directory = str(today_day) + str(today_month) + str(today_year) + "_" + str(today_hour) + "_" + str(today_min) + "_" + str(today_sec)
    path = str(pathlib.Path().resolve()) + '\\' + str(directory)

    # Create the directory
    os.mkdir(path)
    print("folder '% s' created" % directory)
    path_directory = os.path.join(Path(__file__).parent.parent, directory)
    print("Directory '% s' created" % path_directory)

    for key in my_dict.keys():
        df = pd.DataFrame.from_dict(my_dict, orient='index')
        df = df.transpose()
        df.to_parquet(directory + '\\' + str(key)+ '.parquet')
        df = pd.DataFrame.from_dict(my_dict, orient='index')

    #Check the dataframe
    print("The dataframe: ")
    print(df.to_string())

def main():
    try:
        tickers_main = get_csv()
        print('Available stocks/delay:' + str(tickers_main))
        while True:
            t_end = time.time() + 240  # the amount of seconds between each parquet folder create
            while time.time() < t_end:
                time.sleep(1)  # slow down the program for test every 1 second, without it no yahoo symbol test
                active_threads(tickers_main)
                t_end = t_end - 1

            # save results after 120 seconds
            threading.Timer(1.0, save_fastparquet).start()
    except KeyboardInterrupt:
        pass

main()

