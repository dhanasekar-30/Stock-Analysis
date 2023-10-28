from config import csv_path
from connectSpark import *
from stockAnalysis import *
import argparse
from datetime import datetime

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-st",
                        "--start",
                        help="Metion the start date",
                        type=lambda date: datetime.strptime(date, "%Y-%m-%d"),
                        default='1999-11-01')
    parser.add_argument("-ed",
                        "--end",
                        help="Metion the end date",
                        type=lambda date: datetime.strptime(date, "%Y-%m-%d"),
                        default='2022-03-15')
    parser.add_argument("-s",
                        "--sectors",
                        help="Metion the sectors Example - Technology/ Finance",
                        type=str,
                        default='TECHNOLOGY')
    app_args, unknown_args = parser.parse_known_args()
    startDate = app_args.start 
    endDate = app_args.end
    sector = str(app_args.sectors)
    spark = sparkSession()
    connection = spark.connectSpark()
    stocks = Analysis(connection)
    symbol = stocks.fetchSymbol(sector)
    stocks.processData(symbol,startDate,endDate)
    connection.stop()