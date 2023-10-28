from config import *
from pyspark.sql.functions import avg,lit,max
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType , FloatType

class Analysis():
    def __init__(self, connection):
        self.spark = connection        
        self.schema = StructType([
            StructField("Symbol", StringType(), True),
            #StructField("Name", StringType(), True),
            StructField("Avg Open Price", FloatType(), True),
            StructField("Avg Close Price", FloatType(), True),
            StructField("Max High Price", FloatType(), True),
            StructField("Max Low Price", FloatType(), True),
            StructField("Avg Volume", FloatType(), True)
            ])
    
    def fetchSymbol(self, sector):
        try: 
            df = self.spark.read.csv(csv_path, header=True, inferSchema=True) 
            filteredData = df.filter(df.Sector == sector)
            selected_data = filteredData.select("Symbol") 
            companyName = selected_data.collect() 
            finalMetaValue = [row['Symbol'] for row in companyName]
            return finalMetaValue
        except Exception as e:
            print("There is an error in fetching company name - " + str(e))
            
    def processData(self, symbols, startDate, endDate):
         
        fin =[]
        for i in symbols:
            fileName = str(i) + '.csv'
            print("Started processing the file - " + fileName)
            dataFile  = dataFolder + '\\' + fileName
            df = self.spark.read.csv(dataFile, header=True, inferSchema=True) 
            startDateCondition = df[timestamp] > startDate
            endDateCondition = df[timestamp] < endDate            
            filteredPrice = df.filter(startDateCondition & endDateCondition)     
            averageForOpenPrice = filteredPrice.agg(avg(openPrice).alias("openAverage")).collect()[0]['openAverage']
            averageForClosedPrice = filteredPrice.agg(avg(closedPrice).alias("closeAverage")).collect()[0]['closeAverage']
            maxHighPrice = filteredPrice.agg(max(highPrice).alias("highPrice")).collect()[0]['highPrice']
            maxLowPrice = filteredPrice.agg(max(lowPrice).alias("lowPrice")).collect()[0]['lowPrice']
            averageVolume = filteredPrice.agg(avg(volume).alias("averageVolume")).collect()[0]['averageVolume']
            fieldsToBeInserted = (str(i),averageForOpenPrice,averageForClosedPrice,maxHighPrice,maxLowPrice,averageVolume)
            try:
                print("Started appending the result for " + fileName)                
                li = []
                # for column_name, value in zip(finalDf.columns, fieldsToBeInserted):
                    #finalDf = finalDf.withColumn(column_name, lit(value))
                for value in fieldsToBeInserted:
                    li.append(value)
                fin.append(li)
            except Exception as e:
                print("There is an exception - " + str(e))
                break
        finalDf = self.spark.createDataFrame(fin, schema=self.schema) 
        print("The final result ready for showing...")
        finalDf.show()        
        # finalDf.printSchema()