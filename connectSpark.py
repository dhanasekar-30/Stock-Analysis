from pyspark.sql import SparkSession

class sparkSession():
    def connectSpark(self):        
        try:
            print("Started establishing spark session")
            # spark = SparkSession.builder.appName("StockAnalysis").getOrCreate()
            spark = SparkSession.builder.appName("StockAnalysis").config("spark.driver.memory", "4g").getOrCreate()
            print("Spark session established")
        except Exception as exp:
            print('Issue is establishing spark session'+str(exp))
        return spark