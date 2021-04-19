import configparser
from time import time
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType as R, StructField as Fld, StringType as Str, DoubleType as Dbl, LongType as Long, TimestampType
from bs4 import BeautifulSoup
import requests
import pandas as pd
import glob

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")

def create_spark_session():
    """
    create Spark session or "grab" it if it already exists
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport()\
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.sparkContext.setLogLevel('WARN')
    
    return spark

def collect_buoy_filelist(url='https://www.ndbc.noaa.gov/data/historical/stdmet/', ext='.txt.gz', timeframe=range(2000,2021)):
    """
    create a buoy data filelist for the provided timeframe.
    """
    # get the list of data files
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    fileList = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]

    #create year filter
    yearFilter=timeframe
    yearFilter=['h' + str(s) for s in yearFilter]
    filteredFileList = [n for n in fileList if any(m in n for m in yearFilter)]
    print('There are {} files since year {}.'.format(len(filteredFileList), yearFilter[0]))
    return filteredFileList

def load_buoy_data(spark, fileList, tempHour=12): 
    """
    load relevant buoy data.
    """
    for idx,f in enumerate(fileList):
        print('Processing buoy file:', f)
        try:
            df = pd.read_csv(f, sep='\s+', comment='#', dtype={'hh': int})
            if len(df.columns) == 18:
                df.columns=["YYYY","MM","DD","hh","min","WDIR","WSPD","GST","WVHT","DPD","APD","MWD","PRES","ATMP",\
                            "WTMP","DEWP","VIS","TIDE"]
            else: # old data files don't include mm column, so adding it
                df.columns=["YYYY","MM","DD","hh","WDIR","WSPD","GST","WVHT","DPD","APD","MWD","PRES","ATMP","WTMP","DEWP",\
                            "VIS","TIDE"]
                df['min']=0
            # extract the buoyID from the file path then add it as a column to the Dataframe.
            buoyID=f.rsplit('h', 1)[0].split('/')[-1]
            df['buoyID'] = buoyID
            dfFiltered = df[df['hh'] == tempHour]  #only keeping 12 o'clock records.
            if (len(dfFiltered.index) > 0):
                dfTemp = spark.createDataFrame(dfFiltered)
                if idx == 0 :
                    dfBuoy=dfTemp
                else:
                    dfBuoy=dfBuoy.union(dfTemp)
        except pd.io.common.EmptyDataError:
            print ("File {} is empty".format(f))
            continue
        #if idx == 20:
        #    break
    dfBuoy.count()
    return dfBuoy

def load_swims_data(spark, pathToData):
    
    print(pathToData)
    tStart=time()
    df = spark.read.json(pathToData)
    tEnd = time() - tStart
    print('Read data in {} secs'.format(tEnd))
    df.printSchema()
    return df
    
def create_swimmers_table(dfSwimslog, outputData):
    """
    create and save swimmers_table.
    """
    # create swimmers_table using dfSwimslog data
    df2=dfSwimslog.dropDuplicates(['swimmer'])
    swimmers_table=df2.select(split(col("swimmer"),'\s+').getItem(1).alias("firstName"), \
                        split(col("swimmer"),'\s+').getItem(2).alias("lastName"), col("swimmerEmail").alias("Email") )\
                            .withColumn("swimmerIdx", monotonically_increasing_id())

    swimmers_table.printSchema()
    swimmers_table.show(5)

    # write swimmers_table
    fullPath = outputData + 'swimmers/swimmers_table.parquet'
    swimmers_table.write.mode('overwrite').parquet(fullPath)
    return swimmers_table

def create_locations_table(dfSwims, outputData):
    """
    create and save locations_table.
    """

    locations_table=dfSwims.select(col("Location"), split(col("Coordinates"), ',').getItem(0).alias("Longitude"), \
                                split(col("Coordinates"), ',').getItem(1).alias("Latitude"))\
                            .dropDuplicates(['Location']).withColumn("locationIdx", monotonically_increasing_id())
    locations_table.printSchema()
    locations_table.show(5)

    # write locations_table
    fullPath = outputData + 'locations/locations_table.parquet'
    locations_table.write.mode('overwrite').parquet(fullPath)
    return locations_table

def create_conditions_table(dfSwims, dfBuoy, outputData):
    """
    create and save conditions_table.
    """
    dfSwims.show(5)
    dfBuoy.show(5)
    
    conditions_table=dfSwims.join(dfBuoy, ((dfBuoy['buoyID'] == dfSwims['BuoyID']) & \
                                            (dfBuoy['YYYY'] == dfSwims['Year']) & \
                                           (dfBuoy['MM'] == dfSwims["Month"]) & \
                                            (dfBuoy['DD'] == dfSwims['Day']))) \
                                .select(dfBuoy.buoyID,
                                        dfBuoy.ATMP, 
                                        dfBuoy.WTMP,
                                        dfBuoy.TIDE,
                                        dfBuoy.WVHT,
                                        dfBuoy.WDIR,
                                        dfBuoy.WSPD,
                                        dfSwims.Year,
                                        dfSwims.Month,
                                        dfSwims.Day)\
                                .withColumn("conditionsIdx", monotonically_increasing_id())

    conditions_table.printSchema()
    conditions_table.show(5)

    # write conditions_table
    fullPath = outputData + 'conditions/conditions_table.parquet'
    conditions_table.write.partitionBy("Year", "Month", "BuoyID").mode('overwrite').parquet(fullPath)
    return conditions_table

def create_time_table(dfSwims, outputData):
    """
    create and save time_table.
    """
    time_table = dfSwims.select(col('Year').alias('YYYY'), col('Month').alias('MM'), \
                                col('Day').alias('DD'),col('Date')).dropDuplicates()
    time_table.printSchema()
    time_table.show(5)
                    
    # write time table to parquet files partitioned by year and month
    fullPath = outputData + 'time/time_table.parquet'
    time_table.write.partitionBy("YYYY", "MM").mode('overwrite').parquet(fullPath)
    return time_table

def create_swims_table(dfSwimslog, dfSwims, swimmers_table, time_table, conditions_table, locations_table, outputData):
    """
    create and save swims_table.
    """
    swims_table = dfSwimslog.join(dfSwims, (dfSwimslog.swimID == dfSwims.swimID))\
                            .join(swimmers_table, (dfSwimslog.swimmerEmail == swimmers_table.Email)) \
                            .join(time_table, (dfSwims.Date == time_table.Date)) \
                            .join(conditions_table, ((dfSwims.BuoyID == conditions_table.buoyID) & \
                                                    (conditions_table.Year == time_table.YYYY) & \
                                                    (conditions_table.Month == time_table.MM) & \
                                                    (conditions_table.Day == time_table.DD)), 'left_outer') \
                            .join(locations_table, (dfSwims.Location == locations_table.Location)) \
                            .select(swimmers_table.firstName,swimmers_table.lastName, \
                                    swimmers_table.swimmerIdx,locations_table.Location,locations_table.locationIdx,\
                                    conditions_table.conditionsIdx, time_table.YYYY, time_table.MM, time_table.Date, \
                                    dfSwimslog.swimStatus)\
                            .dropDuplicates()\
                            .withColumn("SwimslogIdx", monotonically_increasing_id())

    swims_table.printSchema()
    swims_table.show(5)

    # write swims_table
    fullPath = outputData + 'swims/swims_table.parquet'
    swims_table.write.partitionBy('YYYY','MM','locationIdx').mode('overwrite').parquet(fullPath)
    return swims_table

def test_min_numb_rows(area, df, target):
    print("Testing number of rows on:", area)
    try: 
        assert df.count() >= target
    except AssertionError: 
        print("Number of rows in {} is lower than expected".format(area))

def test_key_uniqueness(area, df, primaryKey):
    print("Testing key uniqueness on:", area)
    try:
        assert df.select(primaryKey).distinct().count() == df.count()
    except AssertionError:
        print("Multiple rows use the same primary key".format(area))

def main():
    """
    Main code block to initialize Spark session, set configuration and call reading functions.
    """
    spark = create_spark_session()
    
    location = config.get("DATA", "LOCATION")
    inputDataSwims = config.get(location, "INPUT_DATA_SWIMS")
    inputDataSwimslog = config.get(location, "INPUT_DATA_SWIMSLOG")
    outputData = config.get(location, "OUTPUT_DATA")
    minRange = config.get("TIMEFRAME", "MIN_RANGE")
    maxRange = config.get("TIMEFRAME", "MAX_RANGE")
    
    print("Loading buoy data:")
    buoyFilelist = collect_buoy_filelist(timeframe=range(int(minRange),int(maxRange)+1))
    dfBuoy=load_buoy_data(spark, buoyFilelist)

    print("Loading swimslog data:")
    dfSwimslog=load_swims_data(spark, inputDataSwimslog)
    
    print("Loading swims data:")
    dfSwims=load_swims_data(spark, inputDataSwims)
    # transformation
    dfSwims=dfSwims.withColumn("Year", date_format(to_date(col("Date"),"MM/dd/yyyy"), "yyyy"))\
                .withColumn("Month", date_format(to_date(col("Date"), "MM/dd/yyyy"), "MM"))\
                .withColumn("Day", date_format(to_date(col("Date"), "MM/dd/yyyy"), "dd"))
    dfSwims.show(5)

    #print("Creating swimmers table:")
    swimmers_table = create_swimmers_table(dfSwimslog, outputData)

    print("Creating locations table:")
    locations_table = create_locations_table(dfSwims, outputData)

    print("Creating conditions table:")
    conditions_table = create_conditions_table(dfSwims, dfBuoy, outputData)

    print("Creating time table:")
    time_table = create_time_table(dfSwims, outputData)

    print("Creating swims table:")
    swims_table = create_swims_table(dfSwimslog, dfSwims, swimmers_table, time_table, conditions_table, locations_table, outputData)

    # Testing min number of rows for each key table
    test_min_numb_rows('swimmers table', swimmers_table, 1001)
    test_min_numb_rows('buoys', dfBuoy, 40000)
    test_min_numb_rows('conditions table', conditions_table, 100)
    test_min_numb_rows('swims table', swims_table, 1)
    test_min_numb_rows('time table', time_table, 30)
    test_min_numb_rows('locations table', locations_table, 100)

    # Testing primary key uniqueness
    test_key_uniqueness ('swimmers table', swimmers_table, 'swimmerIdx')
    test_key_uniqueness ('swims table', swims_table, 'swimslogIdx')
    test_key_uniqueness ('conditions table', conditions_table, 'conditionsIdx')
    test_key_uniqueness ('time table', time_table, 'Date')
    test_key_uniqueness ('locations table', locations_table, 'locationIdx')

    print("All tests completed!")

if __name__ == "__main__":
    main()
