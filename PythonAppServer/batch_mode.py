import pyspark
import pandas as pd
#import datefinder
import numpy as np
import matplotlib.pyplot as plt
#import seaborn as sns
import re   
import seaborn as sns
import statsmodels.api as sm
#from statsmodels.tsa.arima_model import ARIMA, ARIMAResults
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import acf, pacf, kpss
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.seasonal import seasonal_decompose
import datetime
#from fbprophet import Prophet
#import pmdarima as pm
#from pmdarima.model_selection import train_test_split
import numpy as np
import matplotlib.pyplot as plt
import requests
from pyspark.sql.types import *

sc = pyspark.SparkContext(appName="Spark RDD")


global spark
global traffic_df_explicit
global trafficSchema

#------Clean-----------
#Remove tuple with len !=14
def RemoveStrangeTupleLen(tup):
    if len(tup)==14:
        return tup
    
#CLEANING MAC_ADDRESS_SRC FIELD
def RemoveQuoteSrc(tup):
    if tup[0][0]=='"':
        tup[0]=tup[0][1:]
    return tup

def RemoveWeirdAddress(tup):
    if len(tup[0])>17:
        tup[0]=None
    return tup

#CLEANING CAT FIELD
def CATToInt(tup): 
    tup[1] = int(tup[1])
    return tup

#CLEANING TID FIELD
def replaceNullValue_TID(tup):
    if tup[2] != '' and tup[2] != None and tup[2] != 'NaN': 
        return tup
    else: 
        tup[2] = ''
        return tup
    
#CLEANING TS FIELD
def TSToFloat(tup): 
    tup[3] = float(round(float(tup[3])))
    return tup

#CLEANING DST FIELD
#None

#CLEANING SAC FIELD
def replaceNullValue_SAC(tup):
    if tup[5] != '' and tup[5] != None and tup[5] != 'NaN': 
        tup[5] = float(tup[5])
        return tup
    else: 
        tup[5] = None
        return tup

#CLEANING SIC FIELD
def replaceNullValue_SIC(tup):
    if tup[6] != '' and tup[6] != None and tup[6] != 'NaN': 
        tup[6] = float(tup[6])
        return tup
    else: 
        tup[6] = None
        return tup
    
#CLEANING ToD FIELD
def replaceNullValue_ToD(tup):
    if tup[7] != '' and tup[7] != None and tup[7] != 'NaN': 
        tup[7] = float(tup[7])
        return tup
    else: 
        tup[7] = None
        return tup

#CLEANING TN FIELD
def replaceNullValue_TN(tup):
    if tup[8] != '' and tup[8] != None and tup[8] != 'NaN': 
        tup[8] = float(tup[8])
        return tup
    else: 
        tup[8] = None
        return tup

#CLEANING THETA FIELD
def replaceNullValue_THETA(tup):
    if tup[9] != '' and tup[9] != None and tup[9] != 'NaN': 
        tup[9] = float(tup[9])
        return tup
    else: 
        tup[9] = None
        return tup

#CLEANING RHO FIELD
def replaceNullValue_RHO(tup):
    if tup[10] != '' and tup[10] != None and tup[10] != 'NaN': 
        tup[10] = float(tup[10])
        return tup
    else: 
        tup[10] = None
        return tup
    
#CLEANING FL FIELD 
def replaceNullValue_FL(tup):
    if tup[11] != '' and tup[11] != None and tup[11] != 'NaN': 
        tup[11] = float(tup[11])
        return tup
    else: 
        tup[11] = None
        return tup
    
#CLEANING CGS FIELD
def replaceNullValue_CGS(tup):
    if tup[12] != '' and tup[12] != None and tup[12] != 'NaN': 
        tup[12] = float(tup[12])
        return tup
    else: 
        tup[12] = None
        return tup
    
#CLEANING CHdg FIELD
    
def replaceNullValue_CHdg(tup):
    if tup[13] != '' and tup[13] != None and tup[13] != 'NaN': 
        tup[13] = float(tup[13])
        return tup
    else: 
        tup[13] = None
        return tup
    
def RemoveQuoteCHdg(tup):
    if tup[13][-1]=='"':
        tup[13]=tup[13][:-1]
    return tup


def cleaning(tup):
    tup = RemoveQuoteSrc(tup)
    tup = RemoveWeirdAddress(tup)
    tup = CATToInt(tup)
    tup = replaceNullValue_TID(tup)
    tup = TSToFloat(tup)
    tup = replaceNullValue_SAC(tup)
    tup = replaceNullValue_SIC(tup)
    tup = replaceNullValue_ToD(tup)
    tup = replaceNullValue_TN(tup) 
    tup = replaceNullValue_THETA(tup)
    tup = replaceNullValue_RHO(tup)
    tup = replaceNullValue_FL(tup)
    tup = replaceNullValue_CGS(tup)
    tup = replaceNullValue_CHdg(RemoveQuoteCHdg(tup))
    return tup
    
def main_clean(rdd):
    
    rdd = rdd.map(lambda tup: cleaning(tup))
    return(rdd)

#-----Fin Clean---------


#Select specific plane on rdd
def getPacketsByPlane(tid):
    TID = '\'%' + tid + '%\'' 
    QUERY = 'SELECT * FROM global_temp.traffic \
            WHERE TID LIKE ' + str(TID)
    #print(QUERY)
    return spark.sql(QUERY).toPandas()

#
def dictRadarsByAvion(TID):
    df_temp = getPacketsByPlane(TID)
    list_radar = list(df_temp.groupby('DST').size().index)
    dictRadar = {}
    for dst in list_radar:
        #dictRadar[dst] = filterByPlaneAndRadar(TID, dst) ##TOO MUCH TIME
        dictRadar[dst] = df_temp[df_temp['DST'] == dst] 
    return dictRadar

import colorsys
import random
colorsys.hsv_to_rgb(359,100,100)
def randomColor(i, m):
    return rgb_to_hex(colorsys.hsv_to_rgb(float(i) / float(m), 1, 1))
def rgb_to_hex(rgb):
    rgbInt = (int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))
    return '#%02x%02x%02x' % rgbInt

def plotRadar(dictRadar, var=None):
    num_png=0
    if var == None:
        for var in list(dictRadar[list(dictRadar.keys())[0]].columns):
            i, m = 0, len(dictRadar.keys())
            print("i : ",i)
            print("len dictRadar.keys :",m)
            try :
                print(dictRadar)
            except:
                print("fail to print dictRadar")
            for dst in dictRadar.keys():
                plt.plot(dictRadar[dst]['TS'], dictRadar[dst][var], randomColor(i, m))
                i = i + 1
            plt.xlabel('time(s)')
            plt.ylabel(var)
            print("plot png n "+str(num_png)+"\n")
            plt.savefig('test'+str(num_png)+'.png')
            num_png+=1
    else:
        i, m = 0, len(dictRadar.keys())
        for dst in dictRadar.keys():
            plt.plot(dictRadar[dst]['TS'], dictRadar[dst][var], randomColor(i, m))
            i = i + 1
        plt.xlabel('time(s)')
        plt.ylabel(var)
        print("Try save plot as png - 2  \n")
        plt.savefig('test'+str(num_png)+'.png')
        num_png+=1
        
        
def viz(dictRadar, TID):
    print("Start function viz \n")
    plotRadar(dictRadar)
    
    #sns.pairplot(data=getPacketsByPlane(TID), vars=['SAC', 'SIC', 'ToD', 'TN', 'THETA', 'RHO', 'FL', 'CGS', 'CHdg'])
    #plt.show()
    
    #sns.heatmap(data=getPacketsByPlane(TID)[['SIC', 'ToD', 'TN', 'THETA', 'FL', 'CGS', 'CHdg']].corr(), annot = True, cmap = 'Reds')
    #plt.show()


def main():
    global traffic_df_explicit, trafficSchema, spark  
    print("-----Start Batch request------")
    spark = pyspark.sql.SparkSession.builder.appName("Spark-Dataframe-SQL").getOrCreate()

    trafficSchema = StructType ( [StructField("SRC", StringType(), True),
                                    StructField("CAT", LongType(), True),
                                    StructField("TID", StringType(), True),
                                    StructField("TS", DoubleType(), True),
                                    StructField("DST", StringType(), True),
                                    StructField("SAC", DoubleType(), True),
                                    StructField("SIC", DoubleType(), True),
                                    StructField("ToD", DoubleType(), True),
                                    StructField("TN", DoubleType(), True),
                                    StructField("THETA", DoubleType(), True),
                                    StructField("RHO", DoubleType(), True),
                                    StructField("FL", DoubleType(), True),
                                    StructField("CGS", DoubleType(), True),
                                    StructField("CHdg", DoubleType(), True),
                                ] )
        

    traffic_df_explicit = spark.createDataFrame(spark.sparkContext.emptyRDD(),trafficSchema)
    try :
        print("traffic_df_explicit")
        print(traffic_df_explicit[0])
    except:
        print("fail to print traffic_df_explicit")
    traffic_df_explicit.createOrReplaceGlobalTempView('traffic')

    response = requests.get('http://192.168.37.142:50005/stream/2019-05-04-12:00:00/2019-05-04-16:00:00', stream = True)

    print("Trying get reponse")


    i = 0
    list_aux = [] 
    for data in response.iter_lines():
        i = i + 1
        #print(data.decode("UTF-8").split(","))
        list_aux.append(data.decode("UTF-8").split(","))
       
        if not(i % 10000):
            print(i)
            
        if (i==100000): 
            break 
    rdd_traffic = sc.parallelize(list_aux)
    rdd_traffic_clean = main_clean(rdd_traffic)
    main_db(rdd_traffic_clean)     #getlistavion()
    #viz(dictRadarsByAvion('JAF3ML'), 'JAF3ML')
    print("try get pandas dataframe : ")
    pandas=getPacketsByPlane("TOM97V")
    print(pandas)
    viz(dictRadarsByAvion('TOM97V'), 'TOM97V')

    

def getlistavion(): 
    QUERY = 'SELECT DISTINCT(TID) FROM global_temp.traffic'
    return spark.sql(QUERY).toPandas()

#Union de rdd à la rdd globale existante
def main_db(rdd): 
    global traffic_df_explicit, trafficSchema, spark  
    
    traffic_df_explicit_aux = spark.createDataFrame(rdd, trafficSchema)
    traffic_df_explicit = traffic_df_explicit.unionAll(traffic_df_explicit_aux)       
    
    traffic_df_explicit.createOrReplaceGlobalTempView('traffic')
    
    traffic_df_explicit.cache()


if __name__== "__main__":
    main()


