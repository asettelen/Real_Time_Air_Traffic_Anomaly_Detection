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
import sys
import colorsys
import random

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

"""
#Select specific radar on rdd
def getPacketsByRadar(dst):
    DST = '\'%' + dst + '%\'' 
    QUERY = 'SELECT * FROM global_temp.traffic \
            WHERE DST LIKE ' + str(DST)
    #print(QUERY)
    return spark.sql(QUERY).toPandas())"""

"""#Filter packets by TID and DST (list TID OR list SRC can be empty)
def getPacketsFiltered(list_TID,list_SRC):
    OR_LIST_TID=''
    if len(list_TID)!=0:
        for k in range(len(list_TID)):
            if k==0:
                OR_LIST_TID+='( TID LIKE \'%' + list_TID[k] + '%\''
            else:
                OR_LIST_TID+=' OR TID LIKE \'%' + list_TID[k] + '%\''
        OR_LIST_TID+=')'

    OR_LIST_SRC=''
    if len(list_SRC)!=0:
        for k in range(len(list_SRC)):
            if k==0 and OR_LIST_TID=='':
                OR_LIST_SRC+='(SRC LIKE \'%' + list_SRC[k] + '%\''
            elif k==0 and OR_LIST_TID!='':
                OR_LIST_SRC+=' AND (SRC LIKE \'%' + list_SRC[k] + '%\'' 
            else: 
                OR_LIST_SRC+=' OR SRC LIKE \'%' + list_SRC[k] + '%\'' 
        OR_LIST_SRC+=')'

    QUERY = str(OR_LIST_TID)+str(OR_LIST_SRC)
    filter_query = 'SELECT * FROM global_temp.traffic \
            WHERE ' + QUERY
    return(spark.sql(filter_query).toPandas())"""

#Select specific plane on rdd
def getPacketsByPlane(tid):
    TID = '\'%' + tid + '%\'' 
    QUERY = 'SELECT * FROM global_temp.traffic \
            WHERE TID LIKE ' + str(TID)
    #print(QUERY)
    return spark.sql(QUERY).toPandas()

def getPacketsByRadar(dst):
    DST = '\'%' + dst + '%\'' 
    QUERY = 'SELECT * FROM global_temp.traffic \
            WHERE DST LIKE ' + str(DST)
    #print(QUERY)
    return spark.sql(QUERY).toPandas()

def dictRadarsByAvion(TID, list_radar_aux=list()):
    df_temp = getPacketsByPlane(TID)
    list_radar = list(df_temp.groupby('DST').size().index)
    dictRadar = {}
    for dst in list_radar:
        if len(list_radar_aux) > 0: 
            if dst in list_radar_aux:
                dictRadar[dst] = df_temp[df_temp['DST'] == dst]
        else: 
            dictRadar[dst] = df_temp[df_temp['DST'] == dst]
    return dictRadar

def dictPlanesByRadar(DST, list_planes_aux=list()):
    df_temp = getPacketsByRadar(DST)
    list_planes = list(df_temp.groupby('TID').size().index)
    dictPlanes = {}
    for tid in list_planes:
        if len(list_planes_aux) > 0:  
            if tid != '' and tid.strip() in list_planes_aux:
                #dictRadar[dst] = filterByPlaneAndRadar(TID, dst) ##TOO MUCH TIME
                #print(tid)
                dictPlanes[tid] = df_temp[df_temp['TID'] == tid]
        else: 
            dictPlanes[tid] = df_temp[df_temp['TID'] == tid]
    return dictPlanes


        
"""def viz(dictRadar, TID):
    print("Start function viz \n")
    plotRadar(dictRadar)
    #sns.pairplot(data=getPacketsByPlane(TID), vars=['SAC', 'SIC', 'ToD', 'TN', 'THETA', 'RHO', 'FL', 'CGS', 'CHdg'])
    #plt.show()
    
    #sns.heatmap(data=getPacketsByPlane(TID)[['SIC', 'ToD', 'TN', 'THETA', 'FL', 'CGS', 'CHdg']].corr(), annot = True, cmap = 'Reds')
    #plt.show()"""

"""
def plotRadar(dictRadar, var=None,nb_plot_done):
    num_png=nb_plot_done
    if var == None:
        for var in list(dictRadar[list(dictRadar.keys())[0]].columns):
            i, m = 0, len(dictRadar.keys())
            for dst in dictRadar.keys():
                plt.plot(dictRadar[dst]['TS'], dictRadar[dst][var], randomColor(i, m))
                i = i + 1
            plt.xlabel('time(s)')
            plt.ylabel(var)
            print("Saving plot n°"+str(num_png)+" as png\n")
            plt.tight_layout()
            plt.savefig('test'+str(num_png)+'.png', format="PNG")
            plt.clf()
            num_png+=1
    else:
        i, m = 0, len(dictRadar.keys())
        for dst in dictRadar.keys():
            plt.plot(dictRadar[dst]['TS'], dictRadar[dst][var], randomColor(i, m))
            i = i + 1
        plt.xlabel('time(s)')
        plt.ylabel(var)
        print("Saving plot n°"+str(num_png)+" as png\n")
        plt.draw()
        plt.savefig('test'+str(num_png)+'.png', format="PNG")
        plt.clf()
        num_png+=1"""
def plotRadar(dictRadar, nb_plot_done):
    num_png=nb_plot_done
    for var in list(dictRadar[list(dictRadar.keys())[0]].columns):
        i, m = 0, len(dictRadar.keys())
        for dst in dictRadar.keys():
            plt.plot(dictRadar[dst]['TS'], dictRadar[dst][var], randomColor(i, m))
            i = i + 1
        plt.xlabel('time(s)')
        plt.ylabel(var)
        print("Saving plot n°"+str(num_png)+" as png\n")
        plt.tight_layout()
        plt.savefig('test'+str(num_png)+'.png', format="PNG")
        plt.clf()
        num_png+=1
   
        

#Rajout praiplot, heatmap dans les viz ?
def viz1(dictRadar,nb_plot_done):
    print("Save report type 1 \n")
    plotRadar(dictRadar,nb_plot_done)

def viz2(dictRadar, nb_plot_done):
    print("Save report type 2 \n")
    plotRadar(dictRadar,nb_plot_done)

 

colorsys.hsv_to_rgb(359,100,100)
def randomColor(i, m):
    return rgb_to_hex(colorsys.hsv_to_rgb(float(i) / float(m), 1, 1))
def rgb_to_hex(rgb):
    rgbInt = (int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))
    return '#%02x%02x%02x' % rgbInt

#Union de rdd à la rdd globale existante
def main_db(rdd): 
    global traffic_df_explicit, trafficSchema, spark  
    
    traffic_df_explicit_aux = spark.createDataFrame(rdd, trafficSchema)
    traffic_df_explicit = traffic_df_explicit.unionAll(traffic_df_explicit_aux)       
    
    traffic_df_explicit.createOrReplaceGlobalTempView('traffic')
    
    traffic_df_explicit.cache()
    

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
        
    print("Create spark dataframe")
    traffic_df_explicit = spark.createDataFrame(spark.sparkContext.emptyRDD(),trafficSchema)
    traffic_df_explicit.createOrReplaceGlobalTempView('traffic')

    date1=str(sys.argv[1])
    date2=str(sys.argv[2])

    print("--Start get requests--")
    response = requests.get('http://192.168.37.142:50005/stream/'+date1+'/'+date2, stream = True)

    
    

    i = 0
    list_aux = [] 
    for data in response.iter_lines():
        i = i + 1
        #print(data.decode("UTF-8").split(","))
        list_aux.append(data.decode("UTF-8").split(","))
       
        if not(i % 1000):
            print(i,"packets received")
            
        if (i==10000): 
            print("--End of get requests--")
            break 
    rdd_traffic = sc.parallelize(list_aux)
    rdd_traffic_clean = main_clean(rdd_traffic)
    main_db(rdd_traffic_clean)     
    
    list_planes = getlistplanes()
    print("List planes : ",list_planes)

    list_radars = getlistradars()
    print("List radars : ",list_radars)

    nb_plot_done=0
    #-----------CHOIX DE L'UTILISATEUR DES TID ET DST--------
    # A PLACER "AVANT"
    #On considere alors list_selected_tid, list_selected_dst
    #list_selected_tid :[*tid1*,*tid2*] 
    #format list_selected_dst  : (*dst1*,*dst2*) 
    #Exemple : 
    list_selected_tid=['BAW45MA','JAF3ML']
    list_selected_dst=('01:00:5e:50:00:26','01:00:5e:50:00:46','01:00:5e:50:00:06','01:00:5e:50:01:42')
    
    #Save report type 1 
    """
    for tid_selected in list_selected_tid:
        viz1(dictRadarsByAvion(tid_selected, list_radar_aux = list_selected_dst), nb_plot_done)
        nb_plot_done+=14
    """
    #viz1(dictRadarsByAvion('JAF3ML', list_radar_aux = ('01:00:5e:50:01:42')), 'JAF3ML')

    #Save report type 2
    for dst_selected in list_selected_dst:
        viz2(dictPlanesByRadar(dst_selected, list_planes_aux = list_selected_tid), nb_plot_done)
        nb_plot_done+=14
    #viz3(dictPlanesByRadar('01:00:5e:50:01:42'), '01:00:5e:50:01:42')

def getlistplanes(): 
    QUERY = 'SELECT DISTINCT(TID) FROM global_temp.traffic'
    return spark.sql(QUERY).toPandas()

def getlistradars(): 
    QUERY = 'SELECT DISTINCT(DST) FROM global_temp.traffic'
    return spark.sql(QUERY).toPandas()


if __name__== "__main__":
    main()


