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
from fbprophet import Prophet
#import pmdarima as pm
#from pmdarima.model_selection import train_test_split
import numpy as np
import matplotlib.pyplot as plt
import requests
from pyspark.sql.types import *
import math
from threading import Thread
import time
from pyspark.sql.functions import collect_list, struct, to_timestamp


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
    
    #header = rdd.first()
    #rdd = rdd.filter(lambda line: line != header)
    #rdd = rdd.map(lambda tup: RemoveStrangeTupleLen(tup))\
    #         .filter(lambda tup: tup!=None)
             
    rdd = rdd.map(lambda tup: cleaning(tup))
    return(rdd)

# we first need to import types (e.g. StructType, StructField, IntegerType, etc.)
from pyspark.sql.types import *

def getlistavion(): 
    QUERY = 'SELECT DISTINCT(TID) FROM global_temp.traffic'
    return spark.sql(QUERY).toPandas()

def main_db(rdd): 
    global traffic_df_explicit, trafficSchema, spark  
    
    traffic_df_explicit_aux = spark.createDataFrame(rdd, trafficSchema)
    traffic_df_explicit = traffic_df_explicit.unionAll(traffic_df_explicit_aux)       
    
    traffic_df_explicit.createOrReplaceGlobalTempView('traffic')
    
    traffic_df_explicit.cache()
    
    #Queries
    #spark.sql("select TID, DST, COUNT(*) from global_temp.traffic WHERE TID != '' GROUP BY TID, DST ORDER BY COUNT(*) DESC").show()
    #spark.sql("select * from global_temp.traffic").show()

def transform_data_m(row):
    """Transform data from pyspark.sql.Row to python dict to be used in rdd."""
    data = row['data']
    tid = row['TID']
    dst = row['DST']
    
    # Transform [pyspark.sql.Dataframe.Row] -> [dict]
    data_dicts = []
    for d in data:
        data_dicts.append(d.asDict())

    # Convert into pandas dataframe for fbprophet
    data = pd.DataFrame(data_dicts)
    data['ds'] = pd.to_datetime(data['ds'], unit='s')

    return {
        'tid' : tid,
        'dst' : dst,
        'data': data,
    }

def partition_data_m(d):
    """Split data into training and testing based on timestamp."""
    # Extract data from pd.Dataframe
    data = d['data']

    # Find max timestamp and extract timestamp for start of day
    max_datetime = max(data['ds'])
    #start_datetime = max_datetime.replace(hour=00, minute=00, second=00)

    # Extract training data
    train_data = data[data['ds'] <= max_datetime]

    # Account for zeros in data while still applying uniform transform
    #train_data['y'] = train_data['y'].apply(lambda x: np.log(x + 1))

    # Assign train/test split
    #d['test_data'] = data.loc[(data['ds'] < start_datetime)
    #                          & (data['ds'] <= max_datetime)]
    d['train_data'] = train_data

    return d

def create_model_m(d):
    """Create Prophet model using each input grid parameter set."""
    m = Prophet()
    d['model'] = m

    return d

def train_model_m(d):
    """Fit the model using the training data."""
    model = d['model']
    train_data = d['train_data']
    model.fit(train_data)
    d['model'] = model

    return d

def make_forecast_m(d):
    """Execute the forecast method on the model to make future predictions."""
    model = d['model']
    future = model.make_future_dataframe(
        periods=10, freq='4s')
    
    forecast = model.predict(future)
    d['forecast'] = forecast

    return d

def reduce_data_scope_m(d):
    """Return a tuple (app + , + metric_type, {})."""
    return (
        d['tid'] + ',' + d['dst'],
        {
            'forecast': pd.concat([d['train_data']['y'],d['forecast']], axis=1),  
        },
    )

def expand_predictions_m(d):
    tid_dst, data = d
    tid, dst = tid_dst.split(',')
    return [
        (
            tid, 
            dst,
            #p['ds'].strftime("%d-%b-%Y (%H:%M:%S)"),
            time.mktime(datetime.datetime.strptime(p['ds'].strftime("%d-%b-%Y (%H:%M:%S)"), "%d-%b-%Y (%H:%M:%S)").timetuple()),
            p['y'] if not(math.isnan(p['y'])) else None,
            p['yhat'],
            p['yhat_lower'],
            p['yhat_upper'],
        ) for i, p in data['forecast'].iterrows()
    ]

def pred(var):
    
    global traffic_df_explicit, spark, schema_for_m
    
    traffic_for_m = traffic_df_explicit.select(
                     traffic_df_explicit['TID'],
                     traffic_df_explicit['DST'],                    
                     traffic_df_explicit['TS'].cast(IntegerType()).alias('ds'), 
                     traffic_df_explicit[var].alias('y'))\
                   .filter("TID like '%DSO05LM%' and DST like '%01:00:5e:50:01:42%'")\
                   .groupBy('TID', 'DST')\
                   .agg(collect_list(struct('ds', 'y')).alias('data'))\
                   .rdd.map(lambda r: transform_data_m(r))\
                       .map(lambda d: partition_data_m(d))\
                       .filter(lambda d: len(d['train_data']) > 2)\
                       .map(lambda d: create_model_m(d))\
                       .map(lambda d: train_model_m(d))\
                       .map(lambda d: make_forecast_m(d))\
                       .map(lambda d: reduce_data_scope_m(d))\
                       .flatMap(lambda d: expand_predictions_m(d))\
        
    traffic_for_m.cache()
        
    df_for_m = spark.createDataFrame(traffic_for_m, schema_for_m)
            
    #thread
            
    TH = Thread(target = forecast_from_spark, args=(df_for_m,var))
    TH.start()

def forecast_from_spark(df, var):
     #pas de show mais un filter sur les y == NAN pour n'envoyer que les forecast pour ces valeurs mais pas les anciennes
    #df.show()
    #df_for_m.filter(" y == 'NaN'").show() et et transformer y en cgs
    print("Try prediction")
    print(df.select('*').withColumnRenamed('y', var).show())
    print("End prediction")
    #envoie de y et de la prédiction 
    #pour chaque ligne du df 
        #insert_table(table_name, conn, tid, dst, ds, y, yhat, yhat_lower, yhat_upper)

def main():
    

    print("----Start real time mode -----")
    global traffic_df_explicit, spark, schema_for_m, trafficSchema

    sc = pyspark.SparkContext(appName="Spark RDD")

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
    traffic_df_explicit.createOrReplaceGlobalTempView('traffic')

    schema_for_m = StructType([
            StructField("tid", StringType(), True),
            StructField("dst", StringType(), True),
            StructField("ds", StringType(), True),
            StructField("y", FloatType(), True),
            StructField("yhat", FloatType(), True),
            StructField("yhat_lower", FloatType(), True),
            StructField("yhat_upper", FloatType(), True),
        ])
        
    list_aux = [] 
    cmpt_tram = 0        

    response = requests.get('http://192.168.37.142:50005/stream/2019-05-04-12:00:00/2019-05-04-16:00:00', stream=True)
    #type(response)

    #Faire la même chose pour chdg et fl 
    #create_table(table_name="cgs", parameters="tid STRING, dst STRING, \
    #             ds STRING, y FLOAT, yhat FLOAT, yhat_lower FLOAT, yhat_upper FLOAT", database_name="activus"):

    i = 0

    for data in response.iter_lines():
        #print(data.decode("UTF-8"))  
        #print(str(data)[1:])
        i = i + 1
        #print(i)
        #print([data.decode("UTF-8").split(",")]) 
        #print(data.decode("UTF-8").split(","))
        
        ligne = data.decode("UTF-8").split(",")
        list_aux.append(ligne)
        
        
        #prédire lorsque j'ai 5 nouveaux paquets pour un avion considere et un radar considere 
        #-> prediction 
        #ligne[2] : TID
        #ligne[4] : DST
        
        #Pour l'avion et le radar considere
        if('DSO05LM' in ligne[2] and '01:00:5e:50:01:42' in ligne[4]):
            #compteur pour le nombre de tram   
            #print(ligne)
            
            #print(compt_tram)
            
            rdd_traffic = sc.parallelize(list_aux)
            rdd_traffic_clean = main_clean(rdd_traffic)
            
            #print(rdd_traffic_clean.collect())
            main_db(rdd_traffic_clean) 
            
            #faire un show pour un envoi en temps réel à la base de données sql
            
            cmpt_tram += 1 
            list_aux = []     
            
        #time series 
        
            #envoie de la prédiction toutes les 5 trams
            if(cmpt_tram==5):
                
                #faire la prédiction sur la variable de son choix 
                #pred(traffic_df_explicit, var='CGS')
                #pred(traffic_df_explicit, var='CHdg')
                #pred(traffic_df_explicit, var='FL')
                
                #pred(spark, traffic_df_explicit, schema_for_m)
                
                pred(var='CGS')
                #pred(var='CHdg')
                #pred(var='FL')

            
                #Réinitialisation du compteur
                cmpt_tram=0
                
            #Envoie de y
            #clean et envoi de la ligne à la volée
            tid = ligne[2]
            dst = ligne[4]
            ds = float(round(float(ligne[3])))
            FL = float(ligne[11])
            CGS = float(ligne[12])
            CHdg = float(ligne[13])
            yhat = None 
            yhat_lower = None
            yhat_upper = None
            
            #d = {'tid': [tid], 'dst': [dst], 'ds': [ds], 'y': [y], 'yhat': [yhat], 
            #     'yhat_lower': [yhat_lower], 'yhat_upper': [yhat_upper]}
            
                #spark.createDataFrame(traffic_for_m, schema_for_m).show()
            
            print(pd.DataFrame(data={'tid': [tid], 'dst': [dst], 'ds': [ds], 'FL': [FL], 'yhat': [yhat], 
                'yhat_lower': [yhat_lower], 'yhat_upper': [yhat_upper]}))
            print(pd.DataFrame(data={'tid': [tid], 'dst': [dst], 'ds': [ds], 'CGS': [CGS], 'yhat': [yhat], 
                'yhat_lower': [yhat_lower], 'yhat_upper': [yhat_upper]}))
            print(pd.DataFrame(data={'tid': [tid], 'dst': [dst], 'ds': [ds], 'CHdg': [CHdg], 'yhat': [yhat], 
                'yhat_lower': [yhat_lower], 'yhat_upper': [yhat_upper]}))
            
                #insert_table(table_name, conn, tid, dst, ds, y, yhat, yhat_lower, yhat_upper)
                
        
        if(not(i%1000)):
            print(i)
            
        if (i==100000): break 

if __name__== '__main__':
    main()