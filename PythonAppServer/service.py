import pyspark
import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datefinder
from pyspark.sql.types import *
import colorsys
import random
import statsmodels.api as sm
from statsmodels.tsa.arima_model import ARIMA, ARIMAResults
from statsmodels.tsa.stattools import acf, pacf, kpss
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.seasonal import seasonal_decompose
from fbprophet import Prophet
import pmdarima as pm
from pmdarima.model_selection import train_test_split
import warnings
import itertools
import datetime
import codecs
import urllib
import time
from threading import Thread
from datetime import datetime
import errno
import requests

def reportAppendHTML(reportDir, content):
	filename = reportDir + "/report.html"	
	file = codecs.open(filename, "a", "utf-8")
	file.write(content)
	file.close()

def reportAppendFigure(reportDir, reportDirUrl, figName, legend = None):
	filename = reportDir + "/report.html"
	url = reportDirUrl + "/report.html"	
	file = codecs.open(filename, "a", "utf-8")
	file.write("<div style=\"display: inline-block; text-align: center;\">")
	file.write("<img src=\"" + reportDirUrl + "/" + figName + "\" />\n")
	if legend != None:
		file.write("<div style=\"display: inline-block; width: 100%; font-weight: bold;\">" + legend + "</div>\n")
	file.write("</div>\n")
	file.close()
	
def waitForRequests(args):
	try:
		os.mkfifo("mypipe")
	except OSError as oe: 
		if oe.errno != errno.EEXIST:
			raise
	while(1):
		time.sleep(1)
		print("Opening FIFO...")
		str = ""
		with open("mypipe") as fifo:
			print("FIFO opened")
			while True:
				data = fifo.read()
				if len(data) == 0:
					print("Writer closed")
					break
				str += data
		print("Fifo read " + str)
		query = str		
		query2 = query.split('&')
		args = {}
		for i in range(len(query2)):
			c = query2[i].split("=")
			args[c[0]] = urllib.parse.unquote(c[1])
		action = args['action']
		if action == "makeReport":
			now = datetime.now()
			reportKey = now.strftime("%Y-%m-%d-%H-%M-%S")
			reportDir = "../../static/reports/" + reportKey
			reportDirUrl = "http://192.168.37.152/static/reports/" + reportKey
			os.mkdir(reportDir)
			
			reportType = args['type']
			filterPlane = args['filterPlane']
			filterRadar = args['filterRadar']
			
			if filterPlane == '':
				filterPlane = allPlanes;
			if filterRadar == '':
				filterRadar = allRadars;
				
				
			print("All plane = " + allPlanes)
			print("All radar = " + allRadars)
			print("Filter plane = " + args['filterPlane'])
			print("Filter radar = " + args['filterRadar'])
			
			filterPlane = filterPlane.split(',')
			filterRadar = filterRadar.split(',')
			
			
			nb_plot_done=0
			#-----------CHOIX DE L'UTILISATEUR DES TID ET DST--------
			# A PLACER "AVANT"
			#On considere alors list_selected_tid, list_selected_dst
			#list_selected_tid :[*tid1*,*tid2*] 
			#format list_selected_dst  : (*dst1*,*dst2*) 
			#Exemple : 
			list_selected_tid=[]
			for i in range(len(filterPlane)):
				list_selected_tid.append(filterPlane[i])
			list_selected_dst=[]
			for i in range(len(filterRadar)):
				list_selected_dst.append(filterRadar[i])
			
			

			
			if reportType == "type1":
			
				# génration du rapport
				title = "Report"
				reportAppendHTML(reportDir, "<html><head><title>" + title + "</title></head><body style=\"padding: 2em; color: #484848; font-family: arial,sans-serif;\">\n")
				reportAppendHTML(reportDir, "<h1>Report</h1>\n")
				reportAppendHTML(reportDir, "<button onclick=\"window.location.href='http://192.168.37.152/static/reports/'\" style=\"padding: 0.4em 1em;\">Back to report list</button>\n")
				reportAppendHTML(reportDir, "<h4>Here is the requested report :</h4>\n")
			
				#Save report type 1 
				
				for tid_selected in list_selected_tid:
					viz1(dictRadarsByAvion(tid_selected, list_radar_aux = list_selected_dst), nb_plot_done, reportDir, reportDirUrl)
					nb_plot_done+=14

				#viz1(dictRadarsByAvion('JAF3ML', list_radar_aux = ('01:00:5e:50:01:42')), 'JAF3ML')
			
				# récupération des données puis création des graphiques
				"""plt.plot([1,2,4,5,3,2,6,5,9,8,5,5,5,5,1,2,3,6,5,4,7,8,9,6,5,4,12,3])
				plt.savefig(reportDir + "/fig1.png")
				
				
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 1")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 2")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 3")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 4")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 5")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png")"""
				reportAppendHTML(reportDir, "</body></html>")
				
				response = reportDirUrl + "/report.html"
		
				
			elif reportType == "type2":
				# génration du rapport
				title = "Report"
				reportAppendHTML(reportDir, "<html><head><title>" + title + "</title></head><body style=\"padding: 2em; color: #484848; font-family: arial,sans-serif;\">\n")
				reportAppendHTML(reportDir, "<h1>Report</h1>\n")
				reportAppendHTML(reportDir, "<button onclick=\"window.location.href='http://192.168.37.152/static/reports/'\" style=\"padding: 0.4em 1em;\">Back to report list</button>\n")
				reportAppendHTML(reportDir, "<h4>Here is the requested report :</h4>\n")
			
				#Save report type 2
				for dst_selected in list_selected_dst:
					viz2(dictPlanesByRadar(dst_selected, list_planes_aux = list_selected_tid), nb_plot_done, reportDir, reportDirUrl)
					nb_plot_done+=14
				#viz3(dictPlanesByRadar('01:00:5e:50:01:42'), '01:00:5e:50:01:42')
				
				#viz1(dictRadarsByAvion('JAF3ML', list_radar_aux = ('01:00:5e:50:01:42')), 'JAF3ML')
			
				# récupération des données puis création des graphiques
				"""plt.plot([1,2,4,5,3,2,6,5,9,8,5,5,5,5,1,2,3,6,5,4,7,8,9,6,5,4,12,3])
				plt.savefig(reportDir + "/fig1.png")
				
				
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 1")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 2")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 3")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 4")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 5")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png")
				reportAppendFigure(reportDir, reportDirUrl, "fig1.png")"""
				reportAppendHTML(reportDir, "</body></html>")
				
				response = reportDirUrl + "/report.html"
			elif reportType == "type3":
			
				if len(list_selected_tid) == 1 and len(list_selected_dst) == 1:
			
					# génration du rapport
					title = "Report"
					reportAppendHTML(reportDir, "<html><head><title>" + title + "</title></head><body style=\"padding: 2em; color: #484848; font-family: arial,sans-serif;\">\n")
					reportAppendHTML(reportDir, "<h1>Report</h1>\n")
					reportAppendHTML(reportDir, "<button onclick=\"window.location.href='http://192.168.37.152/static/reports/'\" style=\"padding: 0.4em 1em;\">Back to report list</button>\n")
					reportAppendHTML(reportDir, "<h4>Here is the requested report :</h4>\n")
					
					#timeseries(filterByPlaneAndRadar('NAX4258','01:00:5e:50:01:42'), 'CGS', reportDir, reportDirUrl)
					timeseries(filterByPlaneAndRadar(list_selected_tid[0], list_selected_dst[0]), 'CGS', reportDir, reportDirUrl)
					
					
					reportAppendHTML(reportDir, "</body></html>")
					
					response = reportDirUrl + "/report.html"
				
			file = open("event.out", "w") 
			file.write(response)
			file.close()
			
sc = pyspark.SparkContext(appName="Spark RDD")

global spark
global traffic_df_explicit
global trafficSchema
global allPlanes
global allRadars

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
def plotRadar(dictRadar, nb_plot_done, reportDir, reportDirUrl):
	if len(dictRadar) <= 0:
		print('Pas assez de paquets pour cette condition')
		return
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
		filename = reportDir + '/fig'+str(num_png)+'.png'
		plt.savefig(filename, format="PNG")
		reportAppendFigure(reportDir, reportDirUrl, "fig" + str(num_png) + ".png", "Figure " + str(num_png))
		plt.clf()
		num_png+=1
   
		
def filterByPlaneAndRadar(tid, dst):
	TID = '\'%' + tid + '%\'' 
	DST = '\'%' + dst + '%\'' 
	QUERY = 'SELECT * FROM global_temp.traffic WHERE TID LIKE ' + str(TID) + ' AND DST LIKE ' +  str(DST) 
	return spark.sql(QUERY).toPandas()

def timeseries(df, var, reportDir, reportDirUrl): 
	
	i = 0

	df_time_series_radar_avion = df.copy()
	df_time_series_radar_avion['TS_DATE'] = df_time_series_radar_avion['TS'].copy()
	
	for i in range(len(df_time_series_radar_avion['TS'])):
		df_time_series_radar_avion['TS_DATE'][i] = datetime.fromtimestamp(df_time_series_radar_avion['TS'][i].astype('int64'))
	
	ts = pd.Series(df_time_series_radar_avion[var].values, index = df_time_series_radar_avion['TS_DATE'])
	ts.plot(color='r')
	plt.title('Time Series Plot for CGS Variable over time')
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1
	
	result = seasonal_decompose(ts, model='additive', period=10)
	result.plot()
	plt.title('Time Series Decomposition for CGS Variable over time')
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1

	window = 20
	ma = ts.rolling(window).mean()
	mstd = ts.rolling(window).std()

	plt.figure()
	plt.plot(ts.index, ts, 'r')
	plt.plot(ma.index, ma, 'b')
	plt.title('Moving Average and Standard Deviation Analysis for CGS Variable over time')
	plt.fill_between(mstd.index, ma - 2 * mstd, ma + 2 * mstd, color='b', alpha=0.2)
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1
	
	###Fbprophet###
	
	m = Prophet()
	m.fit(df_time_series_radar_avion[['TS_DATE',var]].rename(columns={'TS_DATE': 'ds', var: "y"}))
	future = m.make_future_dataframe(periods=20,freq='min')
	future.tail()
	forecast = m.predict(future)
	plt.title('Prediction using FbProphet')
	m.plot(forecast)
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1
	
	#m.plot_components(forecast)
	
	##ARIMA/ARMA Models 
	
	ts_diff = ts - ts.shift()
	diff = ts_diff.dropna()
	plt.figure()
	plt.plot(diff)
	plt.title('First Difference Time Series Plot')
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1
	
	# DIAGNOSING ACF
	acf = plot_acf(diff, lags = 20)
	plt.title("ACF Plot")
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1
	
	# DIAGNOSING PACF
	pacf = plot_pacf(diff, lags = 20)
	plt.title("PACF Plot")
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1
	
	#print(pm.auto_arima(ts, seasonal=True, m=20))
	"""
	mod = ARIMA(ts, order = (1, 1, 1))
	results = mod.fit()
	ts.plot()
	plt.plot(results.fittedvalues[100:], color='red')
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1
	
	predVals = results.predict(len(ts.index), len(ts.index) + 200, typ='levels')
	dict_for = {}
	
	temps = df_time_series_radar_avion['TS'][len(ts.index) - 1]
	for i in range(len(ts.index), len(ts.index) + 200):
		dict_for[datetime.datetime.fromtimestamp(int(temps))] = predVals[i]
		temps = temps + round(df_time_series_radar_avion['TS'][len(ts.index) - 1] - df_time_series_radar_avion['TS'][len(ts.index) - 2])
	
	ts.plot()
	plt.plot(results.fittedvalues[100:], color='red')
	plt.plot(pd.Series(dict_for), color='orange')
	
	plt.savefig(reportDir + "/fig" + str(i) + ".png", format="PNG")
	reportAppendFigure(reportDir, reportDirUrl, "fig" + str(i) + ".png", "Figure " + str(i))
	plt.clf()
	i += 1
	
	window_for = 20
	ma_for = pd.Series(dict_for).rolling(window).mean()
	mstd_for = pd.Series(dict_for).rolling(window).std()

	plt.figure()
	plt.title('Prediction using ARMA model')
	plt.plot(ts.index, ts, 'b')
	plt.plot(results.fittedvalues[100:], color='red')
	plt.plot(pd.Series(dict_for), color='orange')
	plt.plot(ma_for.index, ma_for, 'b')
	plt.fill_between(mstd_for.index, ma_for - 2 * mstd_for, ma_for + 2 * mstd_for, color='g', alpha=1)"""

#Rajout praiplot, heatmap dans les viz ?
def viz1(dictRadar,nb_plot_done, reportDir, reportDirUrl):
	print("Save report type 1 \n")
	plotRadar(dictRadar,nb_plot_done, reportDir, reportDirUrl)

def viz2(dictRadar, nb_plot_done, reportDir, reportDirUrl):
	print("Save report type 2 \n")
	plotRadar(dictRadar,nb_plot_done, reportDir, reportDirUrl)

 

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
	

def startBatch(startDate, endDate):
	global traffic_df_explicit, trafficSchema, spark , allPlanes, allRadars
	
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

	#date1=str(sys.argv[1])
	#date2=str(sys.argv[2])
	date1 = datetime.fromtimestamp(int(startDate) / 1000).strftime('%Y-%m-%d-%H:%M:%S')
	date2 = datetime.fromtimestamp(int(endDate) / 1000).strftime('%Y-%m-%d-%H:%M:%S')
	print("Date1 : " + date1)
	print("Date2 : " + date2)
	
	print("--Start get requests--")
	response = requests.get('http://192.168.37.142:50005/stream/'+date1+'/'+date2 + '/false', stream = True)

	
	

	i = 0
	list_aux = [] 
	for data in response.iter_lines():
		i = i + 1
		#print(data.decode("UTF-8").split(","))
		list_aux.append(data.decode("UTF-8").split(","))
	   
		if not(i % 1000):
			print(i,"packets received")
		if not(i % 100000):
			rdd_traffic = sc.parallelize(list_aux)
			rdd_traffic_clean = main_clean(rdd_traffic)
			main_db(rdd_traffic_clean)	 
			list_aux = [] 
			
		"""if (i==4000001): 
			print(i,"Packets received \n")
			print("--End of get requests--")
			break """
	"""rdd_traffic = sc.parallelize(list_aux)
	rdd_traffic_clean = main_clean(rdd_traffic)
	main_db(rdd_traffic_clean)	""" 
	print("--End of get requests--")
	list_avions = getlistavion()
	print("Liste avions : ",list_avions)
	
	print('getNumberPacketByAvion')
	avionDF = getNumberPacketByAvion()
	print(avionDF)
	
	allPlanes = '';
	allPlanesCount = '';
	
	size = len(avionDF)
	for i in range(size):
		allPlanes += avionDF._get_value(i, 'TID').strip()
		allPlanesCount += avionDF._get_value(i, 'TID').strip() + '#' + str(avionDF._get_value(i, 'count(1)'))
		if i < size - 1:
			allPlanes += ','
			allPlanesCount += ','

	list_radars = getlistradars()
	
	allRadars = '';
	
	print("Liste radars : ",list_radars)
	
	size = len(list_radars)
	for i in range(size):
		allRadars += list_radars._get_value(i, 'DST').strip()
		if i < size - 1:
			allRadars += ','
	
	#id_plane=*LE TID choisi*
	#viz(dictRadarsByAvion(id_plane), id_plane)
	

	return allPlanesCount + '|' + allRadars;
	

def getlistavion(): 
	QUERY = 'SELECT DISTINCT(TID) FROM global_temp.traffic'
	return spark.sql(QUERY).toPandas()

def getNumberPacketByAvion(): 
	QUERY = 'SELECT TID, count(*) FROM global_temp.traffic GROUP BY TID ORDER BY count(*) DESC'
	return spark.sql(QUERY).toPandas()

def getlistradars(): 
	QUERY = 'SELECT DISTINCT(DST) FROM global_temp.traffic'
	return spark.sql(QUERY).toPandas()

				
def processRequest(name):
	
	# exemple d'utilisation (il  faut coder les caractères spéciaux pour l'url)
	# http://192.168.37.152/viz/?action=startBatch&startDate=1000000&endDate=2000000
	# http://192.168.37.152/viz/?action=makeReport&type=type1&filterPlane=AAAA&filterRadar=all
	
	
	startDate = args['startDate']
	endDate = args['endDate']
	action = args['action']
	
	if action == "startBatch":
		response = startBatch(startDate, endDate)
		#response = "AAAA,BBBB,CCCC,AAAA,BBBB,CCCC,AAAA,BBBB,CCCC,AAAA,BBBB,CCCC,AAAA,BBBB,CCCC,AAAA,BBBB,CCCC,AAAA,BBBB,CCCC,AAAA,BBBB,CCCC|00:1B:44:11:3A:B7,00:1B:44:11:3A:B8,00:1B:44:11:3A:B7,00:1B:44:11:3A:B8,00:1B:44:11:3A:B7,00:1B:44:11:3A:B8,00:1B:44:11:3A:B7,00:1B:44:11:3A:B8,00:1B:44:11:3A:B7,00:1B:44:11:3A:B8,00:1B:44:11:3A:B7,00:1B:44:11:3A:B8,00:1B:44:11:3A:B7,00:1B:44:11:3A:B8,00:1B:44:11:3A:B7,00:1B:44:11:3A:B8"
		thread = Thread(target = waitForRequests, args = ("", ))
		thread.start()
		
	elif action == "startRealtime":
		response = ""
		pass
	
	return response
	

print("")
print("------------------ service.py running... -------------------")
print(sys.argv[1])
query = sys.argv[1].split("&")
filename = "event.out"
args = {}
for i in range(len(query)):
	c = query[i].split("=")
	args[c[0]] = urllib.parse.unquote(c[1])
for x in args:
	print(x + " = " + args[x])	
response = processRequest(args)
file = open("event.out", "w") 
file.write(response)
file.close()
print("------------------ service.py finished   -------------------")
print("")