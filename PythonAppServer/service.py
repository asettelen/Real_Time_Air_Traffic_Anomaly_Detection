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

def processRequest(reportKey, args):
	reportDir = "../../static/reports/" + reportKey
	reportDirUrl = "http://192.168.37.152/static/reports/" + reportKey
	os.mkdir(reportDir)
	
	# exemple d'utilisation (il  faut coder les caractères spéciaux pour l'url)
	# http://192.168.37.152/viz/?action=listPlanesRadars&startDate=1000000&endDate=2000000
	# http://192.168.37.152/viz/?action=makeReport&type=type1&startDate=1000000&endDate=2000000&filterPlane=AAAA&filterRadar=all
	
	
	startDate = args['startDate']
	endDate = args['endDate']
	action = args['action']
	
	if action == "listPlanesRadars":
		
		response = "AAAA,BBBB,CCCC|00:1B:44:11:3A:B7,00:1B:44:11:3A:B8"
		
		
	elif action == "makeReport":
		reportType = args['type']
		filterPlane = args['filterPlane']
		filterRadar = args['filterRadar']
		
		if reportType == "type1":
		
			# récupération des données puis création des graphiques
			plt.plot([1,2,4,5,3,2,6,5,9,8,5,5,5,5,1,2,3,6,5,4,7,8,9,6,5,4,12,3])
			plt.savefig(reportDir + "/fig1.png")
			
			
			# génration du rapport
			title = "Report"
			reportAppendHTML(reportDir, "<html><head><title>" + title + "</title></head><body style=\"padding: 2em; color: #484848; font-family: arial,sans-serif;\">\n")
			reportAppendHTML(reportDir, "<h1>Report</h1>\n")
			reportAppendHTML(reportDir, "<h4>Here is the requested report with 8 graphs :</h4>\n")
			reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 1")
			reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 2")
			reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 3")
			reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 4")
			reportAppendFigure(reportDir, reportDirUrl, "fig1.png", "Figure 5")
			reportAppendFigure(reportDir, reportDirUrl, "fig1.png")
			reportAppendFigure(reportDir, reportDirUrl, "fig1.png")
			reportAppendFigure(reportDir, reportDirUrl, "fig1.png")
			reportAppendHTML(reportDir, "</body></html>")
			
			response = "C bon sa a marcher, l'argument startDate vaut : " + str(startDate)
			response = reportDirUrl + "/report.html"
	
			
		elif reportType == "type2":
			pass
	
	return response
	




print("")
print("------------------ service.py running... -------------------")
query = sys.argv[1].split("&")
reportKey = sys.argv[2]
filename = reportKey + ".tmp"
args = {}
for i in range(len(query)):
        c = query[i].split("=")
        args[c[0]] = urllib.parse.unquote(c[1])
for x in args:
        print(x + " = " + args[x])	
response = processRequest(reportKey, args)
file = open(filename, "w")
file.write(response)
file.close()
print("------------------ service.py finished   -------------------")
print("")