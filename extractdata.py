#!/usr/bin/env python

import os
import dpkt
import time
import datetime
import socket
from dpkt.compat import compat_ord
import asterix
import pandas
import matplotlib.pyplot as plt
import sys
import pickle
"""
try :
  fichierarg = sys.argv[1]
except IndexError :
  print "Il faut rentrer les donnees a utiliser en argument 1 !"
  exit ()
"""

def mac_addr(address):
	"""Convert a MAC address to a readable/printable string
	   Args:
		   address (str): a MAC address in hex form (e.g. '\x01\x02\x03\x04\x05\x06')
	   Returns:
		   str: Printable/readable MAC address
	"""
	return ':'.join('%02x' % compat_ord(b) for b in address)


def moyenne(liste):
	return somme(liste)/len(liste)

list_fichier=[]
list_fichier.append('dacota-5min.pcap')

if __name__ == '__main__':

	for file in list_fichier:
		TS=[]
		ToD=[]
		DST=[]
		SAC=[]
		SIC=[]
		CAT=[]
		TN=[]
		RHO=[]
		THETA=[]
		FL=[]
		CGS=[]
		CHdg=[]
		print('--- Etude de '+ file + '---')
		fichier=file
		f=open(fichier,"rb")
		pcap=dpkt.pcap.Reader(f)
		for ts, buf in pcap:
			eth=dpkt.ethernet.Ethernet(buf)
			dst=mac_addr(eth.dst)
			if dst == "01:00:5e:50:00:26" :          
				try:
					data=eth.data.data.data
				except: 
					data=eth.data.data
				parsed=asterix.parse(data)
				l=len(parsed)
				i=0
				while i<l:
					c=parsed[i]['category']
					if c==48:
						TS.append(ts)
						DST.append(dst)
						try:
							sac=parsed[i]['I010']['SAC']['val']
							SAC.append(sac)
						except:
							SAC.append('NaN')
						try:
							sic=parsed[i]['I010']['SIC']['val']
							SIC.append(sic)
						except:
							SIC.append('NaN')
						try:
							tod=parsed[i]['I140']['ToD']['val']
							ToD.append(tod)
						except:
							ToD.append('NaN')
						try:
							tn=parsed[i]['I161']['Tn']['val']
							TN.append(tn)
						except:
							TN.append('NaN')
						try:
							theta=parsed[i]['I040']['THETA']['val']
							THETA.append(theta)
						except:
							THETA.append('NaN')
						try:
							rho=parsed[i]['I040']['RHO']['val']
							RHO.append(rho)
						except:
							RHO.append('NaN')
						try:
							fl=parsed[i]['I090']['FL']['val']
							FL.append(fl)
						except:
							FL.append('NaN')
						try:
							cgs=parsed[i]['I200']['CGS']['val']
							CGS.append(cgs)
						except:
							CGS.append('NaN')
						try:
							chdg=parsed[i]['I200']['CHdg']['val']
							CHdg.append(chdg)
						except:
							CHdg.append('NaN')
					else:
						pass
					i=i+1
				else:
					pass
				
		d={'TS': TS, 'DST':DST, 'SAC':SAC, 'SIC':SIC, 'ToD': ToD, 'TN': TN, 'THETA': THETA, 'RHO': RHO, 'FL': FL, 'CGS':CGS, 'CHdg': CHdg}
		df=pandas.DataFrame(data=d, index=TS)
		enregistrement=file.replace('.pcap','')+'.csv'  
		df.to_csv (enregistrement, index = None, header=True)

		print('---Fin de l etude de '+ file +'---')

