#!/usr/bin/python
#encoding: utf-8

from datetime import datetime
import requests
from threading import Thread
import dpkt
from dpkt.compat import compat_ord
import asterix
from time import sleep
from flask import Flask, jsonify, request, send_file, Response, stream_with_context
import sqlite3

app = Flask(__name__)

DATACENTER2_ADDRESS = "192.168.37.35:50005"

@app.route("/")
def racine():
    description = "src, cat, tid, ts, dst, sac, sic, tod, tn, theta, rho, fl, cgs, chdg"
    return description

def mac_addr(address):
    """Convert a MAC address to a readable/printable string
       Args:
           address (str): a MAC address in hex form (e.g. '\x01\x02\x03\x04\x05\x06')
       Returns:
           str: Printable/readable MAC address
    """
    return ':'.join('%02x' % compat_ord(b) for b in address)

def query_from_table_files(startD,stopD):
    conn = sqlite3.connect("files.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()
    cur.execute("SELECT rowid,name,moment,length FROM files;")
    rows = cur.fetchall()
    conn.close()
    files = [row[1] for row in rows if row[2]>=startD and row[2]<=stopD]
    return files

@app.route("/stream/<string:startDate>/<string:stopDate>")
def stream_from_pcap_directly(startDate="2019-04-19-00:00:00",stopDate="2019-04-19-23:59:59"):
    startD = datetime.strptime(startDate, "%Y-%m-%d-%H:%M:%S")
    stopD = datetime.strptime(stopDate, "%Y-%m-%d-%H:%M:%S")
    files = query_from_table_files(startD,stopD)
    def generate_csv_for_all_mac(files):
        for file in files:
            fichier=file
            try:
                f=open("/part1/"+fichier,"rb")
                pcap = dpkt.pcap.Reader(f)
                for ts, buf in pcap:
                    eth = dpkt.ethernet.Ethernet(buf)
                    dst = mac_addr(eth.dst)
                    if dst == '01:00:5e:50:10:c4':
                        continue
                    try:
                        data = eth.data.data.data
                    except:
                        data = eth.data.data
                    # print(dst)
                    try:
                        parsed = asterix.parse(data)
                        l = len(parsed)
                        i = 0
                        while i < l:
                            c = parsed[i]['category']
                            cat = 'Nan'
                            try:
                                cat = c
                            except:
                                pass
                            #if c == 48:
                            src= 'Nan'
                            tid = 'NaN'
                            ts ='Nan'
                            sac = 'NaN'
                            sic = 'NaN'
                            tod = 'NaN'
                            tn = 'NaN'
                            theta = 'NaN'
                            rho = 'NaN'
                            fl = 'NaN'
                            cgs = 'NaN'
                            chdg = 'NaN'
                            try:
                                src=mac_addr(eth.src)
                            except:
                                pass
                            try:
                                tid = parsed[i]['I240']['TId']['val']
                            except:
                                pass
                            try:
                                sac = parsed[i]['I010']['SAC']['val']
                            except:
                                pass
                            try:
                                sic = parsed[i]['I010']['SIC']['val']
                            except:
                                pass
                            try:
                                tod = parsed[i]['I140']['ToD']['val']
                            except:
                                pass
                            try:
                                tn = parsed[i]['I161']['Tn']['val']
                            except:
                                pass
                            try:
                                theta = parsed[i]['I040']['THETA']['val']
                            except:
                                pass
                            try:
                                rho = parsed[i]['I040']['RHO']['val']
                            except:
                                pass
                            try:
                                fl = parsed[i]['I090']['FL']['val']
                            except:
                                pass
                            try:
                                cgs = parsed[i]['I200']['CGS']['val']
                            except:
                                pass
                            try:
                                chdg = parsed[i]['I200']['CHdg']['val']
                            except:
                                pass
                            yield str(src)+','+str(cat)+','+str(tid)+','+str(ts)+','+dst+','+str(sac)+','+str(sic)+','+str(tod)+','+str(tn)+','+str(theta)+','+str(rho)+','+str(fl)+','+str(cgs)+','+str(chdg)+'\n'
                            #else:
                            #    pass
                            i = i + 1
                    except:
                        pass
                    else:
                        pass
                f.close()
            except:
                yield requests.get(url="http://%s/stream/%s"%(DATACENTER2_ADDRESS,fichier),stream=True).text
    return Response(stream_with_context(generate_csv_for_all_mac(files)), mimetype="text")


if __name__=="__main__":
    app.run(debug=False, host='0.0.0.0', port=50005)
