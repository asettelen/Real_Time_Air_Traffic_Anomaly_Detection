#!/usr/bin/python
#coding: utf-8

from datetime import datetime
import requests
from threading import Thread
import dpkt
from dpkt.compat import compat_ord
import asterix
from time import sleep
import socket


def mac_addr(address):
    """Convert a MAC address to a readable/printable string
       Args:
           address (str): a MAC address in hex form (e.g. '\x01\x02\x03\x04\x05\x06')
       Returns:
           str: Printable/readable MAC address
    """
    return ':'.join('%02x' % compat_ord(b) for b in address)

LOG=""
NB_PARSE_ERRORS=0

def log(socket):
    socket.sendall(b"%s"%(LOG+"nombre d'erreurs parse:"+str(NB_PARSE_ERRORS)+"\n"))
    socket.close()

def stream_from_pcap_directly(socket, file="2019-12-09-1751.pcap"):
    global NB_PARSE_ERRORS
    global LOG
    fichier = file
    f = open("/add/" + fichier, "rb")
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
        try:
            parsed = asterix.parse(data)
            l = len(parsed)
            i = 0
            while i < l:
                cat = parsed[i]['category']
                src = 'Nan'
                tid = 'NaN'
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
                    src = mac_addr(eth.src)
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
                socket.sendall(b'%s'%(str(src) + ',' + str(cat) + ',' + str(tid) + ',' + str(ts) + ',' + dst + ',' + str(
                    sac) + ',' + str(sic) + ',' + str(tod) + ',' + str(tn) + ',' + str(theta) + ',' + str(
                    rho) + ',' + str(fl) + ',' + str(cgs) + ',' + str(chdg) + '\n'))
                i = i + 1
        except:
            NB_PARSE_ERRORS += 1
            LOG = str(ts) + "\n" + str(buf) + "\n"
            pass
        else:
            pass
    socket.sendall(b'END')
    socket.close()
    f.close()

if __name__=="__main__":
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if server:
        server.bind(('', 50005))
        server.listen(1)
        print("Listening on port 50005")
        print("********************************")
        while True:
            datacenter2_socket, address = server.accept()
            print("Got a connection from: %s"%(str(address)))
            file_name = datacenter2_socket.recv(1024)
            if 'FILE:' in file_name:
                file_name = file_name.split(":")[1]
                print("Delivering content from file: %s"%(file_name))
                th = Thread(target=stream_from_pcap_directly, args=(datacenter2_socket, file_name))
                th.start()
                print("--------------------------------")
            elif 'LOG:' in file_name:
                print("Delivering log")
                th = Thread(target=log, args=(datacenter2_socket))
                th.start()
                print("--------------------------------")
