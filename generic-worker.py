
#This is the basic service that pull events from the rabbit and pushes them to the table.

import time
import json
import sys
import azure
import socket
import pika
import requests
import urllib2
import pandas as pd
import numpy as np
import urllib
import json
import pickle
import unicodedata
from azure.storage import TableService, Entity, BlobService


#this sets up the connection to the queue from the service bus

def gettopic():
    creds = pika.PlainCredentials('your rabbit id', 'password')
    hostnm = socket.gethostname()
    #hostnm = socket.gethostbyaddr(socket.gethostname())[0]
    
    print hostnm
    queueset = set(["1", "2", "3", "4"] )
    #sendrest("start listening at "+str(hostnm))
    while True:
        try:
            connec = pika.BlockingConnection(pika.ConnectionParameters('rabbithost ip address', 5672, '/', creds))
            chan = connec.channel()
            chan.queue_declare(queue='roles')
            while True:
                    method_frame, header_frame, body = chan.basic_get('roles')
                    if method_frame:
                        chan.basic_ack(method_frame.delivery_tag)
                        if body in queueset:
                            chan.cancel()
                            connec.close()
                            return body
                        else:
                           print "unkown role command :"+str(hostnm)+"="+body
                    else:
                        time.sleep(1)
        except:
            print "oops"
            try:
                    chan.cancel()
                    connec.close()
            except:
                    print "double oops"
            time.sleep(5)

    requeued_messages = chan.cancel()
    connec.close()

def processevents(queuename, table_service, hostname):
    creds = pika.PlainCredentials('your rabbit id', 'password')
    connec = pika.BlockingConnection(pika.ConnectionParameters('rabbit ip address', 5672, '/', creds))
    chan = connec.channel()
    chan.queue_declare(queue=queuename)
    
    while True:
        try:
            method_frame, header_frame, body = chan.basic_get(queuename)
            if method_frame:
                t = "prefix "+body+ " "
                chan.basic_ack(method_frame.delivery_tag)
                #print t
            else:
                time.sleep(1)
                t = None
        except:
            print "queue error"
            t = None
        if t != None:
            #print "got something"
            start =t.find("key")
            if start > 0:
                t = t[start+4:]
                s = t.find(" ")
                mesnum = t[:s]
                s = t.find("from ")
                fromip = t[s+5:]
                try:
                    #print best, second, rk
                    partition = "part"+queuename
                    timst = str(int(time.time())%10000000)
                    item = {'PartitionKey': partition, 'RowKey': mesnum, 'hostname': fromip, 
                                 'timestamp': timst, 'enqueued': '00000'}
                    #print item
                    try:
                            table_service.insert_entity('gatherevents', item)
                    except:
                            s= "table service error ... likely duplicate"
                            print s
                except:
                    print "bad json object:"+tt
            else:
                print "no json object:"+t

import sys
import getopt

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
 
        #next set up the table service
        table_service = TableService(account_name='azure table', account_key='long account key for this storage service')
        table_service.create_table('name of table')
        hostname = socket.gethostname()
        quename = gettopic()
        processevents(quename, table_service, hostname)
        print "all done."
    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())
