from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
import datetime 
import time
import os
import requests
import json


KAFKA_TOPIC = "urltopic"
CRAWL_DEPTH = 2
AUTH_TOKEN = os.environ['AUTH_TOKEN']
SKYLR_URL = "https://las-skylr-token.oscar.ncsu.edu/api/data/document/query"

kafka =  KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)

#startTime=(int(datetime.datetime.now().strftime("%s")) - 60*60*24*3)*1000
startTime=0
endTime=int(datetime.datetime.now().strftime("%s"))*1000
UrlFilterList=['localhost','moodle','groups.google','//newtab','wolfware.ncsu','shib.ncsu','chrome-extension','piazza','file//']

def getURLsFromSkylr():
    global startTime,endTime
    headers = {'Content-Type':'application/json', 'AuthToken':AUTH_TOKEN}
    data= {"type":"find","query":{'data.ProjId':"journaling-chrome", 'data.EvtTime':{'$gte':startTime,'$lte':endTime}}}
    startTime=endTime
    endTime=int(datetime.datetime.now().strftime("%s"))*1000
    #print data['query']['data.EvtTime']
    #try:
    r = requests.post(SKYLR_URL,data=json.dumps(data),headers=headers,verify=False)    
    #except:
    #    print "HTTP connection ( for fetching URLs ) to Skylr failed"
    #    return
    #print r.text
    response = json.loads(r.text)   
    for i in response['data']:
        event = i['data']
        #print event
        if 'WebURL' in event and 'TaskName' in event and type(event['TaskName']) is list:
        #    if event['WebURL'].startswith('http') and event['TaskName']!="":
        #        updateStateURL(state,event['WebURL'],formatTask(event['TaskName']),event['EvtTime'])
            if not isFiltered(event['WebURL']):
                print 
                producer.send_messages(KAFKA_TOPIC,bytes(event['WebURL'] + " "+ str(CRAWL_DEPTH) +" "+ "_".join(event['TaskName']) + " " +event['UserId']))
                print event['WebURL'] + " "+ str(CRAWL_DEPTH) +" "+ "_".join(event['TaskName']) + " " +event['UserId']

def isFiltered(url):
    for i in UrlFilterList:
        if i in url:
            return True
    return False

while True:
    getURLsFromSkylr()
    time.sleep(10)