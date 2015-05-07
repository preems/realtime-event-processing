__author__ = 'preems'

import yaml
import sys
import os


if len(sys.argv)!=2:
    print "Usage: setEnv.py <config.yaml>"
    exit()

stream = open(sys.argv[1], 'r')
conf = yaml.load(stream)

with open('setEnv.sh','w') as out:
    out.write('export CRAWL_DEFAULT_DEPTH='+str(conf['crawl.depth']))
    out.write('\nexport KAFKA_TOPIC_CRAWL=' + conf['kafka.topic.crawl.name'])
    out.write('\nexport PORT='+ str(conf['web.server.port']))
    out.write('\nexport KAFKA_ZK_HOST=' + conf['kafka.consumer.host.name']+":"+str(conf['kafka.consumer.host.port']))
    out.write('\nexport DRPC_SERVER_HOST=' + conf['drpc.host.name'])
    out.write('\nexport DRPC_SERVER_PORT='+ str(conf['drpc.host.port']))
    out.write('\nexport KAFKA_TOPIC_DOC='+ conf['kafka.topic.document.name'])
    out.write('\nexport KAFKA_PRODUCER_HOST=' + conf['kafka.producer.host.name']+":"+str(conf['kafka.producer.host.port']))

#os.environ['CRAWL_DEFAULT_DEPTH']=str(conf['crawl.depth'])
#os.environ['KAFKA_TOPIC_CRAWL']=conf['kafka.topic.crawl.name']
#os.environ['PORT']='3000'
#os.environ['KAFKA_ZK_HOST']=conf['kafka.consumer.host.name']+":"+str(conf['kafka.consumer.host.port'])
