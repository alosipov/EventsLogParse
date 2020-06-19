import re
import sys
import datetime  
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
from influxdb import InfluxDBClient

time.sleep(5)

def parse_fix(line,MsgType):
    Msg={}
    if line.find(MsgType)>0:
        fix=re.split("\x01",line[line.find('8='):])
        prefix=re.split(" ",line[:line.find('8=')])
        if (fix) and (prefix): 
            Msg={**Msg, 'time':prefix[3].split("<")[0]} 
            for x in fix:
                fix_tag=iter(x.split("="))
                fixtag=dict(zip(fix_tag,fix_tag))
                Msg={**Msg,**fixtag}
        return Msg

def plot_data(Messages):
    y=np.array(Messages)[:,0].astype(int)
    x=np.array(Messages)[:,1]
    fig=plt.figure(figsize=(20,10))    
    plt.plot(x,y)
    plt.gcf().autofmt_xdate()
    plt.xticks(rotation=90)
    plt.yticks([100,200,300,400,500])
    plt.show()
    return 

def GetFixValues(Msg,TagsReturn):
    ListTags=[None]
    if Msg:
        if TagsReturn:
            for t in TagsReturn:
                if t in Msg.keys():
                    ListTags.append(Msg[t])
                else:
                    ListTags.append(None)
    return ListTags

def getDate(Tag):
    Tag=datetime.datetime.strptime(Tag,"%H:%M:%S.%f")
    return Tag


data=[]
file = open(sys.argv[2], "r")
dateshift=int(sys.argv[3])

Points=[]
Messages=[]
count=0
measurement_name="quik_timn"+sys.argv[1]

client = InfluxDBClient('10.1.110.25', 8086, 'root', 'root')
client.create_database('quik_timn')


while True:
    point=""
    count+=1
    Msg={}
    line=file.readline()
    
    if not line:
        time.sleep(30) 
#    if count==num_lines:
#        break
    
    if line.find("TIMN")>0:

        ln=line.replace("<","=").replace(">",";").replace("(",";").replace(":p",";p").replace(" ",";").split(';')       

        i=0
        for item in ln:
            if item.find("TIMN")>0:

                ln[i]="TIMN="+ln[i+1].replace("=",":")
            i+=1
            
        i=0
        for item in ln[0:8]:
            if item.find(")")>0:
                ln[i]="LogTime="+ln[i+1]
            i+=1
            
        for item in ln:
            if item.find("="):
                timnval=iter(item.split("="))                
                value=dict(zip(timnval,timnval))
                Msg={**Msg,**value}

        try:
            date=datetime.date.today()-datetime.timedelta(dateshift)
            timeL=datetime.datetime.strptime(Msg["LogTime"],"%H:%M:%S.%f")
            dt=datetime.datetime.combine(date,timeL.time()).timestamp()*1000
        
            point="{measurement},Timn={Timn},Thread={Thread},p={p} AvgTimn={AvgTimn}i,MaxTimn={MaxTimn}i,MinTimn={MinTimn}i,Count={Count}i {Ts}".format(
                measurement=measurement_name,
                Ts=int(dt),
                Timn=Msg["TIMN"],  
                p=Msg["p"],  
                Thread=Msg["threadId"],
                AvgTimn=Msg["avgTime"],
                MaxTimn=Msg["maxTime"],
                Count=Msg["count"],
                MinTimn=Msg["minTime"])

        except Exception as t:
            True
            print(t,'\n',ln)
            
        Points.append(point)
        Messages.append(Msg)


    try:
        result=client.write_points(Points, database='quik_timn', time_precision='ms', batch_size=100, protocol='line')
        Points=[]
    except Exception as e:
        raise e


    


