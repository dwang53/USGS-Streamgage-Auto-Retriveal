import numpy as np
import pandas as pd
import urllib.request

import sys,os

def addSlash(text):
    return text+'/'

def genUSGSUrl(siteNo,dtype,startDT,endDT):
    '''
    #dtype = iv(instantaneous values), dv(daily averaged value), mv(monthly averaged value), yv
    ##
    #&format=waterml // WaterML 1.1 wanted
    #&format=waterml,1.1 // WaterML version 1.1 wanted
    #&format=waterml,2.0 // WaterML version 2.0 wanted
    #&format=rdb
    #&format=rdb,1.0
    #&format=json // WaterML 1.1 translated into JSON
    #&format=json,1.1
    #&format=json,2.0 // A JSON version of WaterML2 is not presently available. Will cause an error.
    ##
    #Example startDT=2017-04-25
    ##
    #siteStatus=[ all | active | inactive ]
    ##
    '''
    outformat='rdb'
    siteStatus='all'
    
    Url='http://waterservices.usgs.gov/nwis/'
    Url=Url+addSlash(dtype)
    Url=Url+'?format='+outformat+'&sites='+siteNo+'&startDT='+startDT+'&endDT='+endDT+'&siteStatus='+siteStatus
    return Url


def downloadUSGS(siteNo,dtype,startDT,endDT,saveheaderparth=None,printHeader=True):
    Url=genUSGSUrl(siteNo,dtype,startDT,endDT)
    if printHeader:
        print('Downloading ',Url)
    
    datahead=[]
    outputHead=saveheaderparth+'USGS'+siteNo+'_'+dtype+'_head.txt'
    data = urllib.request.urlopen(Url) # it's a file like object and works just like a file
    for line in data: # files are iterable
        if b'#' in line:
            if printHeader:
                print(line)
            datahead.append(line)
    with open(outputHead,'wb') as f:
        for line in datahead:
            f.write(line)
    df=pd.read_csv(Url,sep='\t',comment='#',header=[0,1],low_memory=False)
    df['datetime']=pd.to_datetime(df['datetime']['20d'])
    df=df.set_index(df['datetime']['20d'])
    return datahead,df


def genUSGS_WQData_Url(siteNo,dtype,paramgroup):
    '''
    #dtype = iv(instantaneous values), dv(daily averaged value), mv(monthly averaged value), yv
    ##
    #&format=waterml // WaterML 1.1 wanted
    #&format=waterml,1.1 // WaterML version 1.1 wanted
    #&format=waterml,2.0 // WaterML version 2.0 wanted
    #&format=rdb
    #&format=rdb,1.0
    #&format=json // WaterML 1.1 translated into JSON
    #&format=json,1.1
    #&format=json,2.0 // A JSON version of WaterML2 is not presently available. Will cause an error.
    ##
    #Example startDT=2017-04-25
    ##
    #siteStatus=[ all | active | inactive ]
    ##
    '''
    outformat='rdb'
    siteStatus='all'
    #'https://nwis.waterdata.usgs.gov/nwis/qwdata/?site_no=15565447&agency_cd=USGS&param_group=SED&format=rdb'
    Url='https://nwis.waterdata.usgs.gov/nwis/'
    Url=Url+addSlash(dtype)
    Url=Url+'?site_no='+siteNo+'&agency_cd=USGS&param_group='+paramgroup+'&format='+outformat
    return Url




def downloadUSGSWQ(siteNo,dtype,paramgroup,saveheaderparth=None,printHeader=True):
    #Url=genUSGSUrl(siteNo,dtype,startDT,endDT)
    Url=genUSGS_WQData_Url(siteNo,dtype,paramgroup)
    if printHeader:
        print('Downloading ',Url)
    
    datahead=[]
    outputHead=saveheaderparth+'USGS'+siteNo+'_'+dtype+'_head.txt'
    data = urllib.request.urlopen(Url) # it's a file like object and works just like a file
    for line in data: # files are iterable
        if b'#' in line:
            if printHeader:
                print(line)
            datahead.append(line)
    with open(outputHead,'wb') as f:
        for line in datahead:
            f.write(line)
    df=pd.read_csv(Url,sep='\t',comment='#',header=[0,1],low_memory=False)
    
    df['datetime']=pd.to_datetime(df['sample_dt']['10d']+' '+ df['sample_tm']['5d']+':00')
    df=df.set_index(['datetime'])
    return datahead,df


def findUSGSCode(Df, paramType):
    if paramType=='Q':
        paramCode='00060'
    elif paramType=='Stage':
        paramCode='00065'
    elif paramType=='Umean':
        paramCode='72294'
    elif paramType=='Turbidity':
        paramCode='63680'
    elif paramType=='Tempmean':
        paramCode='00010'
    else:
        print('Error in findUSGSCode!!! Parameter code not found!!!')
        return
    
    paramCodes = [item[0] for item in Df.columns]
    result = [item for item in paramCodes if paramCode in item and not item.endswith("_cd")]
    if len(result)==0:
        print('Error in findUSGSCode!!! Parameter code not in the dataset!!!','USGS site number:',Df['site_no']['15s'].iloc[0])

        return
    if len(result)>1:
        result = [item for item in paramCodes if '00003' in item and not item.endswith("_cd")]
    
    return result[0]

def readDownloadedData(fname):
    df=pd.read_csv(fname,comment='#',header=[0,1],skiprows=[2],low_memory=False)
    if 'datetime' in df.columns:
        df=df.drop(['datetime'], axis=1,level=0)
    df['datetime']=pd.to_datetime(df['Unnamed: 0_level_0']['Unnamed: 0_level_1'])
    return df




if __name__ == "__main__":
    siteNo='05536368'
    startDT='2000-01-01'
    endDT='2022-12-31'
    dtype='iv'
    datahead,df=downloadUSGS(siteNo,dtype,startDT,endDT)

    dtype='qwdata'
    paramgroup='SED'
    siteNo='15565447'
    dataheadYukonSed,dfYukonSed=downloadUSGSWQ(siteNo,dtype,paramgroup)