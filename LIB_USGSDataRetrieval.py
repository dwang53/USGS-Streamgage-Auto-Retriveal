"""
USGS Streamgage Data Retrieval Script
-------------------------------------
This script facilitates the automated downloading of USGS gauge data.
It retrieves real-time streamflow data from the USGS National Water Information System (NWIS)
using the USGS Water Services API.

Author: Dongchen Wang
Date: 2024-10-15
Version: 1.0

Usage:
    - Ensure you have an active internet connection.
    - Customize the parameters as needed for your specific use case.
    - Run the script in a Python environment.
    - downloadUSGS(siteNo,dtype,startDT,endDT,saveheaderparth=None,printHeader=True) is for USGS streamgage time series data retrieval
    - downloadUSGSWQ(siteNo,dtype,paramgroup,saveheaderparth=None,printHeader=True) is for USGS water quality data retrieval
    - Use df.to_csv(fname) to save the datafile
    - Use readDownloadedData(fname) to read the downloaded data file
    
Dependencies:
    - urllib.request
    - pandas
    - numpy

Example:
    - python usgs_streamgage_retrieval.py
    - or check the jupyter notebook example: USGS_Streamgage-Auto-Retriveal_Example.ipynb

References:
    - USGS Water Services API Documentation: https://waterservices.usgs.gov/
"""


import numpy as np
import pandas as pd
import urllib.request
import urllib.parse

import sys,os

USGS_SEDIMENT_PARAMETER_FALLBACKS = {
    '69273': ('Suspended sediment, fall diameter (deionized water), percent smaller than 0.001 millimeters', '%', 'fraction'),
    '70331': ('Suspended sediment, sieve diameter, percent smaller than 0.0625 millimeters', '%', 'fraction'),
    '70332': ('Suspended sediment, sieve diameter, percent smaller than 0.125 millimeters', '%', 'fraction'),
    '70333': ('Suspended sediment, sieve diameter, percent smaller than 0.25 millimeters', '%', 'fraction'),
    '70334': ('Suspended sediment, sieve diameter, percent smaller than 0.5 millimeters', '%', 'fraction'),
    '70337': ('Suspended sediment, sieve diameter, percent smaller than 1 millimeter', '%', 'fraction'),
    '70338': ('Suspended sediment, sieve diameter, percent smaller than 2 millimeters', '%', 'fraction'),
    '70339': ('Suspended sediment, sieve diameter, percent smaller than 4 millimeters', '%', 'fraction'),
    '70340': ('Suspended sediment, sieve diameter, percent smaller than 8 millimeters', '%', 'fraction'),
    '70341': ('Suspended sediment, sieve diameter, percent smaller than 16 millimeters', '%', 'fraction'),
    '80154': ('Suspended sediment concentration, milligrams per liter', 'mg/l', 'kg/m3'),
}

def addSlash(text):
    return text+'/'


def buildObservedWQParameterSummary(df):
    '''
    Build a simple summary table for the parameter codes actually observed in a
    downloaded Water Quality Portal table.
    '''
    if 'USGSPCode' not in df.columns:
        return pd.DataFrame(columns=['USGSPCode','ResultMeasure/MeasureUnitCode','SuggestedUnitSI','ParameterExplanation'])
    summary = df[['USGSPCode','ResultMeasure/MeasureUnitCode']].dropna(subset=['USGSPCode']).drop_duplicates().copy()
    summary['USGSPCode'] = summary['USGSPCode'].astype(str)
    summary['ParameterExplanation'] = summary['USGSPCode'].map(
        lambda code: USGS_SEDIMENT_PARAMETER_FALLBACKS.get(code, ('', '', ''))[0]
    )
    summary['SuggestedUnitSI'] = summary['USGSPCode'].map(
        lambda code: USGS_SEDIMENT_PARAMETER_FALLBACKS.get(code, ('', '', ''))[2]
    )
    summary['SuggestedUnitSI'] = summary['SuggestedUnitSI'].replace('', np.nan)
    raw_unit = summary['ResultMeasure/MeasureUnitCode'].astype(str)
    summary.loc[raw_unit.str.lower()=='mg/l','SuggestedUnitSI'] = 'kg/m3'
    summary.loc[raw_unit=='%','SuggestedUnitSI'] = 'fraction'
    summary.loc[raw_unit.str.lower()=='tons/day','SuggestedUnitSI'] = 'kg/s'
    return summary.sort_values('USGSPCode').reset_index(drop=True)


def writeObservedWQParameterSummary(df, siteNo, dtype, characteristic_name=None, saveheaderparth=None):
    '''
    Write a plain-text header summary listing the Water Quality Portal parameter
    codes actually observed in the downloaded table.
    '''
    if saveheaderparth != None:
        outputHead=os.path.join(saveheaderparth,'USGS'+siteNo+'_'+dtype+'_head.txt')
    else:
        outputHead=os.path.join('USGS'+siteNo+'_'+dtype+'_head.txt')
    lines = ['# Downloaded from current Water Quality Portal endpoint\n']
    if characteristic_name is not None:
        lines.append('# characteristicName='+str(characteristic_name)+'\n')
    lines.append('# observed parameter codes in the downloaded table:\n')
    lines.append('# USGSPCode | raw unit | suggested SI unit | parameter explanation\n')
    summary = buildObservedWQParameterSummary(df)
    for _, row in summary.iterrows():
        lines.append(
            str(row['USGSPCode'])+' | '
            +str(row['ResultMeasure/MeasureUnitCode'])+' | '
            +str(row['SuggestedUnitSI'])+' | '
            +str(row['ParameterExplanation'])+'\n'
        )
    with open(outputHead,'wb') as f:
        for line in lines:
            f.write(line.encode('utf-8'))
    return outputHead

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

    #    https://waterservices.usgs.gov/nwis/iv/?sites=07381590&startDT=2024-10-08T21:40:46.407-05:00&endDT=2024-10-15T21:40:46.407-05:00&parameterCd=00065&format=rdb
    Url='https://waterservices.usgs.gov/nwis/'
    Url=Url+addSlash(dtype)
    Url=Url+'?format='+outformat+'&sites='+siteNo+'&startDT='+startDT+'&endDT='+endDT+'&siteStatus='+siteStatus
    return Url


def downloadUSGS(siteNo,dtype,startDT,endDT,saveheaderparth=None,printHeader=True):
    Url=genUSGSUrl(siteNo,dtype,startDT,endDT)
    if printHeader:
        print('Downloading ',Url)
    
    datahead=[]
    if saveheaderparth != None:
        outputHead=os.path.join(saveheaderparth,'USGS'+siteNo+'_'+dtype+'_head.txt')
    else:
        outputHead='USGS'+siteNo+'_'+dtype+'_head.txt'
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


def genUSGS_WQData_Url(siteNo,dtype,paramgroup=None,characteristic_name=None):
    '''
    # Water-quality retrieval now uses the current Water Quality Portal CSV API.
    # `paramgroup` is kept for backward compatibility and is mapped to the WQP
    # `characteristicGroup` query parameter.
    # `characteristic_name` can be used for a narrower query, for example
    # "Suspended Sediment Concentration (SSC)".
    '''
    query = {
        'siteid': 'USGS-'+siteNo,
        'mimeType': 'csv',
    }
    if characteristic_name is not None:
        query['characteristicName'] = characteristic_name
    elif paramgroup is not None:
        query['characteristicGroup'] = paramgroup
    Url='https://www.waterqualitydata.us/data/Result/search?'+urllib.parse.urlencode(query, quote_via=urllib.parse.quote)
    return Url




def downloadUSGSWQ(siteNo,dtype,paramgroup=None,saveheaderparth=None,printHeader=True,characteristic_name=None):
    # `dtype` is retained for backward compatibility with earlier calls even
    # though the current WQP endpoint does not use it in the URL.
    Url=genUSGS_WQData_Url(siteNo,dtype,paramgroup,characteristic_name=characteristic_name)
    if printHeader:
        print('Downloading ',Url)
    
    datahead=['# Downloaded from current Water Quality Portal endpoint\n'.encode('utf-8'),
              ('# URL: '+Url+'\n').encode('utf-8')]
    if saveheaderparth != None:
        outputHead=os.path.join(saveheaderparth,'USGS'+siteNo+'_'+dtype+'_head.txt')
    else:
        outputHead=os.path.join('USGS'+siteNo+'_'+dtype+'_head.txt')
    df=pd.read_csv(Url,low_memory=False)
    if 'ActivityStartTime/Time' in df.columns:
        df['ActivityStartTime/Time'] = df['ActivityStartTime/Time'].fillna('12:00:00')
    else:
        df['ActivityStartTime/Time'] = '12:00:00'
    df['datetime']=pd.to_datetime(
        df['ActivityStartDate'].astype(str)+' '+df['ActivityStartTime/Time'].astype(str),
        errors='coerce'
    )
    writeObservedWQParameterSummary(df, siteNo, dtype, characteristic_name=characteristic_name, saveheaderparth=saveheaderparth)
    df=df.set_index(['datetime'])
    return datahead,df


def convertCommonUnitsToSI(df):
    '''
    Convert a few common USGS streamflow and suspended-sediment units into SI.

    Recognized conversions:
    - ft3/s -> m3/s
    - ft -> m
    - ft/s -> m/s
    - mg/L -> kg/m3
    - % -> fraction
    - tons/day -> kg/s using short tons
    '''
    out=df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        for col in out.columns:
            col0 = str(col[0])
            if '00060' in col0 and not col0.endswith('_cd'):
                out[(col0+'_SI','m3/s')] = pd.to_numeric(out[col], errors='coerce')*0.028316846592
            if '00065' in col0 and not col0.endswith('_cd'):
                out[(col0+'_SI','m')] = pd.to_numeric(out[col], errors='coerce')*0.3048
            if '72294' in col0 and not col0.endswith('_cd'):
                out[(col0+'_SI','m/s')] = pd.to_numeric(out[col], errors='coerce')*0.3048
    else:
        if 'ResultMeasureValue' in out.columns and 'ResultMeasure/MeasureUnitCode' in out.columns:
            out['ResultMeasureValue_SI'] = pd.to_numeric(out['ResultMeasureValue'], errors='coerce').astype(float)
            out['ResultMeasureUnit_SI'] = out['ResultMeasure/MeasureUnitCode']
            mg_mask = out['ResultMeasure/MeasureUnitCode'].astype(str).str.lower() == 'mg/l'
            out.loc[mg_mask, 'ResultMeasureValue_SI'] = out.loc[mg_mask, 'ResultMeasureValue_SI']*0.001
            out.loc[mg_mask, 'ResultMeasureUnit_SI'] = 'kg/m3'
            pct_mask = out['ResultMeasure/MeasureUnitCode'].astype(str) == '%'
            out.loc[pct_mask, 'ResultMeasureValue_SI'] = out.loc[pct_mask, 'ResultMeasureValue_SI']/100.0
            out.loc[pct_mask, 'ResultMeasureUnit_SI'] = 'fraction'
            ton_mask = out['ResultMeasure/MeasureUnitCode'].astype(str).str.lower() == 'tons/day'
            out.loc[ton_mask, 'ResultMeasureValue_SI'] = out.loc[ton_mask, 'ResultMeasureValue_SI']*907.18474/86400.0
            out.loc[ton_mask, 'ResultMeasureUnit_SI'] = 'kg/s'
    return out


def findUSGSCode(Df, paramType):
    '''
    # This function can automatically find the parameter number in the USGS data 
    '''
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
        print('Error in findUSGSCode!!! Parameter code not found!')
        print('Please choose from: "Q", "Stage", "Umean", "Turbidity", "Tempmean", or edit this function to self define a parameter.')
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
    #USGS Streamgage flow data download example
    siteNo='07381590'
    startDT='2021-01-01'
    endDT='2022-12-31'
    dtype='iv'
    saveheaderparth='DataDownload'#The foldername to save the USGS header file
    datahead,df=downloadUSGS(siteNo,dtype,startDT,endDT,saveheaderparth=saveheaderparth)


    PlotParameterType='Q'
    plt.figure(figsize=(12,3))
    plt.plot(df[findUSGSCode(df, PlotParameterType)]['14n'])
    plt.grid()
    plt.ylabel(PlotParameterType)
    plt.show()

    #USGS Streamgage water quality data download example
    dtype='qwdata'
    paramgroup='SED'
    siteNo='15565447'
    saveheaderparth='DataDownload'#The foldername to save the USGS header file
    dataheadYukonSed,dfYukonSed=downloadUSGSWQ(siteNo,dtype,paramgroup,saveheaderparth=saveheaderparth)

    dfYukonSed.tail
