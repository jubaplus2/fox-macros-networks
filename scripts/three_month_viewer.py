import pandas as pd
import numpy as np
import datetime
import logging

startdate=datetime.datetime.strptime(str('20220124'), '%Y%m%d').date()

enddate=datetime.datetime.strptime(str('20220424'), '%Y%m%d').date()

directory=pd.read_csv('/home/nielsen/Nielsen_Directory_Server/NielsenDirectoryServer.csv')
directory['Week']=pd.to_datetime(directory['Week'])
directory['Week'] = directory['Week'].dt.date

## extract type30 to get tv information
names30=directory[((directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C30'))|((directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='B30'))]
names30=names30['FilePath']

df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    print(i)
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['Date']=df['all'].str[39:47]
    df['CableID']=df['all'].str[8:14]
    df['Network']=df['all'].str[3:8]
    df['ProgramID']=df['all'].str[14:21]
    df['ProgramName']=df['all'].str[55:80]
    df['StartTime']=df['all'].str[88:92].astype('int')
    df['EndTime']=df['all'].str[94:98].astype('int')
    type30=type30.append(df,ignore_index=True)
    
type30['Date']=pd.to_datetime(type30['Date'])
type30['Network']=type30.Network.str.strip()
type30['TelecastN']=type30['all'].str[21:28]
type30['EpisodeName']=type30['all'].str[130:170]
type30['EpisodeName']=type30.EpisodeName.str.strip()

type30=type30[['Date','CableID','Network','ProgramID','ProgramName','StartTime','EndTime','TelecastN','EpisodeName']]
# type30.to_csv("../outputs/three_month_tv.csv", index=False)
# type30=type30[type30['Network'].str.contains('FOXNC', regex=True)]

programtele=type30[['ProgramID','TelecastN']]
programtele=programtele.drop_duplicates()


## extract type36 to get HHID information
names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table'].isin(['C36','B36']))]
names36=names36['FilePath']

df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    print(i)
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['HHID']=df['all'].str[29:36]
    df['CableID']=df['all'].str[3:9]
    df['ViewDate']=df['all'].str[68:76]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['ProgramID']=df['all'].str[9:16]
    df['TelecastN']=df['all'].str[16:23]
    df['ViewStartTime']=df['all'].str[38:44].astype(int)
    df['ViewEndTime']=df['all'].str[44:50].astype(int)
    # df['ShiftedViewCode']=df['all'].str[76:77]
    # df['VCRCode']=df['all'].str[55:56]
    # df['PlatformCode']=df['all'].str[114:116]
    # df['VODIndicator']=df['all'].str[116:118]
    # df['DelayMins']=df['all'].str[109:114]
    df['PersonIntab'] = df['all'].str[60:66]
    df=df[['HHID','PersonID','PersonIntab','CableID','ProgramID','TelecastN','ViewDate','ViewStartTime','ViewEndTime']]
    df=pd.merge(df,programtele,on=['ProgramID','TelecastN'])
    type36=type36.append(df,ignore_index=True)

# type36.to_csv("../outputs/three_month_viewer.csv", index=False)


## merge type30(tv information) and type36(HHID information)
## redefine the type for some columns for further use

type302=pd.merge(type30,type36,on=['CableID','ProgramID', 'TelecastN'])
type302=type302[['Date', 'CableID', 'Network', 'ProgramID',
       'ProgramName', 'StartTime', 'EndTime', 'TelecastN', 
       'HHID', 'PersonID', 'PersonIntab', 'ViewDate','ViewStartTime', 'ViewEndTime', 'EpisodeName']]
type302['HHID']=type302['HHID'].astype(str).astype(int)
type302['PersonID']=type302['PersonID'].astype(str).astype(int)
type302['ProgramID']=type302['ProgramID'].astype(str).astype(int)
type302['PersonIntab']=type302['PersonIntab'].astype(str).astype(int)
type302['ViewDate_last']=type302['ViewDate']
type302['ViewDate']=pd.to_datetime(type302['ViewDate'])
type302['ViewStartTime2']=type302['ViewStartTime']
type302['ViewEndTime2']=type302['ViewEndTime']
type302['ViewStartTime2']=(np.floor(type302['StartTime']/100)+np.floor((type302['ViewStartTime2']+type302['StartTime']%100)/60))*100+(type302['ViewStartTime2']+type302['StartTime']%100)%60
type302['ViewEndTime2']=(np.floor(type302['StartTime']/100)+np.floor((type302['ViewEndTime2']+type302['StartTime']%100)/60))*100+(type302['ViewEndTime2']+type302['StartTime']%100)%60
type302['ViewTime']=type302['ViewEndTime']
type302['ViewTime']=type302['ViewTime']-type302['ViewStartTime']


type302.to_csv("../outputs/three_month_viewer_and_TV.csv", index=False)