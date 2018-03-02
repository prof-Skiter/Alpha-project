# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 10:53:05 2018

@author: skiter
"""

from sqlalchemy import create_engine
import pandas as pd
import datetime, re, os

global mypath
mypath = r"c:\Python\git\Alpha project\output\\"

def query(df):    
    while True:
        site = input('Site name: ')
        if site == 'n' or '': break
    
        gbLTEold = df['LTE_old'].groupby('Site Name').get_group(site)
        gbLTEnew = df['LTE_new'].groupby('Site Name').get_group(site)
    
        a = input('RFDS? ')
        if a == 'y': getRFDS(site)
        
        ip = gbLTEnew['IP'].drop_duplicates()
        
        hwold = gbLTEold\
            .groupby('Layer', as_index=False)['Site Name','Sector Name','RMOD','Rfsharing','TXRX','dlMimoMode','pMax','Freq/BW']\
            .agg({'Site Name':lambda x: str(set(x)).strip("{}'"),
                  'Sector Name':'count',
                  'RMOD':lambda x: str(set(x)).strip("{}'"),
                  'Rfsharing':lambda x: str(set(x)).strip("{}'"),
                  'TXRX':lambda x: str(set(x)).strip("{}'"),
                  'dlMimoMode':lambda x: str(set(x)).strip("{}'"),
                  'pMax':lambda x: str(set(x)).strip("{}'"),
                  'Freq/BW':lambda x: str(set(x)).strip("{}'")})
 
        hwnew = gbLTEnew\
            .groupby('Layer', as_index=False)['Site Name','Sector Name','RMOD','Rfsharing','TXRX','dlMimoMode','pMax','Freq/BW']\
            .agg({'Site Name':lambda x: str(set(x)).strip("{}'"),
                  'Sector Name':'count',
                  'RMOD':lambda x: str(set(x)).strip("{}'"),
                  'Rfsharing':lambda x: str(set(x)).strip("{}'"),
                  'TXRX':lambda x: str(set(x)).strip("{}'"),
                  'dlMimoMode':lambda x: str(set(x)).strip("{}'"),
                  'pMax':lambda x: str(set(x)).strip("{}'"),
                  'Freq/BW':lambda x: str(set(x)).strip("{}'")})
        
        
        dt = gbLTEnew[['MRBTS/WBTS','Site Name','Sector Name','Cell ID','Freq/BW','PCI/PSC','administrativeState','cellBarred','primPlmnCellres']]
        
        if site in df['3G_old']['Site Name'].drop_duplicates().values:
            gb3Gold = df['3G_old'].groupby('Site Name').get_group(site)
            hw3Gold = gb3Gold\
                    .groupby('Layer', as_index=False)['Site Name','Sector Name','RMOD','Freq/BW']\
                    .agg({'Site Name':lambda x: str(set(x)).strip("{}'"),
                          'Sector Name':'count',
                          'RMOD':lambda x: str(set(x)).strip("{}'"),                          
                          'Freq/BW':lambda x: str(set(x)).strip("{}'")})            
            hwold = hwold.append(hw3Gold)        
 
        if site in df['3G_new']['Site Name'].drop_duplicates().values:
            gb3Gnew = df['3G_new'].groupby('Site Name').get_group(site)
            hw3Gnew = gb3Gnew\
                    .groupby('Layer', as_index=False)['Site Name','Sector Name','RMOD','Freq/BW']\
                    .agg({'Site Name':lambda x: str(set(x)).strip("{}'"),
                          'Sector Name':'count',
                          'RMOD':lambda x: str(set(x)).strip("{}'"),                          
                          'Freq/BW':lambda x: str(set(x)).strip("{}'")})            
            hwnew = hwnew.append(hw3Gnew)        
            ip = ip.append(gb3Gnew['IP'].drop_duplicates())            
            dt = dt.append(gb3Gnew)
  
       
        dt[['MRBTS/WBTS','Site Name','Sector Name','Cell ID','Freq/BW','PCI/PSC','administrativeState','cellBarred','primPlmnCellres']].to_clipboard(index=False)        
        input("DT data copied\nPress Enter to continue...")
        
        ip.to_clipboard(index=False)
        input("IPs copied\nPress Enter to continue...")

        hwold[['Site Name','Layer','Sector Name','RMOD','Rfsharing','TXRX','dlMimoMode','pMax','Freq/BW']].to_clipboard(index=False)
        input("Old Hardware check copied\nPress Enter to continue...")
        
        hwnew[['Site Name','Layer','Sector Name','RMOD','Rfsharing','TXRX','dlMimoMode','pMax','Freq/BW']].to_clipboard(index=False)
        input("New Hardware check copied\nPress Enter to continue...")
        

def fetch_data(statement, cn):
    df = pd.DataFrame(cn.execute(statement).fetchall())
    return df
    
def getIPNO():
    df = fetch_data("""
                      SELECT OBJ.CO_DN, IPNO_MPIA_8
                      FROM C_LTE_IPNO IPNO
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON IPNO.OBJ_GID = OBJ.CO_GID
                      WHERE IPNO.CONF_ID = 1
                      """, LTEconnection)
    df.columns = ['MRBTS/WBTS','IP']
    df['MRBTS/WBTS'] = df['MRBTS/WBTS'].apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
    print('IPNO done')
    return df


def getRMOD():
    df = fetch_data("""
                      SELECT OBJ.CO_DN, RMOD_R_PRODUCT_NAME
                      FROM C_SRER_RMOD_R RMOD
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON RMOD.OBJ_GID = OBJ.CO_GID
                      WHERE RMOD.CONF_ID = 1
                      """, LTEconnection)
    df.columns = ['MRBTS/WBTS','RMOD']
    df['MRBTS/WBTS'] = df['MRBTS/WBTS'].apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
    
    df = df\
        .fillna('X')\
        .groupby('MRBTS/WBTS')['RMOD']\
        .apply(lambda x:
            "%s" % ' '.join(
                    ['x'.join([str((x == i).sum()), i]
                    ) for i in set(x)]))
    
    df = df.reset_index(level=0)
    print('RMOD done')
    return df

def getRFSH():
    df = fetch_data("""
                      SELECT  OBJ.CO_DN, MNL.MNL_R_5R64499SRT
                      FROM C_SRM_MNL_R MNL
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON MNL.OBJ_GID = OBJ.CO_GID
                      WHERE MNL.CONF_ID = 1
                      """, LTEconnection)
    df.columns = ['MRBTS/WBTS','Rfsharing']
    df['MRBTS/WBTS'] = df['MRBTS/WBTS'].apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
    df.Rfsharing = df.Rfsharing.map({0:'none',
                                         1:'UTRAN-EUTRA',
                                         2:'UTRAN-GERAN',
                                         3:'EUTRA-GERAN',
                                         4:'UTRAN-GERAN/CONCURRENT',
                                         5:'EUTRA-GERAN/CONCURRENT',
                                         6:'EUTRA-EUTRA',
                                         7:'EUTRA-EUTRA/EUTRA-GERAN',
                                         8:'EUTRA-CDMA'})
    print('RFSH done')
    return df

def getTXRX():
    df = fetch_data("""
                      SELECT OBJ.CO_DN, CH.CH_DIRECTION
                      FROM C_SRM_CH CH
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON CH.OBJ_GID = OBJ.CO_GID
                      WHERE CH.CONF_ID = 1
                      """, LTEconnection)
    df.columns = ['CELLID','TXRX']
    df.TXRX = df.TXRX.map({1:'TX', 2:'RX'})
    #TXRX.CELLID = TXRX.CELLID.apply(lambda x: x.split(sep='/')[1].replace('MRBTS-', '') + '-' + x.split(sep='/')[5].replace('LCELL-', ''))
    df.CELLID = df.CELLID.apply(lambda x:
                                    re.search("(?<=MRBTS-)(\d+)", x).group(1) +
                                    '-' + 
                                    re.search("(?<=LCELL-)(\d+)", x).group(1))
    
    df = df\
        .groupby('CELLID')['TXRX']\
        .apply(lambda x:
            "%s" % ''.join(
                    [''.join([str((x == i).sum()), i]
                    ) for i in sorted(set(x), reverse=True)]))
    df = df.reset_index(level=0)
    print('TXRX done')
    return df

def getLNCEL():
    df = fetch_data("""
                    SELECT OBJ.CO_DN AS DN,
                    LNCEL.LNCEL_CELL_NAME AS CellName,
                    LNCEL.LNCEL_EUTRA_CEL_ID AS eutraCelId,
                    LNCEL.LNCEL_PHY_CELL_ID AS phyCellId,
                    LNCEL_FDD.LNCEL_FDD_EARFCN_DL AS earfcnDL,
                    LNCEL_FDD.LNCEL_FDD_DL_CH_BW/10 AS dlChBw,
                    LNCEL.LNCEL_P_MAX AS pMax,
                    LNCEL_FDD.LNCEL_FDD_DL_MIMO_MODE as dlMimoMode,
                    LNCEL.LNCEL_AS_26 AS administrativeState,
                    LNCEL_SIB.BARRED AS cellBarred,
                    LNCEL_SIB.RESERVED AS primPlmnCellres                       
                    FROM C_LTE_LNCEL_FDD LNCEL_FDD
                    LEFT JOIN CTP_COMMON_OBJECTS OBJ ON LNCEL_FDD.OBJ_GID = OBJ.CO_GID
                    LEFT JOIN C_LTE_LNCEL LNCEL ON OBJ.CO_PARENT_GID = LNCEL.OBJ_GID
                    LEFT JOIN
                           (SELECT OBJ1.CO_PARENT_GID SIB_PARENT, SIB_CLL_BARRED BARRED, SIB_PPCR_55 RESERVED
                            FROM C_LTE_SIB SIB
                            LEFT JOIN CTP_COMMON_OBJECTS OBJ1 ON SIB.OBJ_GID = OBJ1.CO_GID
                            WHERE SIB.CONF_ID = 1
                            ) LNCEL_SIB ON LNCEL_SIB.SIB_PARENT = LNCEL.OBJ_GID
                                          
                    WHERE LNCEL.CONF_ID = 1 AND LNCEL_FDD.CONF_ID = 1
                    """, LTEconnection)
    df.columns = ['DN','Sector Name','Cell ID','PCI/PSC','earfcnDL','dlChBw','pMax','dlMimoMode','administrativeState','cellBarred','primPlmnCellres']

    df['CELLID'] = df.DN.apply(lambda x:
                                   re.search("(?<=MRBTS-)(\d+)", x).group(1) + 
                                   '-' + 
                                   re.search("(?<=LNCEL-)(\d+)", x).group(1))
    df['MRBTS/WBTS'] = df.DN.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
    df['ID'] = df.DN.apply(lambda x: re.search("(?<=LNCEL-)(\d+)", x).group(1)).astype(int)
    
    df['Site Name'] = df['Sector Name'].str[1:9]
    df.earfcnDL = df.earfcnDL.astype(str)
    df.dlChBw = df.dlChBw.astype(str)
    df['Freq/BW'] = df.earfcnDL + '/' + df.dlChBw
    df['Layer'] = pd.cut(df.ID, range(0,111,10))
    df['Layer'].cat.categories = ['L2100','L1900','L700','L2100-3','NA1','NA2','L600','NA3','NA4','NA5','L2100-2']
    df['Layer'].cat.reorder_categories(['L2100','L2100-2','L2100-3','L1900','L700','L600','NA1','NA2','NA3','NA4','NA5'], ordered=True, inplace=True)
    df['Layer'].cat.remove_unused_categories(inplace=True)
    
    df.drop(['DN', 'earfcnDL', 'dlChBw'],axis=1, inplace=True)
    
    df.dlMimoMode = df.dlMimoMode.map({0:'SingleTX',\
                                             10:'TXDiv',\
                                             11:'4-way TXDiv',\
                                             30:'Dynamic Open Loop MIMO',\
                                             40:'Closed Loop Mimo',\
                                             41:'Closed Loop MIMO (4x2)',\
                                             43:'Closed Loop MIMO (4x4)' })
    df.administrativeState = df.administrativeState.map({1:'unlocked',
                                                         2:'shutting down',
                                                         3:'locked'})
    df.cellBarred = df.cellBarred.map({0:'barred',
                                       1:'notBarred'})
    
    df.primPlmnCellres = df.primPlmnCellres.map({0:'Not Reserved',
                                                 1:'Reserved'})
    
    #df = df[['CELLID','ID','Layer','MRBTS','Site','CellName','eutraCelId','FreqBW','phyCellId','pMax','dlMimoMode','administrativeState','cellBarred','primPlmnCellres']]
    
    print('LNCEL done')
    return df

def get3G():
    df = fetch_data("""
                    SELECT OBJ.CO_DN,
                           OBJ.CO_NAME,
                           WCEL.WCEL_C_ID,
                           WCEL.WCEL_UARFCN,
                           WCEL.WCEL_PRI_SCR_CODE,
                           WCEL.WCEL_ACS_31,
                           WCEL.WCEL_CELL_BARRED,
                           WCEL.WCEL_CELL_RESERVED,
                           WBTS.WBTS_BTSIP_ADDRESS
                    FROM C_RNC_WCEL WCEL
                    LEFT JOIN CTP_COMMON_OBJECTS OBJ ON WCEL.OBJ_GID = OBJ.CO_GID
                    LEFT JOIN C_RNC_WBTS  WBTS ON OBJ.CO_PARENT_GID = WBTS.OBJ_GID
                    WHERE WCEL.CONF_ID = 1 AND WBTS.CONF_ID = 1
                    """, LTEconnection)
        
    df.columns = ['MRBTS/WBTS','Sector Name','Cell ID','Freq/BW','PCI/PSC','administrativeState','cellBarred','primPlmnCellres','IP']
    df['RNC_WBTS'] = df['MRBTS/WBTS'].apply(lambda x:
                                                   re.search("(?<=RNC-)(\d+)", x).group(1) + 
                                                   '-' + 
                                                   re.search("(?<=WBTS-)(\d+)", x).group(1))  
    df['MRBTS/WBTS'] = df['MRBTS/WBTS'].apply(lambda x: re.search("(?<=WBTS-)(\d+)", x).group(1))
    df['Site Name'] = df['Sector Name'].str[1:9]
    df['Layer'] = df['Sector Name'].str[0].map({'P':'UPCS','U':'UAWS'})
    df['administrativeState'] = df['administrativeState'].map({0:'Locked',
                                                               1:'Unlocked'})
    
    df['cellBarred'] = df['cellBarred'].map({0:'Barred',
                                             1:'Not barred'})
    
    df['primPlmnCellres'] = df['primPlmnCellres'].map({0:'Reserved',
                                                       1:'Not reserved'})
      
    RMOD = fetch_data("""
                      SELECT OBJ.CO_DN, RMOD_PROD_CODE
                      FROM C_BTC_RMOD RMOD
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON OBJ.CO_GID = RMOD.OBJ_GID
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ1 ON OBJ1.CO_GID = OBJ.CO_PARENT_GID                
                      LEFT JOIN C_RNC_WBTS WBTS ON OBJ1.CO_PARENT_GID = WBTS.OBJ_GID
                      WHERE RMOD.CONF_ID = 1 AND WBTS.CONF_ID = 1
                      """, LTEconnection)
      
    RMOD.columns = ['RNC_WBTS','RMOD']
    RMOD['RNC_WBTS'] = RMOD['RNC_WBTS'].apply(lambda x:
                                       re.search("(?<=RNC-)(\d+)", x).group(1) + 
                                       '-' + 
                                       re.search("(?<=WBTS-)(\d+)", x).group(1))
    RMOD['RMOD'] = RMOD['RMOD'].str.split('.').str[0].map({'471000A':'FRIA',
                                                           '471895A':'FRIE',
                                                           '472679A':'FXFC',
                                                           '473042A':'FHFB',
                                                           '472569A':'FXFB',
                                                           '473368A':'FRIJ'})
    
    RMOD = RMOD\
            .fillna('X')\
            .groupby('RNC_WBTS', as_index=False)\
            .agg(lambda x: "%s" % ' '.join(['x'.join([str((x == i).sum()), i]) for i in set(x)]))
  
    print('3G done')
    return pd.merge(df, RMOD, on='RNC_WBTS')

def load_df():    
    result = {}
    curdir = os.listdir(mypath)    
    print("Choose a baseline data")
    for tech in ['LTE', '3G']:
        file_dict = {i:f for i,f in enumerate(curdir) if tech in f}
        print('Old ' + tech + ' dumps: ')
        for i in file_dict.keys():
            print(i,': ', file_dict[i])
        a = input('Which one? ')
        file = file_dict[int(a)]
        print('Open file: ', file)
        result[tech + '_old'] = pd.read_csv(mypath + file)
    
    a = input('Pull data from OSS? ')
    if a == 'n':
        for tech in ['LTE', '3G']:        
            date = datetime.datetime.strptime('2018-01-01', "%Y-%m-%d")
            for f in curdir:
                if tech in f:
                    tmp = datetime.datetime.strptime(re.search("^\d+-\d+-\d+", f).group(0), "%Y-%m-%d")
                    if tmp > date:
                        date = tmp
                        file = f
            result[tech + '_new'] = pd.read_csv(mypath + file)
        
    elif a == 'y':
        LTEengine = create_engine('oracle://aurfeng:Default1@T4OSS')
        global LTEconnection
        LTEconnection = LTEengine.connect()
        result['LTE_new'] = pd.merge(getLNCEL(), getTXRX(), on='CELLID')
        result['LTE_new'] = pd.merge(result['LTE_new'], getRMOD(), on='MRBTS/WBTS')
        result['LTE_new'] = pd.merge(result['LTE_new'], getRFSH(), on='MRBTS/WBTS')
        result['LTE_new'] = pd.merge(result['LTE_new'], getIPNO(), on='MRBTS/WBTS')
        result['3G_new'] = get3G()
        a = input('Save? ')
        if a == 'y':
            now = datetime.datetime.now()
            result['LTE_new'].to_csv(mypath + str(now).split()[0] + "_" + "LTE_CFG_QUERRY.csv", index=False)
            result['3G_new'].to_csv(mypath + str(now).split()[0] + "_" + "3G_CFG_QUERRY.csv", index=False)
            print('Saved')
    
    else: print('error')
    
        
    return result

def getRFDS(siteID):
    
    RFDSengine = create_engine('mssql+pyodbc://sskiter1:sskiter1@edw123@PRDEDWPLYW2SQL0.corporate.t-mobile.com/DM_RFDS?driver=SQL+Server+Native+Client+11.0')
    RFDSconnection = RFDSengine.connect()
    
    stmt =  """
            SELECT SL.SiteID
    	         ,AL.SiteLayoutName
    	         ,ST.StatusTypeName
    	         ,SEC.SectorName
    	         ,AD.Azimuth
    	         ,AD.AntennaPosition
            FROM SiteLayout AS SL
            LEFT JOIN SiteLayout AS AL ON AL.SiteLayoutID = SL.ALConfig_SiteLayoutID
            LEFT JOIN StatusType AS ST ON ST.StatusTypeID = SL.StatusTypeID
            LEFT JOIN SiteLayout_SectorLayout AS SSL ON SL.SiteLayoutID = SSL.SiteLayoutID
            LEFT JOIN SectorLayout AS SEC ON SEC.SectorLayoutID = SSL.SectorLayoutID
            LEFT JOIN AntennaDetail AS AD ON SSL.SectorLayoutID = AD.SectorLayoutID
            WHERE SL.SiteID Like '{}'
            """.format(siteID)
    results = RFDSconnection.execute(stmt).fetchall()
    AZ = pd.DataFrame(results)
    AZ.columns = results[0].keys()
    AZ.Azimuth = AZ.Azimuth.fillna(0).astype(int)
    
    config = AZ[['SiteLayoutName']].drop_duplicates().reset_index(drop=True)
    choice = {i:config.SiteLayoutName[i] for i in range(len(config))}
    print('Choose config:')
    for i in range(len(choice)):
        print(i,': ',choice[i])
    a = input()
    AZ = AZ[AZ['SiteLayoutName'] == config.SiteLayoutName[int(a)]]
    
    status = AZ[['StatusTypeName']].drop_duplicates().reset_index(drop=True)
    choice = {i:status.StatusTypeName[i] for i in range(len(status))}
    print('Choose status:')
    for i in range(len(choice)):
        print(i,': ',choice[i])
    a = input()
    AZ = AZ[AZ['StatusTypeName'] == status.StatusTypeName[int(a)]]
    
    tmp = AZ.groupby('SectorName')['Azimuth'].apply(lambda x: "%s" % str(set(x)).strip("{}"))
    Azimuths = '/'.join(tmp)
    
    print('Azimuths: ', Azimuths)
    
    stmt =  """
            SELECT [SiteID]
                  ,[Address]
                  ,[City]
                  ,[State]
                  ,[Zip]
                  ,[Latitude]
                  ,[Longitude]
            FROM [DM_RFDS].[dbo].[Site]
            WHERE [SiteID] Like '{}'
            """.format(siteID)
    results = RFDSconnection.execute(stmt).fetchall()
    site = pd.DataFrame(results)
    site.columns = results[0].keys()
    
    print(site)
    
    pd.DataFrame({'Latitude':[site.Latitude.values[0]],
                  'Longitude':[site.Longitude.values[0]],
                  'Azimuths':Azimuths})[['Latitude','Longitude','Azimuths']].to_clipboard(header=False, index=False, line_terminator='')
    
    input("Press Enter to continue...")    
    
    
    return None

def main():
    query(load_df())
    
if __name__ == "__main__":
    main()