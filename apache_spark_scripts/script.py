from collections import deque
import polars as pl
import pandas as pd
import json
import time
import os
import pyspark
from pyspark import SparkSession


def flattern_json(parentn,d):
    q = deque()
    df =pl.DataFrame([1],columns=['joincol'] ) 
    dicdf = pl.DataFrame([1],columns=['joincol'] ) 

    for key, val in d.items(): 
        q.append((key, val))

    key=None
    val =None

    while q:
        kp,vp = q.popleft()

        if (isinstance(vp, dict)):
            dicdf = flattern_json(parentn+'_'+kp,vp)
            df=pl.DataFrame.join(df,dicdf ,on='joincol',sort=True)
        elif (isinstance(vp, list)):
            templst = list()
            tempdf=pl.DataFrame([1],columns=['joincol'] )
            for v in vp:

                if( isinstance(v,dict)):
                    dicdf = flattern_json(parentn+'_'+kp,v)

                    templst.append(dicdf)
                else:
                    templst.append(pl.DataFrame([v],columns=[parentn+'_'+kp]  ))

            if ( len(templst)==1):
                if  isinstance( templst[0], pl.DataFrame) :
                    tempdf = templst[0]
                else:
                    tempdf   = pl.DataFrame(templst)
                    tempdf.columns=[parentn+'_'+kp]
            else:
                tempdf=pl.concat([templst, templst])            
            try:
                tempdf['joincol']=1
            except:
                continue

            
            df=pl.DataFrame.join(df,tempdf ,on='joincol',sort=True)

        else:
            if(vp != None):
                df[parentn+'_'+kp] = str(vp)



    return df


try:

    spark = SparkSession.builder.appName("bootStrapBill").getOrCreate()

    starttime = time.time()
    time_flatten = []
    time_WriteParq = []
    time_movefiles = []
    total_parquets= []
    time_WriteParqnew = []
    time_movefilesnew = []

    df_files = r'/usr/local/spark/jsons/'

    for path in os.listdir(df_files):   
        resbundle_df = None
        resbundle_df = pd.read_json(open(df_files + path))
        
 
        df_resources = pl.DataFrame()


        for resentry in resbundle_df["entry"]:
            resName_str = resentry['request']['url']
            if(resName_str != 'Patient' and resName_str != 'Encounter'):
                df_res = None
                df_res = pl.DataFrame(columns=['resName','resdata'])
                df_res['resName'] = [resName_str]
                df_res['resdata'] =   [json.dumps(resentry['resource'])]
                df_resources = pl.concat([df_resources, df_res])
                df_res = None

        resbundle_df = None  
        indx = 0
        df_res_toProcess = None
        while len(df_resources) > 0:
            indx += 1
            
            resName_toProcess = df_resources['resName'].iloc[0]

            df_res_toProcess = df_resources[df_resources.resName == resName_toProcess]
            df_flatres = pl.DataFrame()

            for resdata in df_res_toProcess['resdata']:

                res = json.loads(resdata)
                fdf = None

                start_flatten = time.time()
                fdf = flattern_json(resName_toProcess,res).drop(['joincol'], axis=1)
                time_flatten.append(time.time() - start_flatten)
                df_flatres = pl.concat([df_flatres, fdf], axis=0, join='outer')
                fdf = None 
            df_res_toProcess = None

            destfile = resName_toProcess+'.parquet'
            dfoutpath = r"/usr/local/spark/parquet" + destfile

            start_WriteParq = time.time()
            df_flatres.to_parquet(dfoutpath)
            time_WriteParqnew.append(time.time() - start_WriteParq)
            start_movefiles =time.time()
            time_movefilesnew.append(0)


            df_resources = df_resources[df_resources.resName != resName_toProcess]
            
        total_parquets.append(indx)        
        indx += 1

    print('total Bundles/patients = ',indx)    
    print('total Parquetfiles = ',sum(total_parquets)) 
    print('totaltime = ',time.time() - starttime)
    print('total time to flatten = ',sum(time_flatten))
    print('total time to write Parquet = ',sum(time_WriteParq))
    print('total time to move file s= ',sum(time_movefiles))
    

    print('processing finished')

    spark.stop    

except Exception as e:
    print('\nError--',e,'----')
    raise
