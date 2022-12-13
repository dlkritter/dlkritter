from collections import deque
import polars as pl
import pandas as pd
import pyspark.pandas as pspd
import json
import time
from azure.storage.blob import BlobClient
import os
from pyspark.sql import SparkSession
from delta import *


def flattern_json(parentn,d):
    q = deque()
    df = pd.DataFrame([1],columns=['joincol'] )      #initialize with joincol=1 
    dicdf = pd.DataFrame([1],columns=['joincol'] ) #initialize with joincol=1 

    for key, val in d.items(): # This loop push the top most keys and values into queue.
        #print('pushing',parentn,'---',key)
        q.append((key, val))

    key=None
    val =None

    while q:
        kp,vp = q.popleft()
        #print('pulling--',kp)
        if (isinstance(vp, dict)):
            dicdf = flattern_json(parentn+'_'+kp,vp)
            df=pd.merge(df,dicdf ,on='joincol',sort=True)#.drop(['joincol'], axis=1)
        elif (isinstance(vp, list)):
            templst = list()
            tempdf=pd.DataFrame([1],columns=['joincol'] )
            for v in vp:
                #print("kp vp",kp, vp)
                if( isinstance(v,dict)):
                    dicdf = flattern_json(parentn+'_'+kp,v)
                    #newdf=pd.merge(df,dicdf ,on='joincol',sort=True)#.drop(['joincol'], axis=1)
                    templst.append(dicdf)
                else:
                    templst.append(pd.DataFrame([v],columns=[parentn+'_'+kp]  ))
                    #df[parentn+'_'+kp] 

            if ( len(templst)==1):
                if  isinstance( templst[0], pd.DataFrame) :
                    tempdf = templst[0]
                else:
                    tempdf   = pd.DataFrame(templst)
                    tempdf.columns=[parentn+'_'+kp]
            else:
                tempdf=pd.concat(templst)            
            try:
                tempdf['joincol']=1
            except:
                continue

            
            df=pd.merge(df,tempdf ,on='joincol',sort=True)#.drop(['joincol'], axis=1)
            #print('tempdf = ',tempdf.to_string())
            #df[parentn+'_'+kp] = str(vp)
        else:
            if(vp != None):
                df[parentn+'_'+kp] = str(vp)


    #print("final df=",df.to_string())
    #print("*********exiting ", parentn,'*******')

    return df

try:
    #mount_storage(container, storage, mountPoint)
    
    builder = SparkSession.builder.appName("bootStrapBill").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    starttime = time.time()
    time_flatten = []
    time_WriteParq = []
    time_movefiles = []
    total_parquets= []
    time_WriteParqnew = []
    time_movefilesnew = []

    df_files = "/usr/local/spark-3.3.0/jsons/"

    for path in os.listdir(df_files):   #keeping the variable as 'path' as i do not want to disturb the code if i want to revert back
        fullPath = df_files+path
        resbundle_df = pd.read_json(open(fullPath))
        
        #this for loop is to create a dataframe that has only resources 
        df_resources = pd.DataFrame()

    #extract all resources from the entry
        for resentry in resbundle_df["entry"]:
            #get res name and ID
            resName_str = resentry['request']['url']
            if(resName_str != 'foo'):
                #create a dataframe with each resource.
                df_res = None
                df_res = pd.DataFrame(columns=['resName','resdata'])
                df_res['resName'] = [resName_str]
                df_res['resdata'] =   [json.dumps(resentry['resource'])]
                df_resources = pd.concat([df_resources, df_res], axis=0, join='outer')
                df_res = None

        resbundle_df = None  #deallocating the variable is not used any further

    #fetch all 'single domain' resources and create one df/file
        indx = 0
        df_res_toProcess = None
        while len(df_resources) > 0:
            indx += 1
            
            #get the first resource 
            resName_toProcess = df_resources['resName'].iloc[0]

            #fetch all similar resources
            df_res_toProcess = df_resources[df_resources.resName == resName_toProcess]
            df_flatres = pd.DataFrame()

        #build as dataframe of all similar resources
            for resdata in df_res_toProcess['resdata']:
                #load res string as dict
                res = json.loads(resdata)
                fdf = None

                #flatten the resource
                start_flatten = time.time()
                fdf = flattern_json(resName_toProcess,res).drop(['joincol'], axis=1)
                time_flatten.append(time.time() - start_flatten)
                df_flatres = pd.concat([df_flatres, fdf], axis=0, join='outer')
                fdf = None #deallocating the variable is not used any further
            df_res_toProcess = None

            fileName = path.replace('.json', '')
            destfile = resName_toProcess
            dfoutpath = "/usr/local/spark-3.3.0/parquet/"
            dfoutpath = dfoutpath +'/' + resName_toProcess + '/' + fileName + '_' + resName_toProcess


        #write the pandas dataframe to parquet
            start_WriteParq = time.time()
            df_flatres.write_parquet(dfoutpath)
            time_WriteParqnew.append(time.time() - start_WriteParq)
            start_movefiles =time.time()
            time_movefilesnew.append(0)


        #remove all resources that are processed
            df_resources = df_resources[df_resources.resName != resName_toProcess]
            
        total_parquets.append(indx)        
        indx += 1
    delta = spark.readStream.parquet("/usr/local/spark-3.3.0/parquet").maxFilesPerTrigger(4)

    delta.writeStream.format("delta").outputMode("append").start("/usr/local/spark-3.3.0/delta")
    
    print('total Bundles/patients = ',indx)    
    print('total Parquetfiles = ',sum(total_parquets)) 
    print('totaltime = ',time.time() - starttime)
    print('total time to flatten = ',sum(time_flatten))
    print('total time to write Parquet = ',sum(time_WriteParq))
    print('total time to move file s= ',sum(time_movefiles))
    

    print('processing finished')
        

except Exception as e:
    #unmount_storage(mountPoint)
    print('\nError--',e,'----')
    raise
