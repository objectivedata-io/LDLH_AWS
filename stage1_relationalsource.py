

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime
import boto3

glueContext = GlueContext(SparkContext.getOrCreate())

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

#return list of tables to load and loop stage 1 extract for each
glue = boto3.client('glue')
paginator = glue.get_paginator('get_tables')
page_iterator = paginator.paginate(
    DatabaseName='pg_replica'   
)

for page in page_iterator:
    
    tableList = page['TableList']
    
    for tableDict in tableList:
         
        tableName = tableDict['Name']
        runtbl = "etlruns"   
        cols=[]
        for columns in tableDict['StorageDescriptor']['Columns']:
            cols.append(columns['Name'])
            
          
        #set high level tbl run variables
        runstart = datetime.now()
        writePartition = str(runstart)[:10]
        utilityLastRun = str(runstart)
        writePath = "s3://yourpath/"+tableName+"/"+str(writePartition)+"/"
        utilityPath = "s3://yourpath/"+tableName+"/"
        queryCols = ",".join(cols)
            
        #get table last run time into dataframe
        LastRunNode = glueContext.create_dynamic_frame.from_catalog(
            database="utility",
            table_name=runtbl,
            transformation_ctx="LastRunNode",
        )
        
       #initialize lastruntime, date set to '1900' for newly added tables
        SqlQuery0 = """
        SELECT case when  max(lastruntime) is null then '1900-01-01' else
        max(lastruntime) end as lastruntime 
        FROM ETLRUNS
        where partition_0='yourdb' and partition_1='""" +tableName+ "' """"
        
        """
        
        Query_LastRunNode = sparkSqlQuery(
            glueContext,
            query=SqlQuery0,
            mapping={"ETLRUNS":LastRunNode},
            transformation_ctx="Query_LastRunNode",
        )
        
        print(SqlQuery0)
        df_LastRunNode=Query_LastRunNode.toDF()
        
        #create variable with last run
        lastrun_param = df_LastRunNode.select("lastruntime").collect()[0][0]
      
        
        # query postgres and retrieve data
        DeltaNode = glueContext.create_dynamic_frame.from_catalog(
            database="yourdb",
            table_name=tableName,
            transformation_ctx="DeltaNode",
        )
        
        #specify date columns OR script out where clause as needed
        SqlQuery0 = """
        select """+queryCols+""" from REPLICATBL where (date_created > '""" +lastrun_param+ "' or last_updated > '""" +lastrun_param+ "') """"
        
        """
        QueryDeltaNode = sparkSqlQuery(
            glueContext,
            query=SqlQuery0,
            mapping={"REPLICATBL": DeltaNode},
            transformation_ctx="QueryDeltaNode",
        )
        
        print(SqlQuery0)
        df=QueryDeltaNode.toDF()
       
        
        QueryDeltaNodeRepart = QueryDeltaNode.repartition(1)
        
        UtilityNode = glueContext.create_dynamic_frame.from_catalog(
              database="utility",
              table_name=runtbl,
              transformation_ctx="UtilityNode",
        )
          
    
        SqlQuery0 = """
        SELECT '""" +tableName+ "' tbl, '"+utilityLastRun+"' lastruntime """"
     
        """
        
        
        QueryUtilityNode = sparkSqlQuery(
            glueContext,
            query=SqlQuery0,
            mapping={"Utility":UtilityNode},
            transformation_ctx="QueryUtilityNode",
        )

        df=QueryUtilityNode.toDF()
        
        
  ################ LOAD RAW AND UTILITY FILES ############################

       #write delta dataframe to RAW s3
        WriteDeltaNode = glueContext.write_dynamic_frame.from_options(
          frame=QueryDeltaNodeRepart,
          connection_type="s3",
          format="glueparquet",
          connection_options={
              "path": writePath,
              "partitionKeys": [],
          },
          format_options={"compression": "snappy"},
          transformation_ctx="WriteDeltaNode",
        )
       
     # write utility dataframe run time to s3
        WriteUtilityNode = glueContext.write_dynamic_frame.from_options(
          frame=QueryUtilityNode,
          connection_type="s3",
          format="glueparquet",
          connection_options={
              "path": utilityPath,
              "partitionKeys": [],
          },
          format_options={"compression": "snappy"},
          transformation_ctx="WriteUtilityNode",
        )
     
        
