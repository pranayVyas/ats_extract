ats_extract parses timeline server job history available in /ats/done directory and create 4 csv files
  1) Application Detail
  2) Dag Detail
  3) Dag Additional
  4) Vertices Detail
 
Usage steps
  1) compile the job against hadoop-common version available on your cluster
  2) run the job with below parameters 
      a) INFO/DEBUG/TRACE - log level
      b) STARTTIME - job history with modification date => this date will be extracted
      c) ENDTIME - job history with modification date < this date will be extracted
      d) TRUE/FALSE - true will skip vertices processing and create only 1st 3 csv files. false will skip 1st 3 files and create only vertices csv

for e.g. hadoop jar /opt/grid/7/ats_metrics/ats_extract-0.0.1.jar com.hortonworks.ats_extract.Tezcapture INFO 2018-04-01 2018-04-05 true  
Data can be exported to any db. Structure of the csv files if using mysqldb
  
create table app_detail(
appId VARCHAR(50) NOT NULL PRIMARY KEY UNIQUE KEY,
attId VARCHAR(50),
userId VARCHAR(25),
amLaunchTs DATETIME(3),
amStartTs DATETIME(3),
attCount INTEGER,
containerSize INTEGER);

CREATE TABLE dag_detail(
appId VARCHAR(50),
dagId VARCHAR(50) NOT NULL PRIMARY KEY UNIQUE KEY,
dagName VARCHAR(100),
CallerId VARCHAR(100),
callerType VARCHAR(20),
description LONGTEXT,
dagSubmitTs DATETIME(3),
dagInitTs DATETIME(3),
dagStartTs DATETIME(3),
dagFinishTs DATETIME(3),
queueName VARCHAR (20));


CREATE TABLE dag_addtnl(
dagId VARCHAR(50) NOT NULL PRIMARY KEY UNIQUE KEY,
tableName VARCHAR(200),
recordsWritten BIGINT,
memoryRequestedGB BIGINT,
memoryUsedGB BIGINT,
cpuMilliseconds BIGINT,
succeededTasks INTEGER,
failedTasks INTEGER,
numVertices INTEGER,
hdfsReadOps BIGINT,
hdfsWriteOps BIGINT,
hdfsCreateOps BIGINT,
dagDuration BIGINT,
filesWritten INTEGER)

CREATE TABLE vertices_detail(
vertexName VARCHAR (20),
vertexId VARCHAR (50),
dagId VARCHAR (50),
vertexSucceededTasks INTEGER,
vertexFailedTasks INTEGER,
vertexAvgtaskDuration INTEGER,
vertexMintaskDuration INTEGER,
vertexMaxtaskDuration INTEGER,
vertexRunDuration INTEGER,
vertexStartTs DATETIME(3),
vertexEndTs  DATETIME(3),
vertexStatus VARCHAR (15));
      
  
