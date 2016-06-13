__author__ = 'Varatharajan Giri Ramanathan'

import cx_Oracle

#Declar Input and Output Files
tableslist="C:\SVNCodeBase\LegacyArchival-DataAcquistition\dev\coding\GoldenGate\CAS\orc\CASTableslist.txt"
createHiveStmt="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\GoldenGate\\CAS\\orc\\initial\\ORCDDL_opt2_rev2.hql"
createHiveORCingestion="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\GoldenGate\\CAS\\orc\\initial\\ORCINIT_opt2_rev2.hql"

#Declare Connection Parameter here
connstr='SVCHDOOP/Had00pCAS01@P01DOL760D.ent.rt.csaa.com:1521/CASPRODDG'
conn = cx_Oracle.connect(connstr)
cur = conn.cursor()

#Define the Input and Output Files here
input_file = open(tableslist,'r')
createHiveStmt_file=open(createHiveStmt,'w')
HiveORCingestion_file=open(createHiveORCingestion,'w')

#Decleare arraylist for getting ColumnNames and Types as well as Column Names
getColValTypes = []
getColNames=[]

#Define the Schema Details
hiveAVROSchema="CASLND."
hiveORCSchema="CASSTG."

#Define table Creation Scripts (defaults)
tablecreation=["CREATE EXTERNAL TABLE",
               " PARTITIONED BY (load_year INT,load_month INT,load_date INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",
               "STORED AS ORC LOCATION '/data/raw/oltp/cas/staging/"]
ingestionStmt = ["","","INSERT OVERWRITE TABLE ","PARTITION (LOAD_YEAR,LOAD_MONTH,LOAD_DATE) ",",YEAR(TO_DATE(txtimestamp)),MONTH(TO_DATE(txtimestamp)),DAY(TO_DATE(txtimestamp)) FROM ","dfs -touchz /data/raw/oltp/cas/staging/triggers/"]
gg_columns=["TXOPTYPE","TXTIMESTAMP"]
#from_unixtime(unix_timestamp(updatetime, 'yyyy-MM-dd:HH:mm:ss'))

for tableName in input_file:
    sqlStmt='SELECT * FROM {usertable}'.format(usertable=tableName)
    a=cur.execute(sqlStmt)
    tableSchema=cur.description
    for i in tableSchema:
        columnName=str(i[0])
        columnType=str(i[1])
        if 'NUMBER' in columnType:
            columnType="DOUBLE"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'STRING' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'FIXED_CHAR' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'TIMESTAMP' in columnType:
            columnType="TIMESTAMP"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append("from_unixtime(unix_timestamp("+columnName+','+"'yyyy-MM-dd:HH:mm:ss'))")
        elif 'CLOB' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'BLOB' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'VARCHAR' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'CHAR' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'DATE' in columnType:
            columnType="DATE"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'RAW' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'FLOAT' in columnType:
            columnType="DECIMAL"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'LONG' in columnType:
            columnType="DOUBLE"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        else:
            getColValTypes.append(columnName+' '+"STRING")
            getColNames.append(columnName)
    tableName=tableName.split(sep='.')
    createHiveStmt_file.write(tablecreation[0]+' '+hiveORCSchema+tableName[1]+'('+gg_columns[0]+' STRING,'+gg_columns[1]+' TIMESTAMP,'+','.join(getColValTypes)+')'+tablecreation[1]+r"'\001'"+'\n'+tablecreation[2]+tableName[1]+"';"+'\n'+'\n')
    HiveORCingestion_file.write("echo "+'"'+"Execution Starts for Table Name "+hiveORCSchema+tableName[1]+'"')
    HiveORCingestion_file.write('\n')
    HiveORCingestion_file.write("start_time=$()")
    HiveORCingestion_file.write('\n')
    HiveORCingestion_file.write("hive -e"+'"'+ingestionStmt[0]+ingestionStmt[1]+ingestionStmt[2]+hiveORCSchema+tableName[1]+ingestionStmt[3]+'SELECT '+gg_columns[0]+','+gg_columns[1]+','+','.join(getColNames)+ingestionStmt[4]+hiveAVROSchema+tableName[1]+';'+ingestionStmt[5]+hiveORCSchema+tableName[1]+';'+'"'+'\n'+'\n')
    HiveORCingestion_file.write('\n')
    HiveORCingestion_file.write("echo "+'"'+ "TOTAL COUNT OF TABLE "+hiveORCSchema+tableName[1]+'='+'"'+'`'+"hive -e "+'"'+"SELECT COUNT(*) FROM " +hiveAVROSchema+tableName[1]+'"'+'`'+">>/export/home/svclndg/CASggflume/orc/scripts/initial/option2/InitialLoadCountRpt.txt")
    HiveORCingestion_file.write('\n')
    HiveORCingestion_file.write("end_time=$()")
    HiveORCingestion_file.write('\n')
    HiveORCingestion_file.write("diff=$(($end_time-$start_time))")
    HiveORCingestion_file.write('\n')
    HiveORCingestion_file.write("echo "+'"'+"Time Taken to Create "+hiveORCSchema+tableName[1]+" is = $(($diff / 60 )) minutes"+'"'+">>/export/home/svclndg/CASggflume/orc/scripts/initial/option2/InitialLoadExecRpt.txt")
    HiveORCingestion_file.write('\n')
    getColValTypes=[]
    getColNames=[]

createHiveStmt_file.close()
HiveORCingestion_file.close()
cur.close()
conn.close()