__author__ = 'gfp2ram'

import cx_Oracle

#Declar Input and Output Files
tableslist="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\GoldenGate\\CAS\\orc\\CASTableslist.txt"
createHiveORCingestion="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\GoldenGate\\CAS\\orc\\incremental\\ORCINCRREPO"

#Declare Connection Parameter here
connstr='SVCHDOOP/Had00pCAS01@P01DOL760D.ent.rt.csaa.com:1521/CASPRODDG'
conn = cx_Oracle.connect(connstr)
cur = conn.cursor()

#Define the Input and Output Files here
input_file = open(tableslist,'r')
HiveORCingestion_file=open(createHiveORCingestion,'w')

#Decleare arraylist for getting ColumnNames and Types as well as Column Names
getColValTypes = []
getColNames=[]

#Define the Schema Details
hiveAVROSchema="CASLND."
hiveORCSchema="CASSTG."

#Define table Creation Scripts (defaults)
ingestionStmt = ["INSERT INTO TABLE ","PARTITION (LOAD_YEAR,LOAD_MONTH,LOAD_DATE) ",",YEAR(TO_DATE(txtimestamp)),MONTH(TO_DATE(txtimestamp)),DAY(TO_DATE(txtimestamp)) FROM "]
conditionStmt =[" where CAST(csubstr(txtimestamp,1,16) AS STRING) > '${hivevar:previousBatchTime}'","dfs -rm /data/raw/oltp/cas/staging/triggers/CASSTG.","dfs -touchz /data/raw/oltp/cas/staging/triggers/CASSTG."]
gg_columns=["TXOPTYPE","TXTIMESTAMP"]

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
    HiveORCingestion_file.write("CLAIM_CENTER"+tableName[1]+":::"+conditionStmt[1]+tableName[1]+';'+' '+ingestionStmt[0]+hiveORCSchema+tableName[1]+ingestionStmt[1]+'SELECT '+gg_columns[0]+','+gg_columns[1]+','+','.join(getColNames)+ingestionStmt[2]+hiveAVROSchema+tableName[1]+conditionStmt[0]+';'+conditionStmt[2]+tableName[1]+';'+'\n'+'\n')
    getColValTypes=[]
    getColNames=[]

HiveORCingestion_file.close()
cur.close()
conn.close()
