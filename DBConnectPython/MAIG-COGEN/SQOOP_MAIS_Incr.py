__author__ = 'gfp2ram'

__author__ = 'Varatharajan Giri Ramanathan'

import pypyodbc
import datetime

#Declar Input and Output Files
# tableslist="C:\\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\MAIG\\MAIGTables_List1.txt"
# createHiveORCingestion="C:\\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\MAIG\\initial\\MAIGCreateHiveORCingestionStmt_List1.hql"

tableslist="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\COGEN\\COGENTables_List2.txt"
createHiveORCingestion="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\COGEN\\incremental\\orc\\ORCINR_COGEN_List2.hql"


#Declare Connection Parameter here
# connstr = pypyodbc.connect('DRIVER={SQL Server};SERVER=N01DSW077.tent.trt.csaa.pri;DATABASE=INS_AUTO_STAGE2;UID=BDW_CAR_MAIG;PWD=maiguser1')
connstr = pypyodbc.connect('DRIVER={SQL Server};SERVER=N01DSW077.tent.trt.csaa.pri;DATABASE=INS_COGEN;UID=BDW_CAR_MAIG;PWD=maiguser1')

cur = connstr.cursor()
print(cur)

#Define the Input and Output Files here
input_file = open(tableslist,'r')
HiveORCingestion_file=open(createHiveORCingestion,'w')

#Decleare arraylist for getting ColumnNames and Types as well as Column Names
getColNames=[]
getColValTypes=[]

#Define the Schema Details
# hiveAVROSchema="MAIGLND."
# hiveORCSchema="MAIGSTG."

hiveAVROSchema="COGENLND."
hiveORCSchema="COGENSTG."


#Define table Creation Scripts (defaults)
ingestionStmt = ["SET tez.queue.name=default;SET hive.exec.dynamic.partition = true;SET hive.exec.dynamic.partition.mode=nonstrict;SET hive.execution.engine=tez;SET mapreduce.framework.name=yarn-tez;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;","INSERT INTO TABLE ","PARTITION (LOAD_YEAR,LOAD_MONTH,LOAD_DATE) ",",LOAD_YEAR,LOAD_MONTH,LOAD_DATE from "," where load_year=`date +%Y` and load_month=`date +%m` and load_date=`date +%d`"]

for tableName in input_file:
    sqlStmt='SELECT TOP 2 * FROM {usertable}'.format(usertable=tableName)
    print(tableName)
    a=cur.execute(sqlStmt)
    tableSchema=cur.description
    for i in tableSchema:
        columnName=str(i[0])
        columnType=str(i[1])
        if 'int' in columnType.lower():
            columnType="INT"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'decimal' in columnType.lower():
            columnType="DECIMAL(18,3)"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'timestamp' in columnType.lower():
            columnType="TIMESTAMP"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append("from_unixtime(unix_timestamp("+columnName+','+"'yyyy-MM-dd HH:mm:ss'))")
        elif 'varchar' in columnType.lower():
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'binary' in columnType.lower():
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'datetime' in columnType.lower():
            columnType="TIMESTAMP"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append("from_unixtime(unix_timestamp("+columnName+','+"'yyyy-MM-dd HH:mm:ss'))")
        elif 'date' in columnType.lower():
            columnType="DATE"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'char' in columnType.lower():
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'float' in columnType.lower():
            columnType="DECIMAL(18,3)"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'long' in columnType.lower():
            columnType="DOUBLE"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        else:
            getColValTypes.append(columnName+' '+"STRING")
            getColNames.append(columnName)
    tableName=tableName.split(sep='.')
    hiveAVROSchema1=hiveAVROSchema.split(sep='.')
    HiveORCingestion_file.write("USE "+hiveAVROSchema1[0]+';'+"ALTER TABLE "+tableName[1]+"ADD PARTITION(load_year='`date +%Y`',load_month='`date +%m`',load_date='`date +%d`');"+'\n'+ingestionStmt[0]+' '+ingestionStmt[1]+hiveORCSchema+tableName[1]+ingestionStmt[2]+'SELECT '+','.join(getColNames)+ingestionStmt[3]+hiveAVROSchema+tableName[1]+ingestionStmt[4]+';'+'\n'+'\n')
    getColValTypes=[]
    getColNames=[]

HiveORCingestion_file.close()
cur.close()
connstr.close()