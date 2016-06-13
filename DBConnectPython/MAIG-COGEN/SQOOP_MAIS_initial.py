__author__ = 'Varatharajan Giri Ramanathan'

import pypyodbc
import datetime

# Declar Input and Output Files

# tableslist = "C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\MAIG\\Auto_Tablelist_Delta.txt"
# createHiveStmt = "C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\MAIG\\initial\\orc\\ORCDDL_MAIG_Delta.hql"
# createHiveORCingestion = "C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\MAIG\\initial\\orc\\ORCINIT_MAIG_Delta.sh"

tableslist = "C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\COGEN\\Cogen_Tablelist_Delta.txt"
createHiveStmt = "C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\COGEN\\initial\\orc\\ORCDDL_COGEN_Delta.hql"
createHiveORCingestion = "C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\COGEN\\initial\\orc\\ORCINIT_COGEN_Delta.sh"

# Declare Connection Parameter here
# connstr = pypyodbc.connect('DRIVER={SQL Server};SERVER=N01DSW077.tent.trt.csaa.pri;DATABASE=INS_AUTO_DELTA;UID=BDW_CAR_MAIG;PWD=maiguser1')
connstr = pypyodbc.connect('DRIVER={SQL Server};SERVER=N01DSW077.tent.trt.csaa.pri;DATABASE=INS_COGEN_DELTA;UID=BDW_CAR_MAIG;PWD=maiguser1')

cur = connstr.cursor()
print(cur)

# Define the Input and Output Files here
input_file = open(tableslist, 'r')
createHiveStmt_file = open(createHiveStmt, 'w')
HiveORCingestion_file = open(createHiveORCingestion, 'w')

# Decleare arraylist for getting ColumnNames and Types as well as Column Names
getColValTypes = []
getColNames = []

# Define the Schema Details
# hiveAVROSchema="MAIGLND."
# hiveORCSchema="MAIGSTG."

hiveAVROSchema = "COGENLND."
hiveORCSchema = "COGENSTG."


# Define table Creation Scripts (defaults)
tablecreation = ["CREATE EXTERNAL TABLE",
                 " PARTITIONED BY (load_year INT,load_month INT,load_date INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",
                 "  STORED AS ORC LOCATION '/data/raw/oltp/cogen/staging/"]
ingestionStmt = [
    "SET tez.queue.name=default;SET hive.exec.dynamic.partition = true;SET hive.exec.dynamic.partition.mode=nonstrict;SET hive.execution.engine=tez;SET mapreduce.framework.name=yarn-tez;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;",
    "INSERT OVERWRITE TABLE ", " PARTITION (LOAD_YEAR,LOAD_MONTH,LOAD_DATE) ", ",LOAD_YEAR,LOAD_MONTH,LOAD_DATE from "]

for tableName in input_file:
    sqlStmt = 'SELECT TOP 2 * FROM {usertable}'.format(usertable=tableName)
    print(tableName)
    a = cur.execute(sqlStmt)
    tableSchema = cur.description
    for i in tableSchema:
        columnName = str(i[0])
        # columnType = str(i[1])
        columnType = str(i)
        print(columnType)
        if 'int' in columnType.lower():
            columnType = "INT"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'decimal' in columnType.lower():
            columnType = "DECIMAL(18,3)"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'timestamp' in columnType.lower():
            columnType = "TIMESTAMP"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append("from_unixtime(unix_timestamp(" + columnName + ',' + "'yyyy-MM-dd HH:mm:ss'))")
        elif 'varchar' in columnType.lower():
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'binary' in columnType.lower():
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'datetime' in columnType.lower():
            columnType = "TIMESTAMP"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append("from_unixtime(unix_timestamp(" + columnName + ',' + "'yyyy-MM-dd HH:mm:ss'))")
        elif 'time' in columnType.lower():
            columnType = "TIMESTAMP"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append("from_unixtime(unix_timestamp(" + columnName + ',' + "'yyyy-MM-dd HH:mm:ss'))")
        elif 'datetime2' in columnType.lower():
            columnType = "TIMESTAMP"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append("from_unixtime(unix_timestamp(" + columnName + ',' + "'yyyy-MM-dd HH:mm:ss'))")
        elif 'date' in columnType.lower():
            columnType = "DATE"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'char' in columnType.lower():
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'float' in columnType.lower():
            columnType = "DECIMAL(18,3)"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'long' in columnType.lower():
            columnType = "DOUBLE"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        else:
            getColValTypes.append(columnName + ' ' + "STRING")
            getColNames.append(columnName)
    tableName = tableName.split(sep='.')
    queryString=[]
    queryString.append("echo "+'"'+"Execution Starts for Table Name "+hiveORCSchema+tableName[1]+'"')
    queryString.append('\n')
    queryString.append("start_time=$()")
    queryString.append('\n')
    queryString.append("hive -e"+' '+'"'+ingestionStmt[0] + ' ' + ingestionStmt[1] + hiveORCSchema + tableName[1] + ingestionStmt[2] + 'SELECT ' + ','.join(getColNames) + ingestionStmt[3] + hiveAVROSchema + tableName[1] +'"'+';')
    queryString.append('\n')
    queryString.append("end_time=$()")
    queryString.append('\n')
    queryString.append("diff=$(($end_time-$start_time))")
    queryString.append('\n')
    queryString.append("echo "+'"'+"Time Taken to Create "+hiveORCSchema+tableName[1]+" is = $(($diff / 60 )) minutes"+'"'+">>/opt/md/WIP/giri/Cogen_Static_InitialLoadExecRpt.txt")
    queryString.append('\n')
    s=''.join(queryString)
    createHiveStmt_file.write(tablecreation[0] + ' ' + hiveORCSchema + tableName[1] + '(' + ','.join(getColValTypes) + ')' + tablecreation[1] + r"'\001'" + tablecreation[2] + tableName[1] + "'" + ';' + '\n' + '\n')
    # HiveORCingestion_file.write(ingestionStmt[0] + ' ' + ingestionStmt[1] + hiveORCSchema + tableName[0] + ingestionStmt[2] + 'SELECT ' + ','.join(getColNames) + ingestionStmt[3] + hiveAVROSchema + tableName[0] + ';' + '\n' + '\n')
    HiveORCingestion_file.write(s)
    getColValTypes = []
    getColNames = []

createHiveStmt_file.close()
HiveORCingestion_file.close()
cur.close()
connstr.close()
