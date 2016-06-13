# Installed  cx_Oracle Module using this link https://pypi.python.org/pypi/cx_Oracle/5.1.3
# http://stackoverflow.com/questions/20159566/cx-oracle-importerror-dll-load-failed-this-application-has-failed
# select * from v$version;
# http://www.oracle.com/technetwork/topics/winx64soft-089540.html (64 bit)
# http://www.oracle.com/technetwork/topics/winsoft-085727.html (32 bit)
#set PATH = C:\Program Files\Oracle\instantclient-basic-windows.x64-11.2.0.3.0\instantclient_11_2
import cx_Oracle


connstr='SVCHDOOP/Had00pCAS01@P01DOL760D.ent.rt.csaa.com:1521/CASPRODDG'

conn = cx_Oracle.connect(connstr)
cur = conn.cursor()
column_data_types = cur.execute('select * from claim_center.cc_claim')
c = cur.description
getcolumnvalues = []
tablecreation=["CREATE EXTERNAL TABLE CASORC.CC_CLAIM (","  PARTITIONED BY (load_year STRING,load_month STRING,load_date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\\001'  STORED AS ORC;"]
for i in c:
    columnName=str(i[0])
    columnType=str(i[1])
    if 'NUMBER' in columnType:
        columnType="INT"
        getcolumnvalues.append(columnName+' '+columnType)
    elif 'STRING' in columnType:
        columnType="STRING"
        getcolumnvalues.append(columnName+' '+columnType)
    elif 'FIXED_CHAR' in columnType:
        columnType="STRING"
        getcolumnvalues.append(columnName+' '+columnType)
    elif 'TIMESTAMP' in columnType:
        columnType="TIMESTAMP"
        getcolumnvalues.append(columnName+' '+columnType)
    elif 'CLOB' in columnType:
        columnType="STRING"
        getcolumnvalues.append(columnName+' '+columnType)
    elif 'BLOB' in columnType:
        columnType="STRING"
        getcolumnvalues.append(columnName+' '+columnType)
    elif 'VARCHAR' in columnType:
        columnType="STRING"
        getcolumnvalues.append(columnName+' '+columnType)
    elif 'CHAR' in columnType:
        columnType="STRING"
        getcolumnvalues.append(columnName+' '+columnType)
    else:getcolumnvalues.append(columnName+' '+"STRING")

print(tablecreation[0]+','.join(getcolumnvalues)+')'+tablecreation[1])
getcolumnvalues=[]

cur.close()
conn.close()
