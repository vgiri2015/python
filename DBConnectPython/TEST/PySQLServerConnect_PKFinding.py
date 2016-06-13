
# Install pypyodbc as -- > C:\Python34\Scripts>pip install pypyodbc
# https://code.google.com/p/pyodbc/wiki/GettingStarted


import pypyodbc
import datetime

#This Logic is to Find out the Date and DateTime columns in MAIG tables
#tableslist="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\MAIG\\MAIGTables_List1.txt"
tableslist="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\COGEN\\CogenTables_List1.txt"
columnslist="C:\\SVNCodeBase\\LegacyArchival-DataAcquistition\\dev\\coding\\DataAcquisition\\Sqoop_load_till_Avro\\MAIG\\MAIGDateBinaryColumns.txt"
connstr = pypyodbc.connect('DRIVER={SQL Server};SERVER=N01DSW077.tent.trt.csaa.pri;DATABASE=INS_COGEN;UID=BDW_CAR_MAIG;PWD=maiguser1')
# connstr = pypyodbc.connect('DRIVER={SQL Server Native Client 10.0};SERVER=N01DSW077.tent.trt.csaa.pri;DATABASE=INS_COGEN;UID=BDW_CAR_MAIG;PWD=maiguser1')
cur = connstr.cursor()
input_file = open(tableslist,'r')
output_file=open(columnslist,'w')
for tableName in input_file:
    # sqlStmt='SELECT TOP 2 * FROM {usertable}'.format(usertable=tableName)
    sqlStmt='SELECT TOP 1 * FROM dbo.TA200'
    a=cur.execute(sqlStmt)
    tableSchema=cur.description
    print(a.fetchall())
    print(tableSchema)
    for i in tableSchema:
        columnName=str(i[0])
        columnType=str(i[1])
        # if 'date' in columnType.lower().lstrip().rstrip():
        print(tableName+'.'+columnName+'.'+columnType)
            # output_file.write(tableName+'.'+columnName+'.'+columnType+'\n'+'\n')
input_file.close()
output_file.close()
cur.close()
connstr.close()
'''
#This Logic is to Find out the Primary Keys
#res = cur.execute("SELECT distinct OBJECT_NAME(ic.OBJECT_ID) AS TableName,COL_NAME(ic.OBJECT_ID,ic.column_id) AS ColumnName FROM sys.indexes AS i INNER JOIN sys.index_columns AS ic ON i.OBJECT_ID = ic.OBJECT_ID AND i.index_id = ic.index_id WHERE i.is_primary_key = 1")
#res = res.fetchall()
# f = open(outputfile,'w')
# for r in res:
#     tableName = r[0]
#     primarykey = r[1]
#     f.write(tableName+'.'+primarykey)
#     f.write('\n')
#
f.close()
cur.close()
connstr.close()

#Finding PrimaryKeys with API not through SQLs
'''
'''
import pypyodbc
import datetime

conn = pypyodbc.connect(
    'DRIVER={SQL Server};SERVER=N01DSW077.tent.trt.csaa.pri;DATABASE=INS_AUTO_STAGE2;UID=BDW_CAR_MAIG;PWD=maiguser1')
cur = conn.cursor()

# Execute the SQL and Print rows
res = cur.execute("select top 1 * from dbo.CHR")
x = res.description
#print(x)

fetch = res.fetchall()
#for b in fetch:print(b)

# Get the Primary Keys of a Table. a[3] represents the PrimaryKeys
fetchpk = cur.primaryKeys("AUP")
for a in fetchpk:print(a[3])

# Fetch all Columns of a Table. c[3] represents the column name
#fetchallcolumns = cur.columns("AUP")
#for c in fetchallcolumns:print(c[3])

cur.close()
conn.close()
'''