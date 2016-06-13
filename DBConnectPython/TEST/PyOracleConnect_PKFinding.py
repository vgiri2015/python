# Installed  cx_Oracle Module using this link https://pypi.python.org/pypi/cx_Oracle/5.1.3
# http://stackoverflow.com/questions/20159566/cx-oracle-importerror-dll-load-failed-this-application-has-failed
# select * from v$version;
# http://www.oracle.com/technetwork/topics/winx64soft-089540.html (64 bit)
# http://www.oracle.com/technetwork/topics/winsoft-085727.html (32 bit)
import cx_Oracle

outputfile="P:\Workspace\OracleConnectPython\ContactCenterTables.txt"

#connstr='pasreadonly/pasread4devp1@N01DOL424.tent.trt.csaa.pri/PASDEVP1'
connstr='SVCHDOOP/Had00pCAS01@P01DOL760D.ent.rt.csaa.com:1521/CASPRODDG'

conn = cx_Oracle.connect(connstr)
cur = conn.cursor()
res = cur.execute("SELECT distinct cols.table_name, cols.column_name as PrimaryKey, cols.position, cons.status, cons.owner FROM all_constraints cons, all_cons_columns cols WHERE cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner AND cols.OWNER='CONTACT_CENTER' ORDER BY cols.table_name, cols.position")
res = res.fetchall()
f = open(outputfile,'w')
for r in res:
    tableName = r[0]
    primarykey = r[1]
    f.write(tableName+'.'+primarykey)
    f.write('\n')

f.close()
cur.close()
conn.close()