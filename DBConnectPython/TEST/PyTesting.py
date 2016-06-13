import os
import cx_Oracle
import csv

#SQL="SELECT distinct cols.table_name, cols.column_name as PrimaryKey, cols.position, cons.status, cons.owner FROM all_constraints cons, all_cons_columns cols WHERE cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner AND cols.OWNER='PASADM' ORDER BY cols.table_name, cols.position"

#filename="P:\Workspace\OracleConnectPython\Output.csv"
#FILE=open(filename,"w");
#output=csv.writer(FILE, dialect='excel')

#connection = cx_Oracle.connect('pasreadonly/pasread4devp1@N01DOL424.tent.trt.csaa.pri/PASDEVP1')

#cursor = connection.cursor()
#cursor.execute(SQL)
#for row in cursor:
#    output.writerow(row)


#cursor.close()
#connection.close()
#FILE.close()
print("where CAST(concat(substr(txtimestamp,1,10),substr(txtimestamp,12,19)) AS STRING) > "+r"'"+'`date "+%Y-%m-%d%T.000000" -d "1 days ago"`'+r"'"+r'"')
