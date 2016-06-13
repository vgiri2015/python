#C:\Python34\Scripts\pip install ibm_db
import ibm_db

#from pprint import pprint
#pprint (vars(ibm_db))

#print(help(ibm_db))

print(vars(ibm_db))
'''
connstr = ibm_db.connect(
    'Driver={IBM DB2 CLI Driver};'
    'Hostname=192.168.104.130; '
    'Port=8476; '
    #'Protocol=TCPIP; '
    'Database=sispmodd; '
    #'CurrentSchema=schema; '
    'UID=svchdoop; '
    'PWD=SVCHDOOP1;'
)
import ibm_db_dbi

conn=ibm_db_dbi.Connection(connstr)
print(c)
'''

'''Connect to AS400 DB2 -- > This way
import ibm_db, ibm_db_dbi
ibm_db_conn = ibm_db.connect("DRIVER={IBM DB2 CLI DRIVER};DATABASE=mm370lib;HOSTNAME=10.33.xx.x;PORT=446;PROTOCOL=TCPIP;UID=user;PWD=pass;", '', '')
[or]
"Provider=IBMDA400;Data Source=10.33.xx.x;User Id=user;Password=pass;Default Collection=mm370lib;";
'''

