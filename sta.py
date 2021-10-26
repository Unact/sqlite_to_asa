import sqlite3
import sqlanydb
from tendo import singleton
import os
import logging

os.environ["SQLANY_API_DLL"] = "/Applications/SQLAnywhere16/System/lib64/libdbcapi_r.dylib"
logging.basicConfig(level=logging.INFO)
adm_name = 'adm00'
db_path = '/Users/shelter/temp/adm/'
asa_db_pwd_file = '/Users/shelter/temp/adm/sta.pwd'

bagTable_path = db_path+'Bag.db'
clientsTable_path = db_path+'Clients.db'
CollectionTable_path = db_path+'Collection.db'
CycleRatingTable_path = db_path+'CycleRating.db'
InfoTable_path = db_path+'Info.db'
PrinterTable_path = db_path+'Printer.db'
ReceiverTable_path = db_path+'Receiver.db'
SensorTable_path = db_path+'Sensor.db'
SessionRatingTable_path = db_path+'SessionRating.db'
TransactionTable_path = db_path+'Transaction.db'
UpsTable_path = db_path+'Ups.db'
UsersTable_path = db_path+'Users.db'
WorkingInfoTable_path = db_path+'WorkingInfo.db'
#Table_path = db_path+''

def sync_Table(dbname, table_name, row_names, row_count):
#   table_name = 'clientstable'
#    row_names = '[login], [password], cardNumber, cardholderName'
#    row_q = '?, ?, ?, ?' +', ?, ?, ?'
#    row_q = row_q_ + ' ?, ?, ?'
    logging.warning('--- start sync_Table('+dbname+', '+table_name+', '+row_names+', '+str(row_count)+') ---')
    _row_q = ''
    for x in range(row_count):
      _row_q = _row_q+'?,'
    row_q = _row_q + ' ?, ?, ?'
    cur_asa = con_asa.cursor()
    sql = 'select isnull(max(id),-1) from adm.'+table_name+';'
    cur_asa.execute(sql)
    i = cur_asa.fetchone()
    logging.info('max(id)='+str(i))
    cur_asa.execute('select id from adm.device where name = ?;', (adm_name,))
    device_id = cur_asa.fetchone()
    logging.info('device_id='+str(device_id))
    con_sqlite = sqlite3.connect(dbname)
    cur_sqlite = con_sqlite.cursor()
    sql = ('select '+row_names+', '
           +str(device_id[0])+', id, timestamp from '+table_name
           +' where id>'+str(i[0]))
    logging.info(sql)
    cur_sqlite.execute(sql)
    rows = cur_sqlite.fetchall()
    for row in rows:
        sql = ('insert into adm.'+table_name+'('+row_names+', device, id, ts) '
               'values('+row_q+')')
        logging.info(sql)

        cur_asa.execute(sql, row)
    cur_asa.close()
    con_asa.commit()
    logging.warning('--- end sync_Table(' + dbname + ', ' + table_name + ', ' + row_names + ', ' + str(row_count) + ') ---')

me = singleton.SingleInstance() # will sys.exit(-1) if other instance is running

#con_sqlite = sqlite3.connect('/Users/shelter/temp/adm/Bag.db')
#sql_fetch(con_sqlite)
f = open(asa_db_pwd_file, "r")
from_file_pwd = ''
from_file_pwd = f.readline().replace('\n', '')
f.close()

con_asa = sqlanydb.connect(uid='sta', pwd=from_file_pwd, host='dcvsrv07.unact.ru', dbn='adm', servername='dcvsrv07' )
sync_Table(bagTable_path, 'bagtable', 'deviceType, deviceModel, state, errorNumber, description', 5)
sync_Table(clientsTable_path, 'clientstable', '[login], [password], cardNumber, cardholderName', 4)
sync_Table(CollectionTable_path, 'collectiontable', 'cycleId, cycleState, loadingDateTime, unloadingDateTime,'
           'physicalBagNumber, logicalBagNumber, cashCount, clientsCount, operationsCanceled, operationsUnfinished', 10)
sync_Table(CycleRatingTable_path, 'cycleratingtable', 'rating, literalCode, [count]', 3)
sync_Table(InfoTable_path, 'infotable', 'bankName, bankNumber, bankAddress, bankPhone, admNumber, admType,admManufacturer, admModel',8)
sync_Table(PrinterTable_path, 'printertable', 'deviceType, deviceModel, state, errorNumber, description', 5)
sync_Table(ReceiverTable_path, 'receivertable', 'deviceType, deviceModel, state, errorNumber, description', 5)
sync_Table(SensorTable_path, 'sensortable', 'deviceType, deviceModel, state, errorNumber, description', 5)
sync_Table(SessionRatingTable_path, 'sessionratingtable', 'rating, literalCode, [count]', 3)
sync_Table(TransactionTable_path, 'transaсtiontable', 'sessionId, sessionNumber, operationId, sessionOpenDateTime,'
          'operationName, cycleId, cashierId, cashierFullName, client, contractNumber, accountNumber, accountName,'
          'paymentSymbolCode, paymentSymbolName, paymentId, rating, amount, currencyCode, reasonFail, state', 20)
sync_Table(UpsTable_path,'upstable', 'deviceType, deviceModel, state, errorNumber, description', 5)
sync_Table(UsersTable_path,'userstable', '[login], [password], role, rfid', 4)
sync_Table(WorkingInfoTable_path,'workinginfotable', 'cashCount, clientsCount, operationsCanceled, operationsUnfinished, '
            'loadingDateTime, physicalBagNumber, logicalBagNumber, openedOperationCycle, sessionNumber, operationId, '
            'workMode, errorText, serviceMode', 13)
con_asa.commit()


