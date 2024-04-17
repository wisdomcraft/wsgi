import mysql.connector
from config import config


class mysqlLibrary():


    config          = {}
    node            = ''
    nodeList        = []
    connectionDict  = {}


    #构造函数, 初始化
    def __init__(self):
        mysqlconfig = config.get('mysql',None)
        if mysqlconfig == None:
            return {"code":201, 'status':'error', 'data':None, "message":"mysql config not exist in config file, mysqlLibrary.py #21"}
        self.config = mysqlconfig


    #设置节点
    def set(self, setting):
        for key in setting:
            if key == 'node':
                self.node = setting[key]
                if setting[key] not in self.nodeList:
                    self.nodeList.append(setting[key])
                continue
        return self


    #连接数据库服务器端
    def __connect(self):
        node        = self.node
        selfconfig  = self.config
        if len(node) == 0:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, node empty, should set() it, mysqlLibrary.py #42'}
        if selfconfig.__contains__(node) == False:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql parameter is incorrect, mysqlLibrary.py #54'}
        
        connectionDict      = self.connectionDict
        connectionCurrent   = connectionDict.get(node, None)
        if connectionCurrent != None:
            return connectionCurrent

        configCurrent       = selfconfig.get(node, None)
        if configCurrent == None:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql config empty, mysqlLibrary.py #59'}
        
        try:
            connectionCurrent  = mysql.connector.connect(
                host        = configCurrent['host'], 
                user        = configCurrent['user'], 
                password    = configCurrent['password'], 
                database    = configCurrent['database'], 
                port        = configCurrent['port'], 
                connection_timeout = 5, 
                buffered    = True,
                autocommit  = True
            )
            self.connectionDict[node] = connectionCurrent
            return connectionCurrent
        except Exception as error:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql error number {}, error message {}, mysqlLibrary.py #71'.format(error.args[0], error.args[1])}


    #通用的query方法
    def query(self, sql=None):
        if sql == None:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, sql empty in query(), mysqlLibrary.py #81'}
        
        connect = self.__connect()
        cursor  = connect.cursor()
        try:
            cursor.execute(sql)
            return cursor;
        except Exception as error:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql error number {}, error message {}, mysqlLibrary.py #89' . format(error.args[0], error.args[1])}


    #count方法
    def count(self, sql=None):
        if sql == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql empty in count(), mysqlLibrary.py #92'}
        
        if sql.lower().find('select') == -1:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql is not about select, sql: {}, mysqlLibrary.py #96' . format(sql)}
        elif sql.lower().find('count') == -1:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql is not about count, sql: {}, mysqlLibrary.py #98' . format(sql)}
        
        connect = self.__connect()
        if isinstance(connect, dict):
            return connect
        cursor  = connect.cursor()
        try:
            cursor.execute("SET NAMES 'utf8'")
            cursor.execute(sql)
            row = cursor.fetchone()
            data= row[0]
            cursor.close()
            return {'code':200, 'status':'success', 'message':'', 'data':data}
        except Exception as error:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql error number {}, error message {}, {}, mysqlLibrary.py #109' . format(error.args[0], error.args[1], sql)}


    #find方法
    #如果数据存在, data为字典, 否则为None
    def find(self, sql=None, value=None):
        if sql == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql empty in find(), mysqlLibrary.py #121'}
        
        if sql.lower().find('select') == -1:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql is not about select, sql: {}, mysqlLibrary.py #125' . format(sql)}
        
        connect = self.__connect()
        if isinstance(connect, dict):
            return connect
        cursor  = connect.cursor()
        try:
            cursor.execute("SET NAMES 'utf8mb4'")
            if value == None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, value)
            row = cursor.fetchone()
            if row == None:
                return {'code':200, 'status':'success', 'message':'', 'data':None}
            data= dict(zip(cursor.column_names, row))
            cursor.close()
            return {'code':200, 'status':'success', 'message':'', 'data':data}
        except Exception as error:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql error number {}, error message {}, mysqlLibrary.py #144' . format(error.args[0], error.args[1])}


    #select方法
    def select(self, sql):
        if sql == None:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, sql empty in select(), mysqlLibrary.py #150'}

        if sql.lower().find('select') == -1:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql is not about select, sql: {}, mysqlLibrary.py #153' . format(sql)}
        
        connect = self.__connect()
        if isinstance(connect, dict):
            return connect
        cursor  = connect.cursor()

        try:
            cursor.execute("SET NAMES 'utf8mb4'")
            cursor.execute(sql)
            data    = []
            for row in cursor.fetchall():
                data.append(dict(zip(cursor.column_names, row)))
            cursor.close()
            if data == []:
                return {'code':200, 'status':'success', 'message':'', 'data':None}
            return {'code':200, 'status':'success', 'message':'', 'data':data}
        except Exception as error:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql error number {}, error message {}, mysqlLibrary.py #171' . format(error.args[0], error.args[1])}


    #insert方法
    def insert(self, sql=None, value=None):
        if sql == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql empty in insert(), mysqlLibrary.py #177'}
        
        if sql.lower().find('insert') == -1:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql is not about insert, sql: {}, mysqlLibrary.py #180' . format(sql)}
        
        connect = self.__connect()
        if isinstance(connect, dict):
            return connect
        cursor  = connect.cursor()
        
        try:
            cursor.execute("SET NAMES 'utf8mb4'")
            if value == None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, value)
            #connect.commit()
            data = {'insert_id':cursor.lastrowid, 'rowcount':cursor.rowcount}
            return {'code':200, 'status':'success', 'message':'', 'data':data}
        except Exception as error:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql error number {}, error message {}, mysqlLibrary.py #197' . format(error.args[0], error.args[1])}


    #update方法
    def update(self, sql=None, value=None):
        if sql == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql empty in update(), mysqlLibrary.py #203'}
        
        if sql.lower().find('update') == -1:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql is not about update, sql: {}, mysqlLibrary.py #206' . format(sql)}
        
        connect = self.__connect()
        if isinstance(connect, dict):
            return connect
        cursor  = connect.cursor()
        
        try:
            cursor.execute("SET NAMES 'utf8'")
            if value == None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, value)
            #connect.commit()
            data = {'rowcount':cursor.rowcount}
            return {'code':200, 'status':'success', 'message':'', 'data':data}
        except Exception as error:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql error number {}, error message {}, mysqlLibrary.py #223' . format(error.args[0], error.args[1])}


    #delete方法
    def delete(self, sql=None, value=None):
        if sql == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql empty in query(), mysqlLibrary.py #203'}

        if sql.lower().find('delete') == -1:
            return {'code':201, 'status':'error', 'data':None,'message':'error, sql is not about delete, sql: {}, mysqlLibrary.py #206' . format(sql)}

        connect = self.__connect()
        if isinstance(connect, dict):
            return connect
        cursor  = connect.cursor()
        try:
            cursor.execute("SET NAMES 'utf8'")
            if value == None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, value)
            return {'code':200, 'status':'success', 'message':''}
        except Exception as error:
            return {'code':201, 'status':'error', 'data':None, 'message':'error, mysql error number {}, error message {}, mysqlLibrary.py #247' . format(error.args[0], error.args[1])}


    #dict转insert sql
    def dictToInsertSql(self, data=None, argument=None):
        if data == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, data empty in dictToInsertSql(), mysqlLibrary.py #227'}
        if isinstance(data, dict) == False:
            return {'code':201, 'status':'error', 'data':None,'message':'data type is not dict in dictToInsertSql(), mysqlLibrary.py #229'}

        if argument == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, argument empty in dictToInsertSql(), mysqlLibrary.py #232'}
        if isinstance(argument, dict) == False:
            return {'code':201, 'status':'error', 'data':None,'message':'argument type is not dict in dictToInsertSql(), mysqlLibrary.py #234'}

        table   = argument.get('table', None)
        if table == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, table empty in dictToInsertSql() argument, mysqlLibrary.py #238'}
        
        ignore  = argument.get('ignore', None)
        if ignore == True:
            ignore = 'ignore'
        else:
            ignore = ''
        
        keys    = []
        values  = []
        for key,value in data.items():
            keys.append('`' + key + '`')
            if isinstance(value, str)==False and isinstance(value, int)==False and isinstance(value, float)==False and value!=None:
                return {'code':201, 'status':'error', 'data':None,'message':'error, value must be string, int, float or None in dict data in dictToInsertSql(), mysqlLibrary.py #245'}
            if isinstance(value, int) == True:
                value   = str(value)
            if isinstance(value, float) == True:
                value   = str(value)
            if value == None:
                values.append("NULL")
            else:
                value   = value.replace("'", "''")
                values.append("'" + value + "'")

        sql     = 'insert ' + ignore + ' into `' + table + '` (' + ','.join(keys) + ') values (' + ','.join(values) +  ')'
        return {'code':200, 'status':'success', 'message':'', 'data':sql}


    #dict转update sql
    def dictToUpdateSql(self, data=None, argument={}):
        if data == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, data empty in dictToUpdateSql(), mysqlLibrary.py #286'}
        if isinstance(data, dict) == False:
            return {'code':201, 'status':'error', 'data':None,'message':'data type is not dict in dictToUpdateSql(), mysqlLibrary.py #288'}

        if argument == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, argument empty in dictToUpdateSql(), mysqlLibrary.py #291'}
        if isinstance(argument, dict) == False:
            return {'code':201, 'status':'error', 'data':None,'message':'argument type is not dict in dictToUpdateSql(), mysqlLibrary.py #293'}

        table   = argument.get('table', None)
        if table == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, table empty in dictToUpdateSql() argument, mysqlLibrary.py #297'}
        
        column  = argument.get('where_column', None)
        if column == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, where column empty in dictToUpdateSql() argument, mysqlLibrary.py #301'}
        if column not in data:
            return {'code':201, 'status':'error', 'data':None,'message':'error, where column not in dict data in dictToUpdateSql() argument, mysqlLibrary.py #303'}
        where   = "`{}`='{}'" . format(column, data.pop(column))
        
        #接下来, 组织update语句
        if len(data) == 0:
            return {'code':201, 'status':'error', 'data':None,'message':'error, data too less in dictToUpdateSql() argument, mysqlLibrary.py #309'}
        row     = []
        for key,value in data.items():
            if isinstance(value, str)==False and isinstance(value, int)==False and isinstance(value, float)==False:
                return {'code':201, 'status':'error', 'data':None,'message':'error, value must be string, int or float in dict data in dictToUpdateSql(), mysqlLibrary.py #312'}
            if isinstance(value, int)==True or isinstance(value, float)==True:
                value   = str(value)
            value       = value.replace("'", "''")
            row.append("`{}`='{}'" . format(key, value))
        
        sql    = "update `{}` set {} where {}" . format(table, ','.join(row), where)
        return {'code':200, 'status':'success', 'message':'', 'data':sql}


    #list转insert sql
    def multipleListToInsertSql(self, data=None, argument=None):
        if data == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, data empty in multipleListToInsertSql(), mysqlLibrary.py #328'}
        if isinstance(data, list) == False:
            return {'code':201, 'status':'error', 'data':None,'message':'data type is not list in multipleListToInsertSql(), mysqlLibrary.py #330'}
        if len(data) == 0:
            return {'code':201, 'status':'error', 'data':None,'message':'data type is list but length zero in multipleListToInsertSql(), mysqlLibrary.py #332'}

        if argument == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, argument empty in multipleListToInsertSql(), mysqlLibrary.py #335'}
        if isinstance(argument, dict) == False:
            return {'code':201, 'status':'error', 'data':None,'message':'argument type is not dict in multipleListToInsertSql(), mysqlLibrary.py #337'}

        table   = argument.get('table', None)
        if table == None:
            return {'code':201, 'status':'error', 'data':None,'message':'error, table empty in multipleListToInsertSql() argument, mysqlLibrary.py #341'}

        ignore  = argument.get('ignore', None)
        if ignore == True:
            ignore = 'ignore'
        else:
            ignore = ''

        keys_list    = []
        for key in data[0]:
            keys_list.append('`' + key + '`')
            del key

        values_list = []
        for i in range(0, len(data)):
            line    = []
            for value in data[i].values():
                if isinstance(value, str)==False and isinstance(value, int)==False and isinstance(value, float)==False  and value!=None:
                    return {'code':201, 'status':'error', 'data':None,'message':'error, value must be string or int in dict data in multipleListToInsertSql(), mysqlLibrary.py #359'}
                if isinstance(value, int) == True:
                    value   = str(value)
                if isinstance(value, float) == True:
                    value   = str(value)
                if value == None:
                    values.append("NULL")
                else:
                    value   = value.replace("'", "''")
                    line.append("'" + value + "'")
                del value
            values_list.append( '(' + ','.join(line) + ')' )
            del line

        sql     = 'insert %s into `%s` (%s) values %s' % (ignore, table, ','.join(keys_list), ','.join(values_list))
        return {'code':200, 'status':'success', 'message':'', 'data':sql}


    #析构方法
    def __del__(self):
        nodeList = self.nodeList
        if len(nodeList) == 0:
            return None
        connectionDict = self.connectionDict
        for node in nodeList:
            connection = connectionDict.get(node, None)
            if connection != None:
                connection.close()
                connection = None
                self.connectionDict[node] = None
        
        self.nodeList       = []
        self.connectionDict = {}








