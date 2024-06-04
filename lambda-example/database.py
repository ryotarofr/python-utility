import traceback
import psycopg2
import psycopg2.extras


class psqlDBWrapper(object):
    _user = None
    _host = None
    _dataBase = None
    _connection = None

    def __init__(self, host, database, user, password):
        self._host = host
        self._dataBase = database
        self._user = user
        self._password = password

    def Open(self):
        try:
            self._connection = psycopg2.connect(
                user=self._user, password=self._password, host=self._host, database=self._dataBase)
            return (True)
        except Exception:
            traceback.print_exc()
            return (False)

    def Select(self, sqlquery: str, params=None, isAll=False):
        try:
            cursor = self._connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
            print(sqlquery)
            cursor.execute(sqlquery, params)
            if isAll == True:
                results = cursor.fetchall()
                cursor.close()
                return results
            else:
                results = cursor.fetchone()
                cursor.close()
                return results
        except Exception as err:
            self._connection.rollback()
            traceback.print_exc()
            return err

    def Insert(self, sqlquery: str, params=None):
        try:
            cursor = self._connection.cursor()
            print(sqlquery)
            cursor.execute(sqlquery, params)
            # self._connection.commit()
            cursor.close()
            return (True)
        except Exception as err:
            traceback.print_exc()
            return (False)

    def Update(self, sqlquery: str, params=None):
        try:
            print(sqlquery)
            cursor = self._connection.cursor()
            cursor.execute(sqlquery,params)
            result = cursor.rowcount
            # self._connection.commit()
            cursor.close()
            if result == 0:
                return None
            return result
        except Exception:
            traceback.print_exc()
            return None
    
    def Delete(self, sqlquery: str, params=None):
        try:
            print(sqlquery)
            cursor = self._connection.cursor()
            cursor.execute(sqlquery, params)
            result = cursor.rowcount
            # self._connection.commit()
            cursor.close()
            if result == 0:
                return None
            return result
        except Exception:
            traceback.print_exc()
            return None

    def Close(self):
        if self._connection is not None:
            self._connection.close()
    
    def Commit(self):
        try:
            if self._connection is not None:
                self._connection.commit()
                return True
            else:
                print("Connection is not established.")
                return False
        except Exception as err:
            traceback.print_exc()
            return False
    
    def Rollback(self):
        try:
            if self._connection is not None:
                self._connection.rollback()
                return True
            else:
                print("Connection is not established.")
                return False
        except Exception as err:
            traceback.print_exc()
            return False