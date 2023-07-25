from abc import abstractmethod
import vertica_python
import psycopg2
import json
import os


class DBConn:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def connect(self):pass

class VerticaDBConn(DBConn):
    def __init__(self) -> None:
        super().__init__()
        self.settingLocation = os.path.join('data', 'dbConn.settings')

    def connect(self):
        # print('Connecting to Vertica')
        with open('data/dbConn.settings', 'r') as f:
            connInfo = json.load(f)
        if('vertica' in connInfo.keys()):
            connInfo = connInfo['vertica']
        else:
            # print('Connection Failed')
            return None
        # print(type(connInfo))
        for key in connInfo.keys():
            # print(connInfo[key])
            if(connInfo[key] in ['True']):
                connInfo[key] = bool(True)
            if(connInfo[key] in ['False']):
                print(key)
                connInfo[key] = bool(False)
        # print(connInfo)
        try:
            conn = vertica_python.connect(**connInfo)
        except Exception as e:
            print(e)
        finally:
            if(conn):
                # print('Connection established')
                # print(type(conn))
                return conn
            else:
                # print('Connection Failed')
                return None
    
class PostgresDBConn(DBConn):
    def __init__(self) -> None:
        super().__init__()
        self.settingLocation = os.path.join('data', 'dbConn.settings')

    def connect(self):
        # print('Connecting to PostgreSQL')
        with open('data/dbConn.settings', 'r') as f:
            connInfo = json.load(f)
        if('postgres' in connInfo.keys()):
            connInfo = connInfo['postgres']
        else:
            # print('Connection Failed')
            return None
        dbname = None
        user = None
        host = None
        password = None
        port = 5432

        for key in connInfo.keys():
            if(key in ['dbname']):
                dbname = connInfo[key]
            elif(key in ['port']):
                port = int(connInfo[key])
            elif(key in ['user']):
                user = connInfo[key]
            elif(key in ['password']):
                password = connInfo[key]
            elif(key in ['host']):
                host = connInfo[key]

        try:
            conn = psycopg2.connect(dbname = dbname, port = port, user = user, password = password, host = host)
        except psycopg2.Error as e:
            print('Connection Failed')
            print(e.pgcode)
            print(e)
            conn = None
        finally:
            if(conn):
                # print('Connection established')
                # print(type(conn))
                return conn
            else:
                # print('Connection Failed')
                return None