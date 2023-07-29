from typing import List, Tuple, Union
from itertools import combinations
import pandas as pd

import json
import vertica_python
import os
import sqlite3
import re


from utils.Singleton import SingletonMeta
from data.Table import Table
from webtableindexer.Tokenizer import Tokenizer
from data.DBConn import PostgresDBConn, VerticaDBConn

class QueryGenerator():
    def __init__(self, XList: Union[List, Tuple], Y: List, tau: int = 2) -> None:
        self.XList = XList
        self.Y = Y
        self.tau = tau

class DXFQueryGenerator(QueryGenerator):
    def __init__(self, XList: Union[List, Tuple], Y: List, tau: int = 2) -> None:
        super().__init__(XList, Y, tau)

    def __colQueryStrGen__(self, qid: int) -> str:
        __qString__ = """
                      (SELECT tableid, colid 
                       FROM main_tokenized mt{}
                       WHERE tokenized IN {}
                       GROUP BY (tableid, colid) 
                       HAVING COUNT(DISTINCT tokenized) >= {}) AS colX{}
                      """
        
        valList = self.XList
        tau = self.tau
        
        if(len(valList) == 0):
            valString = "()"
        elif(len(valList) == 1):
            if(type(valList) is tuple or type(valList) is list):
                valString = "(" + str(valList[0]) + ")"
            elif(type(valList) is str):
                valString = "(" + str(valList) + ")"
        else:
            if(type(valList) is tuple or type(valList) is list):
                valString = str(tuple(valList))

        if(len(valList) < tau):
            tauval = len(valList)
        else:
            tauval = tau

        __qString__ = __qString__.format(qid, valString, tauval, qid)

        return __qString__
    
    def __YStringGen__(self) -> str:
        __qString__ = """
                      SELECT colY.tableid FROM (
                      SELECT tableid, colid 
                      FROM main_tokenized mty
                      WHERE tokenized IN {}
                      GROUP BY (tableid, colid) 
                      HAVING COUNT(DISTINCT tokenized) >= {}) AS colY
                      """
        Y = self.Y
        tau = self.tau

        if(len(Y) == 0):
            valString = "()"
        elif(len(Y) == 1):
            if(type(Y) is tuple or type(Y) is list):
                valString = "(" + str(Y[0]) + ")"
            elif(type(Y) is str):
                valString = "(" + str(Y) + ")"
        else:
            if(type(Y) is tuple or type(Y) is list):
                valString = str(tuple(Y))

        if(len(Y) < tau):
            tauval = len(Y)
        else:
            tauval = tau

        __qString__ = __qString__.format(valString, tauval)

        return __qString__
    
    def __whereClauseGen__ (self, numcols: int) -> Union[str, None]:
        __template_tableid__ = "colX1.tableid = colX{}.tableid"
        __template_colid__ = "colX{}.colid <> colX{}.colid"
        __template_colid_y__ = "colX{}.colid <> colY.colid"

        # if(numcols == 1):
        #     return None
        
        tableidList = list()
        colidList = list()
        for i in range(numcols):
            if(i + 1 == 1):
                continue
            tableidList.append(__template_tableid__.format(str(i + 1)))
        
        colids = [i + 1 for i in range(numcols)]
        for comb in list(combinations(colids, 2)):
            colidList.append(__template_colid__.format(comb[0], comb[1]))

        colyList = list()
        for i in colids:
            colyList.append(__template_colid_y__.format(str(i)))
        
        whereClause = " AND ".join(tableidList)
        whereClause += " AND colX1.tableid = colY.tableid"
        whereClause += " AND \n"
        whereClause += " AND ".join(colidList)
        whereClause += " AND ".join(colyList)

        return whereClause


    def __queryStringGen__(self) -> str:
        """
        Generating SQL query string.

        SELECT colX1.tableid FROM
	        (SELECT tableid, colid FROM main_tokenized mt WHERE tokenized IN ('emil adolf von behring') GROUP BY (tableid, colid) HAVING COUNT(DISTINCT tokenized) > 0) AS colX1,
	        (SELECT tableid, colid FROM main_tokenized mt2 WHERE tokenized IN ('1901') GROUP BY (tableid, colid) HAVING COUNT(DISTINCT tokenized) > 0) AS colX2,
            (SELECT tableid FROM main_tokenized mt3 WHERE tokenized IN ('medicine') GROUP BY (tableid, colid) HAVING COUNT(DISTINCT tokenized) > 0) AS colY
        WHERE 
	        colX1.tableid = colX2.tableid AND colX1.tableid = colY.tableid
	        colX1.colid <> colX2.colid AND colX1.colid <> colY.colid AND colX2.colid <> colY.colid
        

        Params:
            XList (list of List): list of X
            Y (list): list of Y
        Return:
            qString (str): final query string
        """

        XList = self.XList
        Y = self.Y
        tau = self.tau

        Xlist_t = [list(col) for col in zip(*XList)]
        numcols = len(Xlist_t)

        __querystring__ = "SELECT colX1.tableid FROM \n"
        for i, col in enumerate(Xlist_t):
            if(i > 0):
                __querystring__ += ',\n'
            __colstring__ = self.__colQueryStrGen__(i + 1)
            __querystring__ += '\t'
            __querystring__ += __colstring__
        __querystring__ += ',\n\t'
        __Ystring__ = self.__YStringGen__()
        __querystring__ += __Ystring__
        
        __wherestring__ = self.__whereClauseGen__(numcols)
        # print(__wherestring__)

        if(__wherestring__):
            __querystring__ += '\n WHERE \n'
            __querystring__ += __wherestring__
        
        # __querystring__ += "\n UNION \n"

        

        

        return __querystring__


    def getQueryString(self) -> str:
        # XList = self.XList
        # Y = self.Y
        # tau = self.tau
        querystring = self.__queryStringGen__()
        querystring = querystring.replace("\n", "")
        querystring = querystring.replace("\t", "")

        return querystring
    
    def getQueryString_format(self) -> str:
        # XList = self.XList
        # Y = self.Y
        # tau = self.tau
        querystring = self.__queryStringGen__()
        return querystring

    
class L1QueryGenerator(QueryGenerator):
    def __init__(self, XList: Union[List, Tuple], Y: List, tau: int = 2) -> None:
        super().__init__(XList, Y, tau)

    def __colQueryStrGen__(self, qid: int) -> str:
        __qString__ = """
                      (SELECT tableid, colid 
                       FROM main_tokenized mt{}
                       WHERE tokenized IN {}
                       GROUP BY (tableid, colid) 
                       HAVING COUNT(DISTINCT tokenized) >= {}) AS colX{}
                      """
        
        valList = self.XList
        tau = self.tau
        
        if(len(valList) == 0):
            valString = "()"
        elif(len(valList) == 1):
            if(type(valList) is tuple or type(valList) is list):
                valString = "(" + str(valList[0]) + ")"
            elif(type(valList) is str):
                valString = "(" + str(valList) + ")"
        else:
            if(type(valList) is tuple or type(valList) is list):
                valString = str(tuple(valList))

        if(len(valList) < tau):
            tauval = len(valList)
        else:
            tauval = tau

        __qString__ = __qString__.format(qid, valString, tauval, qid)

        return __qString__
    
    def __YStringGen__(self) -> str:
        __qString__ = """
                      SELECT colY.tableid FROM (
                      SELECT tableid, colid 
                      FROM main_tokenized mty
                      WHERE tokenized IN {}
                      GROUP BY (tableid, colid) 
                      HAVING COUNT(DISTINCT tokenized) >= {}) AS colY
                      """
        Y = self.Y
        tau = self.tau

        if(len(Y) == 0):
            valString = "()"
        elif(len(Y) == 1):
            if(type(Y) is tuple or type(Y) is list):
                valString = "(" + str(Y[0]) + ")"
            elif(type(Y) is str):
                valString = "(" + str(Y) + ")"
        else:
            if(type(Y) is tuple or type(Y) is list):
                valString = str(tuple(Y))

        if(len(Y) < tau):
            tauval = len(Y)
        else:
            tauval = tau

        __qString__ = __qString__.format(valString, tauval)

        return __qString__
    
    def __whereClauseGen__ (self, numcols: int) -> Union[str, None]:
        __template_tableid__ = "colX1.tableid = colX{}.tableid"
        __template_colid__ = "colX{}.colid <> colX{}.colid"

        if(numcols == 1):
            return None
        
        tableidList = list()
        colidList = list()
        for i in range(numcols):
            if(i + 1 == 1):
                continue
            tableidList.append(__template_tableid__.format(str(i + 1)))
        
        colids = [i + 1 for i in range(numcols)]
        for comb in list(combinations(colids, 2)):
            colidList.append(__template_colid__.format(comb[0], comb[1]))
        
        whereClause = " AND ".join(tableidList)
        whereClause += " AND \n"
        whereClause += " AND ".join(colidList)

        return whereClause


    def __queryStringGen__(self) -> str:
        """
        Generating SQL query string.

        SELECT colX1.tableid FROM
	        (SELECT tableid, colid FROM main_tokenized mt WHERE tokenized IN ('emil adolf von behring') GROUP BY (tableid, colid) HAVING COUNT(DISTINCT tokenized) > 0) AS colX1,
	        (SELECT tableid, colid FROM main_tokenized mt2 WHERE tokenized IN ('1901') GROUP BY (tableid, colid) HAVING COUNT(DISTINCT tokenized) > 0) AS colX2
        WHERE 
	        colX1.tableid = colX2.tableid AND 
	        colX1.colid <> colX2.colid
        UNION 
        SELECT tableid FROM main_tokenized mt3 WHERE tokenized IN ('medicine') GROUP BY (tableid, colid) HAVING COUNT(DISTINCT tokenized) > 0

        Params:
            XList (list of List): list of X
            Y (list): list of Y
        Return:
            qString (str): final query string
        """

        XList = self.XList
        Y = self.Y
        tau = self.tau

        Xlist_t = [list(col) for col in zip(*XList)]
        numcols = len(Xlist_t)

        __querystring__ = "SELECT colX1.tableid FROM \n"
        for i, col in enumerate(Xlist_t):
            if(i > 0):
                __querystring__ += ',\n'
            __colstring__ = self.__colQueryStrGen__(i + 1)
            __querystring__ += '\t'
            __querystring__ += __colstring__
        
        __wherestring__ = self.__whereClauseGen__(numcols)

        if(__wherestring__):
            __querystring__ += '\n WHERE \n'
            __querystring__ += __wherestring__
        
        __querystring__ += "\n UNION \n"

        __Ystring__ = self.__YStringGen__()

        __querystring__ += __Ystring__

        return __querystring__


    def getQueryString(self) -> str:
        # XList = self.XList
        # Y = self.Y
        # tau = self.tau
        querystring = self.__queryStringGen__()
        querystring = querystring.replace("\n", "")
        querystring = querystring.replace("\t", "")

        return querystring
    
    def getQueryString_format(self) -> str:
        # XList = self.XList
        # Y = self.Y
        # tau = self.tau
        querystring = self.__queryStringGen__()
        return querystring
    

class DBUtil(metaclass = SingletonMeta):
    def __init__(self, dbConf):
        self.dbConf = dbConf

    def getDBConn(self, specify = None):
        if(not specify):
            if(self.dbConf in ['vertica']):
                dbConn = VerticaDBConn()
            elif(self.dbConf in ['postgres']):
                dbConn = PostgresDBConn()
        elif(specify in ['vertica']):
            dbConn = VerticaDBConn()
        elif(specify in ['postgres']):
            dbConn = PostgresDBConn()
        else:
            print('Wrong DB specification')
            return None
        conn = dbConn.connect()
        return conn


    def closeDBConn(self, conn):
        if(conn):
            conn.close()
    def __jsonClean(self, jsonStr):
        endIdx = len(jsonStr)
        cursorIdx = len(jsonStr)
        jsonCleanString = ''
        while(jsonStr.rfind('"}', 0, endIdx) != -1):
            cursorIdx = jsonStr.rfind('"}', 0, endIdx)
            if(cursorIdx + 1 < endIdx):
                jsonCleanString = jsonStr[cursorIdx:endIdx+1] + jsonCleanString
            endIdx = cursorIdx
            startIdx = jsonStr.rfind(':"', 0, endIdx) + 1
            strProc = jsonStr[startIdx:endIdx].strip('"').replace('"', '')
            jsonCleanString = strProc + jsonCleanString
            endIdx = startIdx + 1
            startIdx = max(jsonStr.rfind(':"', 0, endIdx) + 1, jsonStr.rfind('["', 0, endIdx) + 1, jsonStr.rfind('{"', 0, endIdx) + 1)
            jsonCleanString = jsonStr[startIdx:endIdx] + jsonCleanString
            endIdx = startIdx
        if(endIdx > 0):
            jsonCleanString = jsonStr[0:endIdx] + jsonCleanString
        jsonCleanString = jsonCleanString.replace(',""', 'escapeSpecialStringComma') \
                                         .replace(',""}', 'escapeSpecialStringSqBracket') \
                                         .replace(',""}', 'escapeSpecialStringCurlyBracket')
        jsonCleanString = jsonCleanString.replace(',""', ',"').replace('""]', '"]').replace(':""', ':"').replace('""}', '"}')
        jsonCleanString = jsonCleanString.replace('escapeSpecialStringComma', ',""') \
                                         .replace('escapeSpecialStringSqBracket', ',""]') \
                                         .replace('escapeSpecialStringCurlyBracket', ',""}')
        jsonCleanString = re.sub(r'\,\"\"(\w+)', ',"","\\1', jsonCleanString)
        
        return jsonCleanString

        
    def getQueryString(self, XList, Y, tau, query = 'L1'):
        if(query == 'L1'):
            qg = L1QueryGenerator(XList, Y, tau)
        elif(query == 'DXF'):
            qg = DXFQueryGenerator(XList, Y, tau)
        elif(isinstance(query, QueryGenerator)):
            qg = query

        return qg.getQueryString()
    
    def getQueryString_format(self, XList, Y, tau, query = 'L1'):
        if(query == 'L1'):
            qg = L1QueryGenerator(XList, Y, tau)
        elif(query == 'DXF'):
            qg = DXFQueryGenerator(XList, Y, tau)
        elif(isinstance(query, QueryGenerator)):
            qg = query

        return qg.getQueryString_format()



    # generate SQL string to 
    # def _queryStringGen(self, XList, Y):
    #     """
    #     Generating SQL query string.

    #     SELECT colX.tableid, colX.colid, colY.tableid, colY.colid, 
    #     FROM 
    #          (SELECT tableid, colid FROM main_tokenized WHERE tokenized in (XList) GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= tau) AS colX,
    #          (SELECT tableid, colid FROM main_tokenized WHERE tokenized in (Y) GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= tau) AS colY,
    #     WHERE
    #          colX.tableid = colY.tableid AND
    #          colX.colid <> colY.colid

    #     Params:
    #         XList (list): list of X
    #     Return:
    #         qString (str): final query string
    #     """
        
    #     qString = 'SELECT colX.tableid, colX.colid, colY.tableid, colY.colid \
    #                   FROM \
    #                   (SELECT tableid, colid FROM main_tokenized WHERE tokenized in {} GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= :tau) AS colX, \
    #                   (SELECT tableid, colid FROM main_tokenized WHERE tokenized in {} GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= :tau) AS colY \
    #                   WHERE \
    #                   colX.tableid = colY.tableid AND colX.colid <> colY.colid'

    #     if(len(XList) == 1 and len(Y) == 1):
    #         qString = qString.format('(:X)', '(:Y)')
    #     elif(len(XList) > 1 and len(Y) == 1):
    #         qString = qString.format(':X', '(:Y)')
    #     elif(len(XList) == 1 and len(Y) > 1):
    #         qString = qString.format('(:X)', ':Y')
    #     else:
    #         qString = qString.format(':X', ':Y')
    #     return qString


    def queryWebTables(self, XList, Y, tau, conn = None):

        """
        Use generated query string to issue query in the DB.
        Params: 
            XList (list): list of Xs
            Y (str): Y
            tau (float): threshold
            conn (pg.Connection): default get new conn, else connection
        Return:
            res (list): a set of tableid
        """

        if(not conn):
            conn = self.getDBConn()
        queryString = self.getQueryString(XList, Y, tau)

        cur = conn.cursor()
        cur.execute(queryString)
        res = cur.fetchall()

        if(cur):
            cur.close()
        if(conn):
            self.closeDBConn(conn)

        return res

    def findValidTable(self, XList, Y):
        qString = 'SELECT colX.tableid, colX.colid, colX.rowid, colY.tableid, colY.colid, colY.rowid, colX.tokenized, colY.tokenized \
                      FROM \
                      (SELECT tableid, colid, rowid, tokenized FROM main_tokenized WHERE tokenized in {}) AS colX JOIN \
                      (SELECT tableid, colid, rowid, tokenized FROM main_tokenized WHERE tokenized in {}) AS colY \
                       ON colX.tableid = colY.tableid AND colX.rowid = colY.rowid\
                      WHERE \
                      colX.colid <> colY.colid'
        
        if(len(XList) == 1 and len(Y) == 1):
            qString = qString.format('(:X)', '(:Y)')
        elif(len(XList) > 1 and len(Y) == 1):
            qString = qString.format(':X', '(:Y)')
        elif(len(XList) == 1 and len(Y) > 1):
            qString = qString.format('(:X)', ':Y')
        else:
            qString = qString.format(':X', ':Y')

        conn = self.getDBConn()
        cur = conn.cursor()

        queryData = dict()
        if(len(XList) == 1):
            XList = str(XList[0])
        else:
            XList = tuple(XList)
        if(len(Y) == 1):
            Y = str(Y[0])
        else:
            Y = tuple(Y)
            queryData['X'] = XList
            queryData['Y'] = Y
        cur.execute(qString, queryData)
        res = list()
        for i in cur.fetchall():
            res.append(i)

        if(cur):
                cur.close()
        if(conn):
            self.closeDBConn(conn)

        return res
    

    def reverseQuery(self, XList, Y, tidList):
        qString = 'SELECT id, content, confidence, openrank FROM tables_tokenized_full WHERE id IN {}'
                      
        

        conn = self.getDBConn(specify = 'postgres')
        cur = conn.cursor()
        tokenizer = Tokenizer()

        # queryData = dict()
        if(len(tidList) == 1):
            qString = qString.format('(' + str(tidList[0]) + ')')
        else:
            qString = qString.format(tuple(tidList))
        # if(len(Y) == 1):
        #     Y = str(Y[0])
        # else:
        #     Y = tuple(Y)
        # if(len(tidList) == 1):
        #     tidList = int(tidList[0])
        # else:
        #     tidList = tuple(tidList)
        #     queryData['X'] = XList
        #     queryData['Y'] = Y
        #     queryData['tid'] = tidList
        cur.execute(qString)

        tableJSON = dict()
        round1Fail = 0
        round2Fail = 0
        for item in cur.fetchall():
            if(item[0] not in tableJSON.keys()):
                tableJSON[item[0]] = dict()
            # tableJSON[item[0]]['content'] = item[1]
            tableJSON[item[0]]['content'] = list()
            validFlag = True
            try:
                tupleJSON = json.loads(item[1])
            except json.decoder.JSONDecodeError as e:
                print('round 1 fail' + str(item[0]))
                round1Fail += 1
                try:
                    tupleJSON = json.loads(self.__jsonClean(item[1]))
                except json.decoder.JSONDecodeError as e1:
                    print('round 2 fail' + str(item[0]))
                    round2Fail += 1
                    validFlag = False
                # print(item[1].replace(':""', ':"').replace('""]', '"]').replace(',""', ',"').replace('""}', '"}').replace('" (', '('))
                # print(item[1])
                # print('=====')
            # if(validFlag):
            for tupleT in tupleJSON['tuples']:
                cellList = list()
                for cell in tupleT['cells']:
                    # if('value' not in cell.keys()):
                    #     print(tupleT['cells'])
                    if('value' in cell.keys()):
                        cellList.append(tokenizer.tokenize(cell['value']))
                    else:
                        cellList.append("")
                tableJSON[item[0]]['content'].append(tuple(cellList))
            tableJSON[item[0]]['confidence'] = item[2]
            tableJSON[item[0]]['openrank'] = max(item[3] / 100.0, 0.0)
            colNum = len(cellList)
            col = list()
            for i in range(colNum):
                col.append(i)
            tableJSON[item[0]]['colid'] = col
            # else:
            #     tableJSON.pop(item[0], None)


        print('round 2 fail:' + str(round2Fail))
        if(cur):
                cur.close()
        if(conn):
            self.closeDBConn(conn)

        return tableJSON
    
    def ressetToTableDict(self, res):
        tableDict = dict()
        for item in res:
            if(item[0] not in tableDict.keys()):
                tableDict[item[0]] = {'table': list(), 'key': list()}

            tableitem = tableDict[item[0]]['table']

            while (int(item[2]) >= len(tableitem)):
                tableitem.append([])
            
            col_data = tableitem[int(item[2])]
            while (int(item[1]) >= len(col_data)):
                col_data.append(None)
            col_data[int(item[1])] = item[3]

        for key, tableitem in tableDict.items():
            tableDict[key]['table'] = pd.DataFrame(tableitem['table'])

        for dictkey, dictitem in tableDict.items():
            for i, col in enumerate(dictitem['table'].columns):
                deduped = dictitem['table'].drop_duplicates([col])
                if(len(dictitem['table'].index) == len(deduped.index)):
                    dictitem['key'].append(i)

        return tableDict
    
    def reversedQuery_mt(self, tidList: List) -> List[Tuple]:
        _qString_ = "SELECT tableid, colid, rowid, tokenized FROM main_tokenized WHERE tableid IN {}"

        if(len(tidList) == 1):
            qval = "(" + str(tidList[0]) + ")"
        else:
            qval = str(tuple(tidList))
        _qString_ = _qString_.format(qval)

        conn = self.getDBConn(specify = 'postgres')
        cur = conn.cursor()

        cur.execute(_qString_)
        res = list()
        
        for item in cur.fetchall():
            res.append(item)

        tableDict = self.ressetToTableDict(res)

        return tableDict
    
    

    def testSQL(self):
        conn = self.getDBConn()
        cur = conn.cursor()
        # qstring = "SELECT * from main_tokenized WHERE tokenized like {}limit 10".format("'% in %'")
        qstring = "select colx1.tableid, colx1.colid, colx1.rowid, coly.tableid, coly.colid, coly.rowid from \
                   (select tableid, colid from main_tokenized where tokenized in (:x) group by (tableid, colid) having count(distinct tokenized) >= :tau) as colx1, \
                   (select tableid, colid from main_tokenized where tokenized in (:y) group by (tableid, colid) having count(distinct tokenized) >= :tau) as coly  \
                   where colx1.tableid = coly.tableid and colx1.colid <> coly.colid"
        # qstring = 'SELECT * from main_tokenized limit 5'
        print(qstring)
        qData = dict()
        qData['x'] = 'tore andre flo'
        qData['y'] = 'siena ac'
        qData['tau'] = '1'
        cur.execute(qstring, qData)
        if(cur):
            cur.close()
        if(conn):
            self.closeDBConn(conn)
        return cur.fetchall()

    def findTableContainX(self, XList):
        """
        Find all tid which represents the table containing X from the examples.
        Params:
            XList: list of X from the examples
        Return:
            res: list of tid
        """
        qString = 'SELECT tableid FROM main_tokenized WHERE tokenized IN {}'
        qData = dict()

        if(isinstance(XList, list)):
            if(len(XList) == 1):
                qString = qString.format('(:X)')
                XTuple = str(XList[0])
            else:
                qString = qString.format(':X')
                XTuple = tuple(XList)
        else:
            qString = qString.format(':X')
            XTuple = tuple(XList)
        qData['X'] = XTuple
        
        conn = self.getDBConn()
        cur = conn.cursor()
        print(qData)
        print(qString)
        
        cur.execute(qString, qData)
        res = list()
        for i in cur.fetchall():
            res.append(i[0])
        if(cur):
            cur.close()
        if(conn):
            self.closeDBConn(conn)
        return res

    def findTableContainExample(self, XList, Y):
        """
        Find all tid which represents the table containing examples.
        Params:
            XList: list of X from the examples
            Y: list of Y from the examples
        Return:
            res: list of tid
        """
        qString = 'SELECT DISTINCT sel1.tableid FROM \
                  (SELECT tableid, colid, rowid from main_tokenized WHERE tokenized IN {}) as sel1, \
                  (SELECT tableid, colid, rowid from main_tokenized WHERE tokenized IN {}) as sel2 \
                  WHERE sel1.tableid = sel2.tableid AND sel1.colid <> sel2.colid AND sel1.rowid = sel2.rowid'
        qData = dict()

        if(isinstance(XList, list)):
            if(len(XList) == 1 and len(Y) == 1):
                qString = qString.format('(:X)', '(:Y)')
                XTuple = str(XList[0])
                YTuple = str(Y[0])
            elif (len(XList) > 1 and len(Y) > 1):
                qString = qString.format(':X', ':Y')
                XTuple = tuple(XList)
                YTuple = tuple(Y)
        qData['X'] = XTuple
        qData['Y'] = YTuple

        
        conn = self.getDBConn()
        cur = conn.cursor()
        print(qData)
        print(qString)
        
        cur.execute(qString, qData)
        res = list()
        for i in cur.fetchall():
            res.append(i)
        if(cur):
            cur.close()
        if(conn):
            self.closeDBConn(conn)
        return res

    # Limit here Num Tables
    def findallbytid(self, tid):
        """
        Find all the instances with the same tid
        Params:
            tid: tableid
        Return:
            res: a list of results, each element is a tuple from the DB
        """
        print(tid)
        if(isinstance(tid, tuple)):
            tid = tid[0]
        qString = 'SELECT * FROM main_tokenized WHERE tableid = {}'.format(tid)
        print(qString)
        conn = self.getDBConn()
        cur = conn.cursor()
        cur.execute(qString)
        res = list()
        for i in cur.fetchall():
            res.append(i)
        if(cur):
            cur.close()
        if(conn):
            self.closeDBConn(conn)
        return res

    # Wrong function!!!!!!!
    def findAllFromTables(self, tidList):
        conn = self.getDBConn()
        cur = conn.cursor()
        qString = "SELECT tableid, colid, rowid, tokenized FROM main_tokenized WHERE tableid IN "
        if(len(tidList) == 1):
            qString += "({})".format(tidList[0])
            cur.execute(qString)
        elif(isinstance(tidList, tuple) and len(tidList) > 1):
            qString += ":X"
            qData = dict()
            qData['X'] = tidList
            cur.execute(qString, qData)
        elif(isinstance(tidList, list) and len(tidList) > 1):
            qString += ":X"
            tid = tuple(tidList)
            qData = dict()
            qData['X'] = tid
            cur.execute(qString, qData)
        res = list()
        for i in cur.fetchall():
            res.append(i)

        if(cur):
            cur.close()
        if(conn):
            self.closeDBConn(conn)

        return res


    def getAllTid(self) -> list:
        conn = None
        cur = None
        if(os.path.isfile('./table/tid.txt')):
            print('reading tid from local file')
            res = list()
            with open('./table/tid.txt', 'r') as f:
                for tid in f:
                    if(str(tid).startswith('[') and str(tid).endswith(']')):
                        res.append(str(tid)[1:-1])
                    else:
                        res.append(str(tid))
        else:
            print('querying all tids')
            conn = self.getDBConn()
            cur = conn.cursor()

            qString = 'SELECT DISTINCT tableid from main_tokenized'
            cur.execute(qString)
            res = list()
            # i = 0
            for row in cur.fetchall():
                res.append(row)
        if(cur):
            cur.close()
        if(conn):
            self.closeDBConn(conn)
        return res
