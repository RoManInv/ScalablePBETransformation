from data.DBUtil import DBUtil
from data.Schema import TableSchema
from data.Answer import Answer
from data.Table import Table

from typing import List

class DirectTransformer:
    def __init__(self) -> None:
        pass

    def validateResultSet(self, resultSet: list) -> list:
        XTABLE = 0
        XCOL = 1
        XROW = 2 
        YTABLE = 3
        YCOL = 4
        YROW = 5

        for row in resultSet:
            if(not row[XTABLE] == row[YTABLE] or row[XCOL] == row[YCOL] or not row[XROW] == row[YROW]):
                resultSet.remove(row)

        return resultSet

    def validateTable(self, queryResult, tableResult):
        queryTable = list()
        tableTable = list()
        for item in queryResult:
            queryTable.append(item[0])
        for item in tableResult:
            tableTable.append(item)

        validTable = list()
        for item in queryTable:
            if(item in tableTable):
                validTable.append(item)
        return validTable

    def validateExampleFromTable(self, exampleList: List[Answer], reversedQS, tidList):
        for tid in tidList:
            currTable = reversedQS[tid]
            currTokenList = currTable['content']
            for e in exampleList:
                for row in currTokenList:
                    if(e.X in row and e.Y in row):
                        e.appearIn.append(tid)

        return exampleList

                

    def getTableListFromRS(self, resultSet: list) -> list:
        tableList = list()
        for row in resultSet:
            tableList.append(row[0])
        return tableList

    # def generateExampleAnswerFromQuery(self, resultSet):
    #     exampleAnswerList = list()
    #     for row in resultSet:

    def transform(self, XList, Y, Q, reversedQS, tableList):
        transformList = list()
        explanationDict = dict()
        for x, y in zip(XList, Y):
            for tid in tableList:
                currTable = reversedQS[tid]
                currCol = currTable['colid']
                foundExample = False
                xCol = -1
                yCol = -1
                for tupleT in currTable['content']:
                    if(x in tupleT and y in tupleT):
                        foundExample = True
                        xCol = tupleT.index(x)
                        yCol = tupleT.index(y)
                        break
                if(foundExample):
                    for q in Q:
                        for tupleT in currTable['content']:
                            if(q in [tupleT[xCol]]):
                                qy = tupleT[yCol]
                                ans = Answer(q, qy, tid, xCol, yCol, None, (x, y, xCol, yCol, tid), isExample = False)
                                transformList.append(ans)
                                if((q, qy) not in explanationDict.keys()):
                                    explanationDict[(q, qy)] = list()
                                explanationDict[(q, qy)].append((x, y, tid))



        return transformList, explanationDict


    def finalAnswerGen(self, answerList: List[Answer], Q: List):
        finalAnswerDict = dict()
        for answer in answerList:
            if(answer.X in Q):
                if(answer.X not in finalAnswerDict.keys()):
                    finalAnswerDict[answer.X] = (answer, answer.getScore())
                else:
                    if(answer.getScore() > finalAnswerDict[answer.X][1]):
                        finalAnswerDict[answer.X] = (answer, answer.getScore())
        finalAnswerList = list()
        for key in finalAnswerDict.keys():
            finalAnswerList.append(finalAnswerDict[key][0])
        return finalAnswerList

    # colX.tableid, colX.colid, colX.rowid, colY.tableid, colY.colid, colY.rowid, colX.tokenized, colY.tokenized
    # def transform(self, XList, Y, Q, reversedQS):
    #     transformList = list()
    #     explanationDict = dict()
    #     for x, y in zip(XList, Y):
    #         for row in reversedQS:
    #             currTable = -1
    #             if(x in [row[-2]] and y in [row[-1]]):
    #                 currTable = row[0]
    #                 xRow = row[2]
    #                 xCol = row[1]
    #                 yRow = row[-3]
    #                 yCol = row[-4]
    #                 for ansRow in reversedQS:
    #                     for q in Q:
    #                         if(currTable != -1 and ansRow[-2] in [q] and
    #                            ansRow[0] == currTable and ansRow[3] == currTable and
    #                            ansRow[1] == xCol and ansRow[-4] == yCol and ansRow[3] != xRow):
    #                            ans = Answer(ansRow[-2], ansRow[-1], ansRow[0], ansRow[1], ansRow[-4], ansRow[2], (x, y, xCol, xRow, yCol, yRow), False)
    #                            transformList.append(ans)
    #                            if((ansRow[-2], ansRow[-1]) in explanationDict.keys()):
    #                                explanationDict[(ansRow[-2], ansRow[-1])].append((x, y, xCol, xRow, yCol, yRow))
    #                            else:
    #                                explanationDict[(ansRow[-2], ansRow[-1])] = list()
    #                                explanationDict[(ansRow[-2], ansRow[-1])].append((x, y, xCol, xRow, yCol, yRow))

    #     return transformList, explanationDict



    # def transform(self, XList, Y, Q, tableList):
    #     """
    #     Finding direct transformations. In the 1st step, we have retrieved the tables that contains both X and Y. We extract these table ids and query Q in these tables to find transformations.
    #     For each X:
    #         For each Y:
    #             if(x,y) in the same table, same row and different column:
    #                 record column id and table id
    #                 for each Q:
    #                     if(q) in this table:
    #                         query for the row id of q
    #                         query for tokenized field where tableid = Q.tableid, rowid = Q.rowid and colid <> Q.colid
    #                         for each result:
    #                             transformList.append((q, res))
    #                             explanationDict[(q, res)] = (x, y, tableid, x.colid, y.colid, q.colid, res.colid, q.rowid, res.rowid)
    #     return transformList, explanationDict
    #     """
    #     dbUtil = DBUtil()
    #     tableDict = dict()
    #     ts = TableSchema()
    #     transformList = list()
    #     explanationDict = dict()
    #     # conn = dbUtil.getDBConn()
    #     for tid in tableList:
    #         table = dbUtil.findallbytid(tid)
    #         tableDict[tid] = table
    #     # print(tableDict)

    #     for x, y in zip(XList, Y):
    #         xFlag = False
    #         yFlag = False
    #         for tid in tableDict.keys():
    #             for item in tableDict[tid]:
    #                 # print(item)
    #                 if(x in item):
    #                     # print('x detected')
    #                     xFlag = True
    #                     xCol = item[ts.colid]
    #                     xRow = item[ts.rowid]
    #                     print(xCol, xRow)
    #                 if(y in item):
    #                     # print('y detected')
    #                     yFlag = True
    #                     yCol = item[ts.colid]
    #                     yRow = item[ts.rowid]
    #                     # print(yCol, yRow)
    #             if(xFlag and yFlag):
    #                 for q in Q:
    #                     qRow = -1
    #                     for item in tableDict[tid]:
    #                         # print(q, item)
    #                         if(q in item and item[ts.colid] == xCol):
    #                             # print('q in table')
    #                             qRow = item[ts.rowid]
    #                             # print(qRow)
    #                     qFlag = False
    #                     for item in tableDict[tid]:
    #                         if(qRow != -1 and item[ts.tokenized] in [q] and item[ts.colid] == xCol):
    #                             qFlag = True
    #                             qCol = item[ts.colid]
    #                             break
    #                     for item in tableDict[tid]:
    #                         if(qFlag):
    #                             if(item[ts.rowid] == qRow and item[ts.colid] == yCol):
    #                                 ans = Answer(q, item[ts.tokenized], tid, xCol, qCol, qRow, ((x, y, tid, xCol, xRow)), isExample = False)
    #                                 transformList.append(ans)
    #                                 explanationDict[(q, item[ts.tokenized])] = (x, y, tid, xCol, xRow)
            
        

    #     return transformList, explanationDict