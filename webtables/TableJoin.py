from re import T

from nltk import text
from data.DBUtil import DBUtil
from utils.timeout import TimeoutException
from utils.timeout import Timer

"""
{
    "content": [(cell, cell, ...), ...],
    "confidence":
    "openrank":
    "colid": [0, 1, ...]
}
"""


class TableJoin:

    def __init__(self) -> None:
        self.__tokenized = 0
        self.__tableid = 1
        self.__colid = 2
        self.__rowid = 3

    def __queryForTables(self, XList: "list") -> "list":
        """
        Query all the tables that contains X
        Params:
            XList: list of X from examples
        Return:
            list of tid that represent tables containing X
        """
        dbUtil = DBUtil()

        return dbUtil.findTableContainX(XList)

    def __findJoinColumns_db(self, tid: "str", XList: "list") -> "list":
        """
        FindJoinColumns algorithm from the paper. 
        Find all Zs where the instances can validate a FD X->Z.
            X->Z if X and Z is in the same row but different column, and for a X, there is no a second value of Z.
            If multiple Zs can be defined by X, this Z and the previous Z should be deleted from the list. 
        Params:
            tid: table id that we are currently working on
            XList: list of X from examples
        Return:
            ZList: list of Z that is joinable
        """
        dbUtil = DBUtil()
        # __tokenized = 0
        # __tableid = 1
        # __colid = 2
        # __rowid = 3
        # print('Finding all tables')
        XAll = dbUtil.findallbytid(tid)
        # print(tid[:10])
        XZ = list()
        ZDeleted = list()
        ZList = list()
        print('Find Zs')
        for item in XAll:
            # print('X: ' + str(item))
            if(item[self.__tokenized] in ZDeleted):
                continue
            # print('not deleted')
            if(item[self.__tokenized] in XList):
                thisXCol = item[self.__colid]
                thisXRow = item[self.__rowid]
                for item2 in XAll:
                    print(str((item, item2)))
                    if((item[self.__tokenized], item2[self.__tokenized]) not in XZ):
                        if(item2[self.__colid] != thisXCol and item2[self.__rowid] == thisXRow and item2[self.__tokenized] not in [item[self.__tokenized]]):
                            XZ.append((item[self.__tokenized], item2[self.__tokenized]))
                            # print(str((item[self.__tokenized], item2[self.__tokenized])))
                    else:
                        if(item2[self.__colid] == thisXCol or item2[self.__rowid] != thisXRow):
                            ZDeleted.append(item2[self.__tokenized])
                            # print(XZ)
                            XZ.remove((item[self.__tokenized], item2[self.__tokenized]))
            # print('=========================')
        for tup in XZ:
            ZList.append(tup[1])
        print(ZList)
        return ZList

    def __findJoinColumns(self, tid: "str", XList: "list", allTable: "dict") -> "list":
        # print('finding all tables')
        XALL = allTable[tid]['content']
        XZ = list()
        ZDeleted = list()
        ZList = list()
        for tup in XALL:
            inDelete = False
            for cell in tup:
                if(cell in ZDeleted):
                    inDelete = True
                    break
            if(inDelete):
                continue
            for cell in tup:
                if(cell in XList):
                    thisXCol = tup.index(cell)
                    thisXRow = allTable[tid]['content'].index(tup)
                    for tup2 in XALL:
                        for cell2 in tup2:
                            # with open('./ZList.txt', 'a') as f:
                            #         f.write('\n' + str((cell, cell2)) + ' ' + str((thisXCol, thisXRow, tup2.index(cell2), allTable[tid].index(tup2))) + '\n')
                            if((cell, cell2) not in XZ):
                                jcXCol = tup2.index(cell2)
                                jcXRow = allTable[tid]['content'].index(tup2)
                                # with open('ZList.txt', 'a') as f:
                                    # f.write('\n' + str((cell, cell2)) + ' ' + str((thisXCol, thisXRow, jcXCol, jcXRow)) + '\n')
                                if(thisXCol != jcXCol and thisXRow == jcXRow and cell2 not in [cell]):
                                    XZ.append((cell, cell2))
                            # else:
                            #     if(thisXCol == jcXCol or thisXRow != jcXRow):
                            #         ZDeleted.append(cell2)
                            #         XZ.remove((cell, cell2))
        for tup in XZ:
            if(tup[1] not in ZList):
                ZList.append(tup[1])
        # if(XZ):
        #     with open('ZList.txt', 'a') as f:
        #         f.write(len(XZ))
        return ZList


    def __findJoinableTables_db(self, tid: "str", zi, YList):
        """
        FindJoinableTable algorithm from the paper
        Given a Z and the table currently working on, if there is a FD Z->Y that can be validated from the instances, the table is joinable.
            Z->Y if Z and Y is in the same row but different column, and there is no multiple Ys for a Z.
            If multiple Ys can be defined by Z, return False and immediately break the search.
        Params:
            tid: table id that is currently being worked on
            zi: a instance of Z that is currently being worked on
            YList: list of Y from examples
        Return
            flag: boolean where True means the current table is joinable, vice versa.
        """
        flag = False
        dbUtil = DBUtil()
        insAll = dbUtil.findallbytid(tid)
        ZDeleted = list()
        ZY = list()
        for item in insAll:
            if(item[self.__tokenized] in ZDeleted):
                continue
            if(item[self.__tokenized] in [zi]):
                thisZCol = item[self.__colid]
                thisZRow = item[self.__rowid]
                for item2 in insAll:
                    if(item2[self.__tokenized] in YList):
                        if((item[self.__tokenized], item2[self.__tokenized]) not in ZY):
                            if(item2[self.__colid] != thisZCol and item2[self.__rowid] == thisZRow):
                                ZY.append((item[self.__tokenized], item2[self.__tokenized]))
                                flag = True
                        else:
                            if(item2[self.__colid] == thisZCol or item2[self.__rowid] != thisZRow):
                                flag = False
                                break
        return flag

    def __findJoinableTables(self, tid: "str", zi, YList, allTable: "dict"):
        flag = False
        ZDeleted = list()
        ZY = list()
        for row in allTable[tid]['content']:
            isInZDeleted = False
            for cell in row:
                if(cell in ZDeleted):
                    isInZDeleted = True
                    break
            if(isInZDeleted):
                continue
            for cell in row:
                if(cell in [zi]):
                    thisZCol = row.index(cell)
                    thisZRow = allTable[tid]['content'].index(row)
                    for row2 in allTable[tid]['content']:
                        isInYList = False
                        for cell2 in row2:
                            jcXCol = row2.index(cell2)
                            jcXRow = allTable[tid]['content'].index(row2)
                            if(cell2 in YList):
                                if((cell, cell2) not in ZY):
                                    if(jcXCol != thisZCol and jcXRow == thisZRow):
                                        ZY.append((cell, cell2))
                                        flag = True
                                else:
                                    if(jcXCol == thisZCol or jcXRow != thisZRow):
                                        flag = False
                                        break
        return flag

    def __findJoinableColumns(self, tid: "str", zi, YList, allTable: "dict") -> "bool":
        flag = False
        insAll = allTable[tid]
        ZDeleted = list()
        ZY = list()
        for row in insAll:
            inZDeleted = False
            for cell in row:
                if(cell in ZDeleted):
                    inZDeleted = True
                    break
            if(inZDeleted):
                continue
            for cell in row:
                if(cell in [zi]):
                    thisZCol = row.index(cell)
                    thisZRow = insAll.index(row)
                    for row2 in insAll:
                        row2BreakFlag = False
                        for cell2 in row2:
                            if(cell2 in YList):
                                jcZCol = row2.index(cell2)
                                jcZRow = insAll.index(row2)
                                if((cell, cell2) not in ZY):
                                    if(thisZCol != jcZCol and thisZRow == jcZRow):
                                        ZY.append((cell, cell2))
                                        flag = True
                                else:
                                    if(thisZCol == jcZCol or thisZRow != jcZRow):
                                        flag = False
                                        row2BreakFlag = True
                        if(row2BreakFlag):
                            break
        return flag

    def __decompositeExample(self, exampleList) -> "tuple[list, list]":
        """
        Decomposite the example tuples into X and Y. We assume the last element is Y, and all the rest are X.
        The order is ESSENTIAL! We use the same order to demonstrate the correlation.
        Params:
            exampleList (list of tuple): list of example tuples
        return:
            XList: list of X
            YList: list of Y
        """
        XList = list()
        YList = list()
        for XY in exampleList:
            Y = XY[-1]
            X = XY[:-1]
            XList.append(X)
            YList.append(Y)
        return XList, YList

    def __getNumCoveredTuples_db(self, tj, XList, Y) -> int:
        cover = 0
        dbUtil = DBUtil()
        table = dbUtil.findallbytid(tj)
        for (x, y) in zip(XList, Y):
            isCover = False
            for rec in table:
                if(x in rec):
                    xCol = rec[self.__colid]
                    xRow = rec[self.__rowid]
                    for r in table:
                        if(y in r and r[self.__colid] != xCol and r[self.__rowid] == xRow):
                            cover += 1
                            isCover = True
                            break
                if(isCover):
                    break
        return cover

    def __getNumCoveredTuples(self, tj: "str", XList, Y, allTable: "dict") -> int:
        cover = 0
        table = allTable[tj]['content']
        for x, y in zip(XList, Y):
            isCover = False
            for row in table:
                if(x in row and y in row):
                    cover += 1
        return cover
    
    def tableJoiner(self, maxPathLength, XList, YList, tau, initTableList, QList, allTable: "dict"):
        dbUtil = DBUtil()

        path = 1
        tables = list()
        currentTablePath = list()
        allTablePaths = list()
        # XList, YList = self.__decompositeExample(exampleList)
        # if(isinstance(QList, list)):
        #     XList.extend(QList)
        # else:
        #     XList.append(QList)
        timer = Timer()
        try:
            with timer.time_limit(7200):
                while(path <= maxPathLength):
                    if(path == 1):
                        xTables = self.__queryForTables(XList)
                        print(len(xTables))
                        # print(xTables)
                        TX = list()
                        txCount = 0
                        for tid in xTables:
                            if(tid not in initTableList):
                                if(txCount < 50):
                                    TX.append(tid)
                                    if(tid not in allTable.keys()):
                                        missingTable = dbUtil.reverseQuery(XList, YList, tid)
                                        if(isinstance(missingTable, list)):
                                            allTable[tid] = missingTable[0]
                                        else:
                                            allTable[tid] = missingTable
                                    # allTable[tid] = missingTable
                        # with open('./ZList.txt', 'a') as f:
                        #     f.write(str(allTable[list(allTable.keys())[0]]))
                        # print(TX)
                        for tid in TX:
                            ZList = list()
                            ZList = self.__findJoinColumns(tid, XList, allTable)
                            # with open('ZList.txt', 'a') as f:
                            #     f.write(len(ZList))
                            Tj = list()
                            if(ZList):
                                for Z in ZList:
                                    if(self.__findJoinableTables(tid, Z, YList, allTable)):
                                        Tj.append(tid)
                                    if(Tj):
                                        for t in Tj:
                                            if(self.__getNumCoveredTuples(t, XList, YList, allTable) >= tau):
                                                tables.append(t)
                                        currentTablePath.append((tid, Z))
                    path += 1
                    allTablePaths.append(currentTablePath)
        except TimeoutException as e:
            print("Timed out!")
         
        return allTablePaths


        # else:
        #     newTablePath = list()
        #     for tup in currentTablePath:
        #         tzlist = self.__queryForTables(tup[1])
        #         tz = list()
        #         for t in tzlist:
        #             if(t not in initTableList):
        #                 tz.append(t)
        #         for table in tz:
        #             !!!!table = table join tup[0]!!!!
        #             joinCol = self.__findJoinColumns(table, tup[1])
        #             for z in joinCol:
        #                 tj = self.__findJoinableTables(table, z, YList)
        #                 if(tj):
        #                     if(self.__getNumCoveredTuples(tj, XList, YList) > tau):
        #                         tables.append(tj)
        #                     newTablePath.append(tj, z)
        #     currentTablePath = newTablePath
        #     !!!!ClearFrequentedTables(currentTablePath)!!!!
        # path += 1
        # allTablePaths.append(currentTablePath)
    # def tableJoiner(self, maxPathLength, XList, YList, tau, initTableList, QList, allTable: "dict", timeLimit = 3600):
    #     timer = Timer()
    #     try:
    #         with timer.time_limit(timeLimit):
    #             allTablePath = self.tableJoiner(maxPathLength, XList, YList, tau, initTableList, QList, allTable)
    #     except TimeoutException as e:
    #         print("Timed out!")