from data.Answer import Answer
from data.dbUtil import DBUtil
from webtableindexer.Tokenizer import Tokenizer
from transformer.transformer import DirectTransformer
from data.graph import GENERATE
from utils.intersect import intersect_procedure
from transformer.transformer import DirectTransformer

import os
import pandas as pd
import argparse
import time
import pprint

__BENCHMARK__ = 'benchmark'
__EXAMPLENUM__ = 2
__QNUM__ = __EXAMPLENUM__ + 3

def parseArg():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dbname', type = str, default = 'postgres')
    parser.add_argument('-l', '--limit', type = int, default = 200)
    parser.add_argument('-v', '--verbose', action = 'store_true')
    parser.set_defaults(verbose = False)
    args = parser.parse_args()
    argDict = dict()
    argDict['dbname'] = args.dbname
    argDict['limit'] = args.limit
    argDict['verbose'] = args.verbose
    return argDict

def testCSV(verbose):
    tokenizer = Tokenizer()
    __path__ = 'benchmarkcsv'
    __mainfile__ = 'main.csv'
    __filelist__ = ['costrec.csv', 'markuprec.csv']
    data = pd.read_csv(os.path.join(__path__, __mainfile__))
    examples = data.iloc[:__EXAMPLENUM__, :]
    XList = examples.iloc[:, :-1].values.tolist()
    Y = examples.iloc[:, -1].values.tolist()
    XList_tokenize = list()
    for xeach in XList:
        templist = list()
        for token in xeach:
            templist.append(str(token).lower())
        XList_tokenize.append(templist)
    XList= XList_tokenize
    Y = [str(item).lower() for item in Y]
    exampleList = list()
    for x, y in zip(XList, Y):
        ans = Answer(x, y, isExample = True)
        exampleList.append(ans)
    transformer = DirectTransformer(exampleList)
    Q = data.iloc[__EXAMPLENUM__:__QNUM__, :-1].values.tolist()
    # Q = [str(item).lower() for item in Q]
    for i in range(len(Q)):
        for j in range(len(Q[i])):
            Q[i][j] = str(Q[i][j]).lower()
    Q = [tuple(i) for i in Q]

    graphDict, reversedQS = transformer.transform_csv(XList, Y, Q, verbose = verbose)
    if(verbose):
        with open('graphdict.txt', 'w') as f:
            for key, val in graphDict.items():
                f.write(str(key) + '\n')
                f.write(str(val) + '\n')
                f.write('===========')
    init = True
    finalDict = dict()
    progDict = dict()

    timetrans = time.time()
    GraphDict_common = intersect_procedure(graphDict, verbose = verbose)
    print('Time for full transformation: ' + str(time.time() - timetrans))

    if(verbose):
        with open('graphfinal.txt', 'w') as f:
            for key, val in GraphDict_common.items():
                f.write(str(key) + '\n')
                f.write('----------\n')
                for item in val:
                    f.write(str(item) + '\n')
                f.write('===========\n')
    
    for key, graph in GraphDict_common.items():
        break
    if(graph):
        answer = transformer.discover_csv(Q, reversedQS, graph[0])
    else:
        answer = None
    if(verbose):
        with open('finaltransformation.txt', 'w') as f:
            if(answer):
                for key, val in answer.items():
                    f.write('Transformation: ' + str(key) + ' ==> ' + str(val) + '\n')
            else:
                f.write('No transformation found due to no graph found')
    print(answer)


def getExampleAndQuery(path, file, numExample = 5, qnum = 20):
    data = pd.read_csv(os.path.join(path, file))
    examples = data.iloc[:numExample, :]
    XList = examples.iloc[:, :-1].values.tolist()
    Y = examples.iloc[:, -1].values.tolist()
    XList_tokenize = list()
    for xeach in XList:
        templist = list()
        for token in xeach:
            templist.append(str(token).lower())
        XList_tokenize.append(templist)
    XList = XList_tokenize
    Y = [str(item).lower() for item in Y]
    exampleList = list()
    for x, y in zip(XList, Y):
        ans = Answer(x, y, isExample = True)
        exampleList.append(ans)
    if(len(data) < numExample + qnum):
        exceedFlag = True
    else:
        exceedFlag = False
    if(not exceedFlag):
        Q = data.iloc[numExample:__QNUM__, :-1].values.tolist()
    else:
        Q = data.iloc[numExample:, :-1].values.tolist()
    for i in range(len(Q)):
        for j in range(len(Q[i])):
            Q[i][j] = str(Q[i][j]).lower()
    Q = [tuple(i) for i in Q]

    return XList, Y, exampleList, Q

def testDB():
    __path__ = 'benchmark'
    __mainfile__ = 'CountryToCapital.csv'

    XList, Y, exampleList, Q = getExampleAndQuery(__path__, __mainfile__)

    transformer = DirectTransformer(exampleList)

    dbUtil = DBUtil(dbConf = 'postgres')
    # print(dbUtil.getQueryString_format(XList, Y, 2, 'DXF'))
    # XList = [['emil adolf von behring', '1901'], ['jean henri dunant', '1901']]
    # Y = ['medicine', 'peace']

    # tableList = list()
    # res = dbUtil.queryWebTables(XList, Y, 2)
    # for item in res:
    #     tableList.append(item[0])
    
    # print(len(tableList))

    # tableDict = dbUtil.reversedQuery_mt(tableList)

    graphs, reversedQS = transformer.transform_db(XList, Y, Q, query = 'DXF')





if(__name__ == '__main__'):
    args = parseArg()
    testDB()