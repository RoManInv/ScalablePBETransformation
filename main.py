from data import program
from data.Answer import Answer
from data.dbUtil import DBUtil
from webtableindexer.Tokenizer import Tokenizer
from transformer.transformer import DirectTransformer
from data.graph import GENERATE, discover
from utils.intersect import intersect_procedure
from transformer.transformer import DirectTransformer
from utils.tokens import Makenode
from utils.complexTGen import complexTGenerator

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
    tokenizer = Tokenizer()
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
    xlist = list()
    for x, y in zip(XList, Y):
        xlist.append([tokenizer.tokenize(xelem, y) for xelem in x if xelem is not None])
    XList = xlist
    for x, y in zip(XList, Y):
        ans = Answer(x, y, isExample = True)
        exampleList.append(ans)
    yexample = Y[0]
    if(len(data) < numExample + qnum):
        exceedFlag = True
    else:
        exceedFlag = False
    if(not exceedFlag):
        Q = data.iloc[numExample:qnum, :-1].values.tolist()
        QTruth = data.iloc[numExample:qnum, -1].values.tolist()
    else:
        Q = data.iloc[numExample:, :-1].values.tolist()
        QTruth = data.iloc[numExample:, -1].values.tolist()
    qlist = list()
    for q in Q:
        qlist.append([tokenizer.tokenize(qelem, yexample) for qelem in q])
    Q = qlist
    for i in range(len(Q)):
        QTruth[i] = str(QTruth[i]).lower()
        for j in range(len(Q[i])):
            Q[i][j] = str(Q[i][j]).lower()
    Q = [tuple(i) for i in Q]
    QTruth_dict = dict()
    for q, qt in zip(Q, QTruth):
        QTruth_dict[q] = qt

    # print(XList, Y, Q)

    return XList, Y, exampleList, Q, QTruth_dict

def testDB(path, mainfile, verbose = False):
    starttime = time.time()
    __path__ = path
    # __mainfile__ = 'CountryToCapital.csv'
    __mainfile__ = mainfile
    with open('report.txt', 'a') as f:
            f.write("Testing " + str(__mainfile__) + '\n')

    XList, Y, exampleList, Q, QTruth = getExampleAndQuery(__path__, __mainfile__)
    # Qlist_sep = list()
    # for item in Q:
    #     strlist = list()
    #     for String in item:
    #         strlist.extend(program.Makenode_comb(String, []))
    #     Qlist_sep.append(strlist)
    # Q = Qlist_sep
    # print(Q)
    # print(QTruth)
    # print(XList)

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

    currtime = time.time()
    graphs, reversedQS = transformer.transform_db(XList, Y, Q, query = 'Proteus')
    print("Time consumption for full transformation: " + str(time.time() - currtime))
    if(graphs is None and reversedQS is None):
        print("This file is temporarily skipped due to taking too long")
        with open('report.txt', 'a') as f:
            f.write("This file is temporarily skipped due to taking too long\n")
            f.write("===============\n")
        return
    # print(graphs[0])
    graphs = graphs[0]
    graphs = sorted(graphs, key = lambda x: x['support'], reverse = True)

    

    # with open('graphs.txt', 'w') as f:
    #     for graph in graphs:
    #         for key, val in graph.items():
    #             f.write(str(key) + ':\n')
    #             # for item in val.values():
    #             f.write('Graph:\n')
    #             f.write(str(val['graph']))
    #             f.write('\n')
    #             f.write('Support: ' + str(val['support']) + '\n')
    #             # f.write('\n')
    #             f.write('==================\n')
    with open('graphs.txt', 'w') as f:
        f.write(str(graphs))

    disctime = time.time()

    correct = 0
    wrong = 0
    covered = 0
    total = len(Q)

    remainingquery = [q for q in Q]



    for graphdict in graphs:
        g = graphdict['graph']
        ansDict = discover(remainingquery, reversedQS, g)
        # print(ansDict)
        if(verbose):
            with open('report.txt', 'a') as f:
                f.write(str(ansDict))
                f.write('\n')
                f.write("===============\n")
        for key, val in ansDict.items():
            if(not val == ''):
                if(QTruth[tuple(key)].lower() == val.lower()):
                    correct += 1
                    if(key in remainingquery):
                        remainingquery.remove(key)
        
        if(not remainingquery):
            covered = total
            break
    if(remainingquery):
        covered = total - len(remainingquery)
    if(verbose):
        with open('report.txt', 'a') as f:
            f.write("\tTime for transformation discovery: " + str(time.time() - disctime) + '\n')
            f.write(str(total) + " queries in total.\n")
            f.write(str(covered) + " queries are covered.\n")
            f.write(str(correct) + " queries are correct.\n")


            f.write('Total time consumption: ' + str(time.time() - starttime) + '\n')
            f.write("===============\n")

def testbatch_exp(verbose = False):
    path = 'benchmarkForReport/experiment'
    with open('exclude.txt', 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    for file in os.listdir(path):
        print('Testing ' + str(file))
        if(str(file) in lines):
            print('skipped')
            continue
        if(os.path.isfile(os.path.join(path, file))):
            try:
                testDB(path, file, verbose)
            except:
                print('This file cannot be read')

def testbatch_func(verbose = False):
    path = 'benchmarkForReport/functional'
    with open('exclude.txt', 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    for file in os.listdir(path):
        print('Testing ' + str(file))
        if(str(file) in lines):
            print('skipped')
            continue
        if(os.path.isfile(os.path.join(path, file))):
            try:
                testDB(path, file, verbose)
            except:
                print('This file cannot be read')

def testbatch(verbose = False):
    path = 'benchmarkcomplex'
    # with open('exclude.txt', 'r') as f:
    #     lines = f.readlines()
    # lines = [line.strip() for line in lines]
    for file in os.listdir(path):
        print('Testing ' + str(file))
        # if(str(file) in lines):
        #     print('skipped')
        #     continue
        if(os.path.isfile(os.path.join(path, file))):
            try:
                testDB(path, file, verbose)
            except:
                print('This file cannot be read')

def genComplexT(path, file, header):
    generator = complexTGenerator(3)
    generator.generate(path, file, header = header)

if(__name__ == '__main__'):
    args = parseArg()
    with open('report.txt', 'w') as f:
        f.write("Result for each dataset\n")
        f.write("==========\n")
    # testbatch_exp(True)
    # testbatch_func(True)

    # testbatch(True)

    path = 'benchmarkcomplex'
    file = 'CountryYearToPresident.csv'

    testDB(path, file, verbose = True)
    # tokenizer = Tokenizer()
    # data = pd.read_csv(os.path.join(path, file))
    # for row in data.values:
    #     for val in row:
    #         print(Makenode(val, []))
    # for row in data.values:
    #     print(row[0], tokenizer.tokenize(row[0], row[1]))

    # path = "benchmarkcomplex"
    # file = "CountryToLanguage.csv"
    # genComplexT(path, file, 0)
