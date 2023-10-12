import os
import time
from typing import List, Tuple
import pandas as pd
import traceback


from data.Answer import Answer
from webtableindexer.Tokenizer import Tokenizer
from data.dbUtil import DBUtil

from data.graph import GENERATE, GENERATE_scalable

class DirectTransformer:
    def __init__(self, exampleList: List[Answer]) -> None:
        self.examples = exampleList

        
    def __df_contain_str__(self, df, val):
        cols = df.columns
        for col in cols:
            testdf = df[df[col].astype(str).str.contains(val)]
            # print(testdf)
            if(not testdf.empty):
                idxList = testdf.index
                returndf = df.iloc[idxList]
                print(returndf)
                return returndf
        return None

    def validateExampleFromTable_pbe(self, reversedQS: dict, tidList: list, returnexp = False):
        notcoveredList = list()
        notcoveredFlag = True
        for tid in tidList:
            if(tid not in reversedQS.keys()):
                notcoveredList.append(tid)
                continue
            currTable = reversedQS[tid]
            currTokenList = currTable['content']
            for e in self.examples:
                for row in currTokenList:
                    if((any(xpartial in row for xpartial in e.Xtoken))):
                        e.appearIn.append(tid)
                        notcoveredFlag = False
            if(notcoveredFlag):
                notcoveredList.append(tid)
            notcoveredFlag = True

        for tid in notcoveredList:
            if(tid in tidList):
                tidList.remove(tid)

        return tidList


    def transform_csv(self, XList, Y, Q, eLimit = 5, qsLimit = -1, verbose = False):
        __path__ = 'benchmarkcsv'
        __mainfile__ = 'main.csv'
        __filelist__ = ['costrec.csv', 'markuprec.csv']
        answerList = list()
        tableList = list()
        reversedQS = dict()
        transformList = list()
        explanationDict = dict()
        tokenizer = Tokenizer()
        graphList = list()
        MAX_ITER = 10

        mainfile = pd.read_csv(os.path.join(__path__, __mainfile__))
        reversedQS = dict()
        tid = 1
        for item in __filelist__:
            file = pd.read_csv(os.path.join(__path__, item), dtype = str)
            reversedQS[tid] = {'table': file, 'key': list()}
            tid += 1

        for dictkey, dictitem in reversedQS.items():
            for i, col in enumerate(dictitem['table'].columns):
                deduped = dictitem['table'].drop_duplicates([col])
                if(len(dictitem['table'].index) == len(deduped.index)):
                    dictitem['key'].append(i)


        for x, y in zip(XList, Y):
            # print(x, y)
            xlist = [str(item).lower() for item in x]
            ans = Answer(xlist, str(y).lower(), isExample = True)
            answerList.append(ans)
        # print(answerList)
        if(verbose):
            with open('groupcomb.txt', 'w') as f:
                f.write('Start combination \n')
            with open('groupdetail.txt', 'w') as f:
                f.write('Each group \n')
            with open('detail_validated.txt', 'w') as f:
                f.write('detail_validated:\n')
            with open('detail_validated_dedup.txt', 'w') as f:
                f.write('Deduplicated detail_validated:\n')
        graphList = list()
        for answer in answerList:
            try:
                gentime = time.time()
                g1 = GENERATE(answer.X, answer.Y, reversedQS, verbose = verbose)
            except Exception as e:
                print("Exception:")
                traceback.print_exc()
                g1 = None
            if(g1):
                print('\tTime for graph generation: ' + str(time.time() - gentime))
                graphList.append((g1, (answer.X, answer.Y)))
        
        graphDict = dict()
        print('final graph')
        for graphtuple in graphList:
            # print(graphtuple[1])
            key = list(graphtuple[1])
            for i in range(len(key)):
                if(type(key[i]) is list):
                    key[i] = tuple(key[i])
            key = tuple(key)
            val = graphtuple[0]
            if(key not in graphDict.keys()):
                graphDict[key] = list()
            if(type(graphtuple[0]) is list):
                graphDict[key].extend(val)
            else:
                graphDict[key].append(val)                         

                        
        return graphDict, reversedQS
    
    def transform_db(self, XList, Y, Q, query = 'DXF', eLimit = 5, qsLimit = -1, verbose = False):
        __path__ = 'benchmarkcsv'
        __mainfile__ = 'main.csv'
        __filelist__ = ['costrec.csv', 'markuprec.csv']
        answerList = list()
        tableList = list()
        reversedQS = dict()
        transformList = list()
        explanationDict = dict()
        tokenizer = Tokenizer()
        graphList = list()
        MAX_ITER = 10
        dbUtil = DBUtil(dbConf = 'postgres')

        retrievedFlag = True
        currtime = time.time()
        res = dbUtil.queryWebTables(XList, Y, 2, query)
        print("\tTime consumption of querying web table: " + str(time.time() - currtime))
        if(res):
            for item in res:
                tableList.append(item[0])
            print(len(tableList))
            currtime = time.time()
            reversedQS = dbUtil.reversedQuery_mt(tableList)
            print("\tTime consumption of table retrieval: " + str(time.time() - currtime))
            # for key, tableitem in reversedQS.items():
            #     reversedQS[key] = pd.DataFrame(tableitem)

            # if(len(tableList) > 2000 or len(reversedQS) > 3500):
            #     return None, None
        else:
            res = None
            retrievedFlag = False

        for x, y in zip(XList, Y):
            # print(x, y)
            # xlist = [str(item).lower() for item in x]
            xlist = [tokenizer.tokenize(item, y) for item in x]
            ans = Answer(xlist, str(y).lower(), isExample = True)
            answerList.append(ans)

        graphList = list()
        inputlist = list()
        outputlist = list()
        # for answer in answerList:
        #     try:
        #         gentime = time.time()
        #         g1 = GENERATE(answer.X, answer.Y, reversedQS, verbose = verbose)
        #     except Exception as e:
        #         print("Exception:")
        #         traceback.print_exc()
        #         g1 = None
        #     if(g1):
        #         print('\tTime for graph generation: ' + str(time.time() - gentime))
        #         graphList.append((g1, (answer.X, answer.Y)))

        for answer in answerList:
            inputlist.append(answer.X)
            outputlist.append(answer.Y)
        # print(inputlist, outputlist)
        try:
            gentime = time.time()
            graph = GENERATE_scalable(inputlist, outputlist, reversedQS, verbose = verbose)
        except Exception as e:
            print("Exception:")
            traceback.print_exc()
            graph = None
        finally:
            print('\tTime for graph generation: ' + str(time.time() - gentime))
        if(graph):
            # print('\tTime for graph generation: ' + str(time.time() - gentime))
            if(graph is not None):
                graphList.append(graph)
        
        graphDict = graphList
        print('final graph')

        # for graphtuple in graphList:
        #     # print(graphtuple[1])
        #     key = list(graphtuple[1])
        #     for i in range(len(key)):
        #         if(type(key[i]) is list):
        #             key[i] = tuple(key[i])
        #     key = tuple(key)
        #     val = graphtuple[0]
        #     if(key not in graphDict.keys()):
        #         graphDict[key] = list()
        #     if(type(graphtuple[0]) is list):
        #         graphDict[key].extend(val)
        #     else:
        #         graphDict[key].append(val)

        # reversedQS = dict()
        # tid = 1
        # for item in __filelist__:
        #     file = pd.read_csv(os.path.join(__path__, item), dtype = str)
        #     reversedQS[tid] = {'table': file, 'key': list()}
        #     tid += 1



        # for x, y in zip(XList, Y):
        #     # print(x, y)
        #     xlist = [str(item).lower() for item in x]
        #     ans = Answer(xlist, str(y).lower(), isExample = True)
        #     answerList.append(ans)
        # # print(answerList)
        # if(verbose):
        #     with open('groupcomb.txt', 'w') as f:
        #         f.write('Start combination \n')
        #     with open('groupdetail.txt', 'w') as f:
        #         f.write('Each group \n')
        #     with open('detail_validated.txt', 'w') as f:
        #         f.write('detail_validated:\n')
        #     with open('detail_validated_dedup.txt', 'w') as f:
        #         f.write('Deduplicated detail_validated:\n')
        # graphList = list()
        # for answer in answerList:
        #     try:
        #         gentime = time.time()
        #         g1 = GENERATE(answer.X, answer.Y, reversedQS, verbose = verbose)
        #     except Exception as e:
        #         print("Exception:")
        #         traceback.print_exc()
        #         g1 = None
        #     if(g1):
        #         print('\tTime for graph generation: ' + str(time.time() - gentime))
        #         graphList.append((g1, (answer.X, answer.Y)))
        
        # graphDict = dict()
        # print('final graph')
        # for graphtuple in graphList:
        #     # print(graphtuple[1])
        #     key = list(graphtuple[1])
        #     for i in range(len(key)):
        #         if(type(key[i]) is list):
        #             key[i] = tuple(key[i])
        #     key = tuple(key)
        #     val = graphtuple[0]
        #     if(key not in graphDict.keys()):
        #         graphDict[key] = list()
        #     if(type(graphtuple[0]) is list):
        #         graphDict[key].extend(val)
        #     else:
        #         graphDict[key].append(val)                         

                        
        return graphDict, reversedQS
    
    def __get_row_from_table__(self, table, col, val):
        if((type(col) is int or type(col) is str) and type(val) is str):
            reslist = table.loc[table[table.columns[col]] == val].values.tolist()
        else:
            reslist = table
            for c, v in zip(col, val):
                reslist = reslist.loc[reslist[reslist.columns[c]] == v]
            reslist = reslist.values.tolist()
        if(reslist):
            return reslist[0]
        else:
            return None

    # def discover_csv(self, Q, reversedQS, graph):
    #     ansDict = dict()
    #     for q in Q:
    #         transformation = ''
    #         for Ws in graph.W:
    #             FirstProg = True
    #             currval = ''
    #             for atom in Ws:
    #                 if(atom.id == 'ConstStr'):
    #                     currval = atom.get_value()
    #                 elif(atom.id == 'SubStr'):
    #                     if(FirstProg):
    #                         atom.String = q
    #                         currval = atom.get_value()
    #                         FirstProg = False
    #                     else:
    #                         atom.String = currval
    #                         currval = atom.get_value()
    #                 elif(atom.id == 'Lookup'):
    #                     if(FirstProg):
    #                         atom.String = q
    #                         atom.row = self.__get_row_from_table__(reversedQS[atom.Table]['table'], atom.fromcol, atom.String)
    #                         currval = atom.get_value()
    #                         FirstProg = False
    #                     else:
    #                         atom.String = currval
    #                         atom.row = self.__get_row_from_table__(reversedQS[atom.Table]['table'], atom.fromcol, atom.String)
    #                         currval = atom.get_value()
    #             transformation += currval
    #         if(q not in ansDict.keys()):
    #             ansDict[q] = ''
    #         ansDict[q] = transformation
    #     return ansDict
    
    def discover_csv(self, Q, reversedQS, graph):
        ansDict = dict()
        for q in Q:
            transformation = ''
            for Ws in graph.W:
                FirstProg = True
                currval = ''
                for atom in Ws:
                    if(atom.id == 'ConstStr'):
                        currval = atom.get_value()
                    elif(atom.id == 'SubStr'):
                        if(FirstProg):
                            atom.String = q
                            currval = atom.get_value()
                            FirstProg = False
                        else:
                            atom.String = currval
                            currval = atom.get_value()
                    elif(atom.id == 'Lookup'):
                        if(FirstProg):
                            atom.String = (q[atom.src[0]],)
                            atom.row = self.__get_row_from_table__(reversedQS[atom.Table]['table'], atom.fromcol, atom.String)
                            currval = atom.get_value()
                            FirstProg = False
                        else:
                            atom.String = currval
                            atom.String = list()
                            for val in atom.src:
                                if(val == -1):
                                    atom.String.append(currval)
                                else:
                                    atom.String.append(q[val])
                            atom.String = tuple(atom.String)
                            atom.row = self.__get_row_from_table__(reversedQS[atom.Table]['table'], atom.fromcol, atom.String)
                            currval = atom.get_value()
                transformation += currval
            if(q not in ansDict.keys()):
                ansDict[q] = ''
            ansDict[q] = transformation
        return ansDict