import numpy as np
import itertools

import pandas as pd

def makeCombination(arr, size):
    return itertools.combinations(arr, size)

def identifyKeyCand(table):
    numRows = len(table.index)
    keyCand = set()
    i = 1
    colIndex = [i for i in range(len(table.columns))]
    while(i < len(table.columns)):
        colCand = makeCombination(colIndex, i)
        for col in colCand:
            currData = table.iloc[:, list(col)].values.astype('str')
            currData = np.unique(currData, axis = 0)
            if(int(np.shape(currData)[0]) == numRows):
                keyCand.add(col)
        i += 1
    
    return keyCand

# data = pd.read_csv('benchmarkcsv/costrec.csv')
# print(data)
# print(identifyKeyCand(data))