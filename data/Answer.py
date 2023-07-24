class Answer:
    def __init__(self, x = None, y = None, tid = None, xCol = None, yCol = None, rowid = None, ref = None, isExample = False) -> None:
        self.X = x
        self.Y = y
        self.tableid = tid
        self.xCol = xCol
        self.yCol = yCol
        self.rowid = rowid
        self.ref = ref
        self.score = -1
        self.isExample = isExample
        self.appearIn = list()
        self.directFound = False
        self.Xtoken = self.__processToken(x)
        self.ytoken = self.__processToken(y)


    def __repr__(self) -> str:
        repStr = "X: {}; Y: {}; tid: {}; current score: {}; isExample: {}; Xtokens: {}; Ytokens: {}\n".format(self.X, self.Y, self.tableid, self.score, self.isExample, self.Xtoken, self.ytoken)
        return repStr

    def __str__(self) -> str:
        repStr = "X: {}; Y: {}; tid: {}; current score: {}; isExample: {}; Xtokens: {}; Ytokens: {}\n".format(self.X, self.Y, self.tableid, self.score, self.isExample, self.Xtoken, self.ytoken)
        return repStr

    def __processToken(self, s: str) -> list:
        tokenList = list()
        # print(s)
        if(type(s) is list):
            for string in s:
                lens = len(string.split())
                if(string):
                    for token in string.split(' '):
                        if(token not in tokenList):
                            tokenList.append(token)
                    for i in range(len(string.split())):
                        if(i == 0):
                            continue
                        remaininglen = lens
                        curridx = 0
                        while(remaininglen > 0):
                            currtok = ' '.join(string.split()[curridx: i + 1])
                            remaininglen -= i
                            curridx += i
                            if(currtok not in tokenList):
                                tokenList.append(currtok)
        elif(len(s.split()) == 1):
            return [s.lower()]
        else:
            for token in s.split(' '):
                if(token not in tokenList):
                    tokenList.append(token)
            for i in range(len(s.split())):
                if(i == 0):
                    continue
                remaininglen = len(s.replace(token, '', 1))
                curridx = 0
                while(remaininglen > 0):
                    currtok = ' '.join(s.split()[curridx: i + 1])
                    remaininglen -= i
                    curridx += i
                    if(currtok not in tokenList):
                        tokenList.append(currtok)
        if(tokenList):
            return tokenList
        else:
            return 

    def setScore(self, score) -> int:
        self.score = score
        return self.score

    def getScore(self) -> int:
        return self.score