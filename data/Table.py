class Table:
    def __init__(self, tid, prior = 0.5) -> None:
        self.tid = tid
        self.score = -1
        self.prior = prior

    def __repr__(self) -> str:
        repStr = "tid: {}; score: {}".format(self.tid, self.score)
        return repStr

    def __str__(self) -> str:
        repStr = "tid: {}; score: {}".format(self.tid, self.score)
        return repStr

    def setScore(self, score, init = False):
        if(init):
            self.score = score
        else:
            self.score = score
        return self.score

    def getScore(self):
        return self.score
    