

from typing import List
from data.Answer import Answer

from typing import List, Tuple

class Evaluation:
    def reportGen(self, answerList: List[Answer], groundtruth: List[Tuple], fileLoc = './report.txt'):
        with open(fileLoc, 'w') as f:
            f.write("(x, y) >>> (ground truth) >>> Correctness")
            for answer in answerList:
                for gt in groundtruth:
                    if(answer.X in [gt[0]]):
                        if(answer.Y in [gt[1]]):
                            f.write(str((answer.X, answer.Y)) + '>>>' + str(gt) + '>>> Correct')
                        else:
                            f.write(str((answer.X, answer.Y)) + '>>>' + str(gt) + '>>> Wrong')
                f.write('\n')
                


    def evaluate(self, answerList: List[Answer], groundtruth: List[Tuple]):
        correct = 0
        wrong = 0
        for answer in answerList:
            for gt in groundtruth:
                if(answer.X in [gt[0]]):
                    if(answer.Y in [gt[1]]):
                        correct += 1
                    else:
                        wrong += 1
        if(correct + wrong == 0):
            return 0
        else:
            return float(correct) / float(correct + wrong)