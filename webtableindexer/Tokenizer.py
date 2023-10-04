from utils.Singleton import SingletonMeta

import nltk
import string

class Tokenizer(metaclass = SingletonMeta):
    def __init__(self) -> None:
        pass

    # def tokenize(self, text):
    #     stopwords = ['a','the','of','on','in','an','and','is','at','are','as','be','but','by','for','it','no','not','or',
    #                  'such','that','their','there','these','to','was','with','they','will',  'v', 've', 'd']#, 's']
    #     punct = [',', '.', '!', ';', ':', '?', "'", '"']
    #     try:
    #         tok = ' '.join([w for w in nltk.tokenize.casual_tokenize(text, preserve_case = False) if w not in stopwords])
    #     except:
    #         return None
    #     # cleaned = re.sub('[\W_]+', ' ', str(text).encode('ascii', 'ignore').decode('ascii')).lower()
    #     # feature_one = re.sub(' +', ' ', cleaned).strip()
        
    #     # for x in stopwords:
    #     #     feature_one = feature_one.replace(' {} '.format(x), ' ')
    #     #     if feature_one.startswith('{} '.format(x)):
    #     #         feature_one = feature_one[len('{} '.format(x)): ]
    #     #     if feature_one.endswith(' {}'.format(x)):
    #     #         feature_one = feature_one[:-len(' {}'.format(x))]
    #     return tok

    def __punc_ident__(self, src: str) -> set:
        return set([i for i in src if i in string.punctuation])
    
    def tokenize(self, text: str, src: str = None) -> str:
        stopwords = ['a','the','of','on','in','an','and','is','at','are','as','be','but','by','for','it','no','not','or',
                     'such','that','their','there','these','to','was','with','they','will',  'v', 've', 'd']#, 's']
        text = text.lower()
        if(src is None):
            return text
        puncSet = self.__punc_ident__(src)
        text = "".join([c for c in text if (c.isalnum() or c in puncSet or c == " ") and c not in stopwords])
        return text
        
