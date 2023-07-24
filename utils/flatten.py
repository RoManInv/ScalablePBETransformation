from typing import List


def __deduplicate(obj: List) -> List:
    newrs = list()
    for item in obj:
        if(item not in newrs):
            newrs.append(item)
    return newrs

def __flatten_init(obj) -> List:
    flatlist = list()
    if(isinstance(obj, list) or isinstance(obj, tuple)):
        for item in obj:
            flatlist.extend(__flatten_init(item))
    else:
        flatlist.append(obj)
    return flatlist

def flatten(obj) -> List:
    flatlist = __flatten_init(obj)
    flatlist = __deduplicate(flatlist)
    return flatlist