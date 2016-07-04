
from collections import Iterable


def flatten1(l):
    '''
    flatten a list of lists into a list
    [[1],[2]] -> [1,2]
    '''
    return [item for sublist in l for item in sublist]


def flatten(l):
    '''
    flatten recursively
    '''
    return list(flatten_(l))

def flatten_(container):
    for i in container:
        if isinstance(i, Iterable) and not isinstance(i, str):
            for j in flatten(i):
                yield j
        else:
            yield i
