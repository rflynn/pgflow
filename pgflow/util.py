
from itertools import chain


def flatten(container):
    return list(flatten_(container))

def flatten_(container):
    for i in container:
        try:
            for j in flatten(i):
                yield j
        except:
            yield i

def flatten__(l):
    #x = list(chain.from_iterable(l))
    x = list(chain(*l))
    print('flatten', l, x)
    return x

