import sys

from common        import *
from FJoin         import FJoin
from TAggregate    import TAggregate
from TBucketize    import TBucketize
from TClosure      import TClosure
from TDifference   import TDifference
from TFilter       import TFilter
from TIntersection import TIntersection
from TJoin         import TJoin
from TPartition    import TPartition
from TRead         import TRead
from TSort         import TSort
from TUnion        import TUnion
from TXpand        import TXpand
from TWrite        import TWrite

#----------------------------------------------------------------------
# Maps string to operator classes
#
OPERATION_MAP = {
    'fj' : FJoin,
    'ta' : TAggregate,
    'tb' : TBucketize,
    'tc' : TClosure,
    'td' : TDifference,
    'tf' : TFilter,
    'ti' : TIntersection,
    'tj' : TJoin,
    'tp' : TPartition,
    'tr' : TRead,
    'ts' : TSort,
    'tu' : TUnion,
    'tx' : TXpand,
    'tw' : TWrite,
    }

def log(message):
    sys.stderr.write(message)

def die(message = None, exitCode = -1):
    if message is not None:
        log(message)
        log(NL)
    sys.exit(exitCode)

def buildPipeline(tokens):
    if len(tokens) == 0:
        return None
    #
    def addSeg(p, pclass, pargs):
        p2 = pclass(pargs)
        if p:
            if p2.ninputs > 0 and p2.options.in1 == "-":
                p2.t1 = p
            elif p2.ninputs > 1 and p2.options.in2 == "-":
                p2.t2 = p
        pargs[:] = []
        return p2
    #
    pclass = None
    p = None
    pargs = []
    i = 0
    stack = []
    while i < len(tokens):
        t = tokens[i]
        if t == "(":
            stack.append((pclass, p, pargs, i))
            pclass = None
            p = None
            pargs = []
        elif t == ")":
            pp = addSeg(p, pclass, pargs)
            (pclass, p, pargs, j) = stack.pop()
            pargs.append(pp)
        elif t == "|":
            p = addSeg(p, pclass, pargs)
        elif i==0 or tokens[i-1] in ["(","|"]:
            pclass = OPERATION_MAP.get(t,None)
            if pclass is None: die("Unknown operator specified: " + t)
        else:
            pargs.append(t)
        i = i + 1

    p=addSeg(p, pclass, pargs)
    #
    return p

def pp(p, d=0):
    indent = "  "*d
    print indent, "%s(%d)"%(p.__class__.__name__, p.id)
    if p.ninputs == 0:
        print indent, "f=", p.options.filename
        if type(p.options.filename ) is not types.StringType:
            print indent, "Huh."
            pp(p.options.filename, d+1)
    if p.ninputs > 0:
        print indent, "t1="
        pp(p.t1, d+1)
    if p.ninputs > 1:
        print indent, "t2="
        pp(p.t2, d+1)


def interpretCommandLine(args):
    if type(args) is types.StringType:
        args = args.strip().split()
    ttobj = buildPipeline(args)
    if ttobj.__class__ is not TWrite:
        tw = TWrite(["-o", "-"])
        tw.t1 = ttobj
        ttobj = tw

    #pp(ttobj)

    for row in ttobj:
        pass
