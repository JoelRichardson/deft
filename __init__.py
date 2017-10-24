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

def die(message = None, exitCode = -1):
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write(NL)
    sys.exit(exitCode)

def buildPipeline(args):
    if len(args) == 0:
        die("No arguments.")
    # Subdivide args into sublists split by pipes
    stages = []
    cstage = []
    for a in args:
        if a in ["|","--pipe"]:
            if len(cstage) == 0:
                die("No arguments in pipe stage.")
            stages.append(cstage)
            cstage = []
        else:
            cstage.append(a)
    stages.append(cstage)

    # At this point, stages is a list of lists.
    # Each inner list consists of an operator followed by its parameters
    prev = None
    pipeline = []
    for stage in stages:
        op = stage[0]
        opArgs = stage[1:]
        # create the TableTool instance
        opClass = OPERATION_MAP[op]
        opObj = opClass(opArgs)
        # For stages after the first, plug the previous tool into its input
        if prev:
            if opObj.options.in1 == "-":
                opObj.t1 = prev
            elif opObj.nInputs == 2 and opObj.options.in2 == "-":
                opObj.t2 = prev
        pipeline.append(opObj)
        prev = opObj
    return pipeline

def interpretCommandLine(args):
    pipeline = buildPipeline(args)
    ttobj = pipeline[-1]
    if ttobj.__class__ is not TWrite:
        tw = TWrite(["-o", "-"])
        tw.in1 = ttobj
        ttobj = tw
    for row in ttobj:
        pass
