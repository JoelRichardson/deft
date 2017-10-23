#----------------------------------------------------------------------
#----------------------------------------------------------------------
#
# tu.py
#
'''
        tu - table union
Outputs all rows of table1 followed by
all rows of table2 not in table 1, based on a key.
(No assumptions are made about union-compatability, i.e., the tables
are not required to have the same columns.)
'''
#----------------------------------------------------------------------
from TDiffIntUnion import TDiffIntUnion
from common import *

class TUnion (TDiffIntUnion):
    USAGE=__doc__
    def __init__(self,argv):
        TDiffIntUnion.__init__(self,argv)

    #---------------------------------------------------------
    def go(self):
	keys = {}
        for row in self.t1:
	    key = self.makeKey(row, self.kcols1)
	    keys[key] = 1
            yield row

        for row in self.t2:
	    key = self.makeKey(row, self.kcols2)
	    if not keys.has_key(key):
                yield row

