#----------------------------------------------------------------------
#----------------------------------------------------------------------
#
# ti.py
#
'''
        ti - table intersection
Outputs the rows of table1 that also occur in table2, based on keys.
'''
#
#----------------------------------------------------------------------
from TDiffIntUnion import TDiffIntUnion
from common import *

class TIntersection (TDiffIntUnion):
    USAGE=__doc__
    def __init__(self,argv):
        TDiffIntUnion.__init__(self,argv)

    #---------------------------------------------------------
    def go(self):
	keys = {}
        for row in self.t2:
	    key = self.makeKey(row, self.kcols2)
	    keys[key] = 1

        for row in self.t1:
	    key = self.makeKey(row, self.kcols1)
	    if keys.has_key(key):
                yield row

