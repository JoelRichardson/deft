#----------------------------------------------------------------------
#----------------------------------------------------------------------
#
# td.py
#
'''
        td - table difference
Outputs the rows of table 1 that do not occur in table 2, based on keys.
'''
#----------------------------------------------------------------------
from TDiffIntUnion import TDiffIntUnion
from common import *

#----------------------------------------------------------------------
class TDifference (TDiffIntUnion):
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
	    if not keys.has_key(key):
                yield row
