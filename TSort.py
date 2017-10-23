#----------------------------------------------------------------------
#----------------------------------------------------------------------
#
# ts.py
#
'''
        ts - table sort
Sorts a table based on column(s) and sort direction(s).
'''
#
#----------------------------------------------------------------------
#
from UnaryTableTool import UnaryTableTool
from common import *

class TSort ( UnaryTableTool ) :
    USAGE=__doc__
    def __init__(self,argv):
	UnaryTableTool.__init__(self)
	self.rows = []
	self.parseCmdLine(argv)

    #---------------------------------------------------------
    def initArgParser(self):
	self.parser.add_option("-k", dest="sortKeys", 
	    action="append", default = [],
	    metavar="COL[:r]",
	    help="Specifies column to sort on , with optional 'r' specifying " +\
	         "to reverse the sort order. Repeatible, for specifying multilevel sort.")
	UnaryTableTool.initArgParser(self)

    #---------------------------------------------------------
    def processOptions(self):
	nsk = []
	for skey in self.options.sortKeys:
	    reverse=False
	    if skey.endswith(":r"):
	        reverse=True
		skey = skey[:-2]
	    elif skey.endswith("r"):
	        reverse=True
		skey = skey[:-1]
	    col=int(skey)
	    nsk.append( (col, reverse) )
	self.options.sortKeys = nsk
	#self.parser.error("...")

    #---------------------------------------------------------
    def doSort(self, rows, column, reverse):
	kfun = lambda row, c=column: row[c]
        rows.sort(None,kfun,reverse)

    #---------------------------------------------------------
    def go(self):
	self.rows = []
        for row in self.t1:
	    self.rows.append(row)

	self.options.sortKeys.reverse()
	for (col,rev) in self.options.sortKeys:
	    self.doSort(self.rows, col, rev)

	for row in self.rows:
              yield row
