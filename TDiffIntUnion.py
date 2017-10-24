#------------------------------------------------------------
#------------------------------------------------------------
# tdiu.py
#
#----------------------------------------------------------------------
from TableTool import TableTool
from common import *
#----------------------------------------------------------------------
#
class TDiffIntUnion( TableTool ):
    def __init__(self,argv):
	TableTool.__init__(self,2)
	self.kcols1 = []
	self.kcols2 = []
	self.t2Keys = {}
	self.parseCmdLine(argv)

    #---------------------------------------------------------
    def initArgParser(self):
	TableTool.initArgParser(self)
	self.parser.add_option("--k1", dest="k1", 
	    action="append", default = [],
	    metavar="COLUMN(S)",
	    help="Specifies key column(s) for table T1.")

	self.parser.add_option("--k2", dest="k2", 
	    action="append", default = [],
	    metavar="COLUMN(S)",
	    help="Specifies key column(s) for table T2.")


    #---------------------------------------------------------
    #
    def processOptions(self):
	TableTool.processOptions(self)
	if len(self.options.k1) > 0:
	    self.kcols1 = self.parseIntList(self.options.k1)
	if len(self.options.k2) > 0:
	    self.kcols2 = self.parseIntList(self.options.k2)

	nkc1 = len(self.kcols1)
	nkc2 = len(self.kcols2)

	if nkc1 != nkc2:
	    self.parser.error("Same number of key columns must " + \
	    	"be specified for both IDs.")

	
    #---------------------------------------------------------
    def makeKey(self, row, cols):
	key = []
	for c in cols:
	    v = row[c]
	    key.append( v )
	return tuple(key)

