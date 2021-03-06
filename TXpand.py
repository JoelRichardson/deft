#------------------------------------------------------------
#------------------------------------------------------------
# txpand.py
#
'''
        tx - table expand
Expands list-valued columns in the input.
Each input rows generates one or more output rows.
The number of rows generated by an input row equals
the number of elements in the list-valued column being
expanded. Multiple columns may be expanded at once.
In that case, they are expanded "in parallel". That
is, for a given input row, the j-th output row 
contains the j-th list element from each of the expanded
columns. 
'''
#----------------------------------------------------------------------
from TableTool import TableTool
from common import *

DEFAULT_PSS='[,]'

#----------------------------------------------------------------------
#
class TXpand ( TableTool ) :
    USAGE=__doc__
    def __init__(self,argv):
	self.xpColumns = [] # list (col,pref,sep,suff)
	TableTool.__init__(self,1,argv)

    #---------------------------------------------------------
    def initArgParser(self):
	TableTool.initArgParser(self)
	self.parser.add_option("-x", "--expand", 
	    action="append", dest="xpSpecs", default=[], 
	    metavar="COL[:PSS]",
	    help="Expand column COL. " + \
		"Use PSS as prefix/sep/suffix (Optional. Default=','). ")

    #---------------------------------------------------------
    def processOptions(self):
        TableTool.processOptions(self)
	for spec in self.options.xpSpecs:
	    self.processXspec(spec)

    #---------------------------------------------------------
    def processXspec(self, spec):
	tokens = string.split(spec, ':', 1)
	#
	col = int(tokens[0])
	pss = DEFAULT_PSS
	if len(tokens) == 2 and len(tokens[1]) > 0:
	    pss = tokens[1]
	#
	self.separator = ','
	self.prefix = ''
	self.suffix = ''
	lx = -1
	if pss != None:
	    lx = len(pss)

	if lx == 0:
	    self.separator = ''
	    self.prefix = ''
	    self.suffix = ''
	elif lx==1:
	    self.separator = pss
	    self.prefix = ''
	    self.suffix = ''
	elif lx==2:
	    self.separator = ''
	    self.prefix = pss[0]
	    self.suffix = pss[1]
	elif lx==3:
	    self.separator = pss[1]
	    self.prefix = pss[0]
	    self.suffix = pss[2]

	self.xpColumns.append( (col,self.prefix,self.separator,self.suffix) )

    #---------------------------------------------------------
    # Parses a string encoded list into an actual list.
    # 
    #
    def expandValue(self, value, prefix, sep, suffix, conv=None):
	if type(value) is not types.StringType:
	    return None

	a=len(prefix)
	b=len(value) - len(suffix)

	valPrefix = value[0:a]
	if valPrefix != prefix:
	    raise "SyntaxError"
	valSuffix = value[b:]
	if valSuffix != suffix:
	    raise "SyntaxError"

	valItems = map(conv, re.split(sep, value[a:b]))
	return valItems

    #---------------------------------------------------------
    def expandRow(self, row):
	xvals = [] # list of (col, expanded-val-list)
	nxr = 1	   # number of expanded rows generated by this row

	# expand all the specified columns.
	# determine number of expanded rows.
	#
	for (col,pfx,sep,sfx) in self.xpColumns:
	    xvs = self.expandValue(row[col],pfx,sep,sfx)
	    if xvs is not None:
		xvals.append( (col,xvs) )
		nxr = max( nxr, len(xvs) )

	xrows = []
	i=0
	while i < nxr:
	    xrow = row[:]
	    for (col, xvlist) in xvals:
		if i < len(xvlist):
		    xrow[col] = xvlist[i]
		else:
		    xrow[col] = ''
	    xrows.append(xrow)
	    i = i+1

	return xrows

    #---------------------------------------------------------
    def go(self):
      for inrow in self.t1:
	for xr in self.expandRow(inrow):
            yield xr


