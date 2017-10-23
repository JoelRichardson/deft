#------------------------------------------------------------
#------------------------------------------------------------
#
# tjoin.py
#
'''
        tj - table join
Table joining is an algebraic transformation.
It takes two tables as input and produces one 
table as output.

EVALUATION:

INPUT/OUTPUT:

FORMAT:
    Currently supports only tab-delimited files without column
    headers, comment lines, or anything else (TO BE FIXED SOON).

OPTIONS:

    --k1 COLUMNS
    --k2 COLUMNS
        Specifies the column(s) for an equi-join. COLUMNS is a comma-separated
        list of integer column indices (no spaces). --j1 specifies columns 
        from T1, and --j2 specifies an equal number of columns from T2. 
        In order for a pair of rows (r1, r2) to satisfy the join, they
        must: (1) have equal values in corresponding columns named in
        --j1/--j2, and (2) pass any additional command line filters.

        If neither --j1 nor --j2 is specified, tj will perform a
        nested loops join, where the join condition is specified
        by any command line filters. If no filters are specified,
        tj generates the combinatorial cross-product of tuples
        from the input tables.

    expression
        All positional command line arguments are Python expressions that
        either generate output column values from a pair of input rows, 
        or impose additional filtering on the joined pairs.
        They work just like in tf, except that the defined names are
        within the expression are:
            IN1	- the input row from T1
            IN2	- the input row from T2
            OUT	- the output row
        If no expressions are given, the expression "IN1+IN2" is
        used. Thus, the default is to output all columns from both rows.

        Filters (expressions beginning with a '?' character) can be included.

--------------------------------------------------------------------
'''
#----------------------------------------------------------------------
#
from BinaryTableTool import BinaryTableTool
from common import *

class TJoin( BinaryTableTool ):
    USAGE=__doc__
    def __init__(self,argv):
	BinaryTableTool.__init__(self)

	self.jcols1 = []
	self.jcols2 = []

        self.ncols1 = 0
        self.ncols2 = 0

	self.doLeftOuter = False
	self.doRightOuter = False

	self.swappedInputs = False
	self.selfJoin = False
	self.inner = None
	self.parseCmdLine(argv)

    #---------------------------------------------------------
    def initArgParser(self):

	self.parser.add_option("--k1", dest="j1", 
	    action="append", default = [],
	    metavar="COLUMN(S)",
	    help='''Specifies T1 join key column(s). For a multipart key, you can use a comma 
            separated list of column numbers or simply repeat the --k1 option.
            (Remember, column numbers start at 0!)''')

	self.parser.add_option("--k2", dest="j2", 
	    action="append", default = [],
	    metavar="COLUMN(S)",
	    help='''Specifies T2 join key column(s). For a multipart key, you can use a comma 
            separated list of column numbers or simply repeat the --k2 option.
            (Remember, column numbers start at 0!)''')

	self.parser.add_option("-c", "--columns", dest="ocols",
	    action="append", default = [],
	    metavar="COLUMN(S)",
	    help='''Specifies with columns of the input rows should be output, and in what order.
            The two input rows are named 'a' and 'b', and you specify which columns to output
            using Python array syntax. Example: -c "a[1] b a[0] b[3:6]" .
            This says, output col 1 of a, followed by all of b's columns, followed by a's 0-th column,
            followed by a's columns 3,4,5.
            ''')

	self.parser.add_option("--left-outer", dest="dlo", 
	    action="store_true", default = False,
	    help='''Perform left-outer join (default: No). In a left-outer join, every row
            in T1 produces at least one output row: if row in T1 does not match anything in T2,
            the output row contains NULLs in the T2 columns.''')

	self.parser.add_option("--right-outer", dest="dro", 
	    action="store_true", default = False,
	    help='''Performs right-outer join (default: No). In a right-outer join, every row
            in T2 produces at least one output row: if row in T2 does not match anything in T1,
            the output row contains NULLs in the T1 columns.''')

	self.parser.add_option("-n", "--null-string", dest="nullString", 
	    action="store", default = "", metavar="NULLSTR",
	    help="Specifies string to use for NULL values output but left/right outer joins. (Default: empty string)")

	BinaryTableTool.initArgParser(self)
    #---------------------------------------------------------
    #
    def processOptions(self):
	if len(self.options.j1) > 0:
	    self.jcols1 = self.parseIntList(self.options.j1)
	if len(self.options.j2) > 0:
	    self.jcols2 = self.parseIntList(self.options.j2)

	njc1 = len(self.jcols1)
	njc2 = len(self.jcols2)

	if njc1 != njc2:
	    self.parser.error("Same number of join columns must " + \
	    	"be specified for both tables.")

	self.doLeftOuter = self.options.dlo
	self.doRightOuter = self.options.dro

        # matches array access syntax for r1 and r2, e.g., 
        #  r1[0]
        #  r2[1:4]
        #  r2
        #  r1[:-1]
        rex= re.compile(r'^r[12](\[(-?\d+)?:?(-?\d+)?\])?$')
        parts = []
        for oc in self.args:
            tokens = oc.strip().split()
            for t in tokens:
                m = rex.match(t)
                if not m:
                    self.parser.error("Syntax error in column spec.")
                if "[" in t and ":" not in t:
                    parts.append("[%s]"%t)
                else:
                    parts.append(t)
        if len(parts) == 0:
            parts = ['r1','r2']
        expr = "lambda r1, r2: " + "+".join(parts)
        self.fun = eval(expr)

    #---------------------------------------------------------
    # Decide who's inner and who's outer. The inner
    # table is the one that gets loaded, the outer
    # is then scanned to do the join. We want the
    # smaller table to be the inner, which in this
    # program is self.t2. The net effect of this
    # method is to possibly swap self.t1 and self.t2 (and
    # other stuff), and to record that we did so.
    #
    def pickInnerOuter(self):
	self.swappedInputs = False
	self.selfJoin = (self.t1.fileDesc == self.t2.fileDesc)
	if self.selfJoin:
	    return 

	t1sz = self.t1.fileSize()
	t2sz = self.t2.fileSize()
	if t1sz != -1 and (t2sz == -1 or t1sz < t2sz):
	    #self.debug("Swapping input tables.")
	    self.swappedInputs = True
	    self.t1,self.t2 = self.t2,self.t1
	    self.jcols1,self.jcols2 = self.jcols2,self.jcols1
	    self.doLeftOuter,self.doRightOuter = self.doRightOuter,self.doLeftOuter

    #---------------------------------------------------------
    def loadInner(self):
	#self.pickInnerOuter()
	# at this point, self.t2 is the inner, self.t1 is the outer
	self.inner = { }
	if self.doRightOuter:
	    self.innerList=[]

        for i,row in enumerate(self.t2):
            self.ncols2 = len(row)
	    key = self.makeKey(row, self.jcols2)
	    if not self.inner.has_key(key):
		self.inner[key] = [(i,row)]
	    else:
		self.inner[key].append((i,row))

	    if self.doRightOuter:
		self.innerList.append(row)


    #---------------------------------------------------------
    def processPair(self, r1, r2):
	if r1 is None:
	    r1 = [self.options.nullString] * self.ncols1
	if r2 is None:
	    r2 = [self.options.nullString] * self.ncols2
	if self.swappedInputs:
	    row = self.fun(r2,r1)
	else:
	    row = self.fun(r1,r2)
	if row is not None:
	    return row

    #---------------------------------------------------------
    def go(self):
	self.loadInner()
	if self.selfJoin:
            self.ncols1 = self.ncols2
	    for i,rowlist in self.inner.itervalues():
		for outerrow in rowlist:
		    for innerrow in rowlist:
			yield self.processPair(outerrow,innerrow)
	else:
            for outerrow in self.t1:
                self.ncols1 = len(outerrow)
		key = self.makeKey(outerrow, self.jcols1)
		if self.inner.has_key(key):
		    innerRows = self.inner[key]
		    for i,innerrow in innerRows:
			yield self.processPair(outerrow,innerrow)
			if self.doRightOuter:
			    self.innerList[ i ] = None
		elif self.doLeftOuter:
		    yield self.processPair(outerrow, None)
	    if self.doRightOuter:
		unseen = filter(lambda x: x is not None, self.innerList)
		for r in unseen:
		    yield self.processPair(None, r)

