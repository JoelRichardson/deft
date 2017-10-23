#------------------------------------------------------------
# TFilter.py
#
'''
            tf - table filter
General table computation/selection/projection filter.
This tool allows you to: restrict and/or reorder columns; 
compute new columns; filter rows for those satisfying a condition.

GENERATORS AND FILTERS:

    Each positional argument is a Python expression, optionally beginning
    with a '?' character.
    Expressions without a leading '?' generate a output columns.
    We'll call these generators.
    Expression with a leading '?' filter the input.
    We'll call these filters.
    In all cases, Python rules govern the expression syntax.

    If no generators are specified on the command line, 
    the output rows have all the columns of the input rows.
    If generators are specified, they determine the output columns:
    each generator adds one or more columns. Generator order determines 
    columns order.

EVALUATION:

    For each input row, the argument expressions are evaluated in the 
    order given. If a given expression is a generator, its value sets 
    the next output column; if the value is a list of n items, 
    it sets the next n output columns.
    If an expression is a filter, it is evaluated and the
    value interpreted as a Boolean. If True, processing on 
    the current input row continues. If False, that row is skipped.
    Multiple filters are AND-ed. To do something more complicated,
    remember that a filter can be any Python expression.

    The following names are defined and can be used within
    an expression:
     
    IN	the current input row. This is a list containing
        the values from each column. The ith column's
        value is written: IN[i]. Column numbers start
        at 0. 

    string	the Python string ligrary

    re	the Python regular expression library

    math	the Python math library

    all the __builtin__ functions

EXAMPLE:
    ... | tf ?IN[2] IN[1] IN[3]*IN[4] | ...

    Reads table from stdin. For those rows where column[2] is
    not 0/empty/null/False, it outputs a row containing
    column 1 and the product of columns 3 and 4.
'''
from UnaryTableTool import UnaryTableTool
from common import *

class TFilter ( UnaryTableTool ) :
    USAGE=__doc__
    def __init__(self,argv):
	UnaryTableTool.__init__(self)
	self.parseCmdLine(argv)

    def initArgParser(self):
        UnaryTableTool.initArgParser(self)

	self.parser.add_option("--exec-file", dest="execFile", default=None,
	    action="store",
	    metavar="FILE",
	    help="Execs the code in FILE, e.g., for defining functions to " +\
	       "use in command line filters and generators.")

	self.parser.add_option("--expr-file", dest="exprFiles", default=[],
	    action="append",
	    metavar="FILE",
	    help="Loads expressions (filters/generators) from FILE." + \
	    	" Expression loaded from files are evaluated before " + \
		"command line expressions.")

    #---------------------------------------------------------
    # Subclasses should override to add processing for
    # additional options. 
    #
    def processOptions(self):
	self.loadExprs()

    #---------------------------------------------------------
    #
    def loadExprs(self):
	exprs = []
	for f in self.options.exprFiles:
	    efd = open(f,'r')
	    line = efd.readline()
	    while(line):
		if line[0] not in [NL,HASH]:
		    exprs.append(string.strip(line))
		line = efd.readline()
	    efd.close()

	if len(exprs) > 0:
	    self.args = exprs + self.args
	self.compileFunctions(self.args)

	# Create the global context in which expressions
	# will be evaluated.
	s = "import sys\nimport string\nimport re\nimport math\n"
	if self.options.execFile is not None:
	    fd=open(self.options.execFile,'r')
	    s = s + fd.read()
	    fd.close()
	exec(s,self.functionContext)


    #---------------------------------------------------------
    # Compiles positional command line arguments into generator
    # and/or filter functions. A parallel list of booleans
    # indicates whether a function is a filter or a generator.
    #
    def compileFunctions(self, args):
	noGenerators = True
	for arg in args:
	    isf = (arg[0] == '?')
	    self.functions.append( self.makeFunction( arg ) )
	    self.isFilter.append( isf )
	    noGenerators = noGenerators and isf

	if noGenerators:
	    if self.ninputs==1:
		self.functions.append( \
		    self.makeFunction( "IN" ) )
		self.isFilter.append( False )
	    elif self.ninputs==2:
		self.functions.append( \
		    self.makeFunction( "IN1+IN2" ) )
		self.isFilter.append( False )

    #---------------------------------------------------------
    # Given a string expression, returns a callable object that
    # evaluates it. The arguments to the function are IN1 and IN2. 
    #
    def makeFunction(self, expr):
	if expr[0] == '?':
	    expr = 'bool(' + expr[1:] + ')'
	if self.ninputs == 1:
	    s = "lambda IN: " + expr
	elif self.ninputs == 2:
	    s = "lambda IN1, IN2: " + expr
	return eval(s, self.functionContext)

    #---------------------------------------------------------
    # Evaluates the list of functions to generate zero
    # or one output rows. Each function is evaluated
    # in order. If f is a filter, and it evaluates to
    # False, the function returns, and no row is output.
    # If f is a generator, its value(s) is(are)
    # appended as the next output column(s). (The output
    # row grows as the functions evaluate.)
    #
    # If no filter fails, writes the output row,
    # and increment the row count.
    #
    def generateOutputRow(self, r1, r2=None):
	outrow = [ ]
	i=-1
	for f in self.functions:
	    if self.ninputs==1:
		x=f(r1)
	    else:
		#print "f", f
		#print "r1", r1
		#print "r2", r2
		x=f(r1,r2)

	    i=i+1
	    if self.isFilter[i]:
		if x:
		    continue
		else:
		    return None

	    # generate output column(s)
	    if type(x) is types.ListType:
		outrow = outrow + x
	    elif type(x) is types.TupleType:
		outrow = outrow + list(x)
	    else:
		outrow.append(x)

	# end for-loop
	return outrow
	
    #---------------------------------------------------------
    def go(self):
        for inrow in self.t1:
	    outrow = self.generateOutputRow(inrow)
	    if outrow is not None:
              yield outrow

