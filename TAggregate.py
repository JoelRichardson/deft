#----------------------------------------------------------------------
# AGGREGATE
#----------------------------------------------------------------------
'''
        ta - table aggregate
Computes aggregate values (such as min/max/count/etc) over a table,
optionally grouped by specified keys (similar to SQL group-by).
The input table is logically partitioned by specifying one or more 
grouping columns; input rows having the same values in these columns
belong to the same partition. Each partition generates one
output row. Each output row contains (at least) the group-by
values that define its partition. Additional columns may
be added that contain specified aggregate functions of the
input rows. If no grouping columns are specified, the aggregates
are computed over the whole table.

Example: Here is a table of information about students.
    name    resident?  year    GPA
    ====    ======     ====    ===
    John    n          1       3.5
    Mary    n          2       3.7
    Jean    y          1       2.9
    Joe     n          1       2.5
    Bill    y          3       3.4
    Bob     y          3       3.6
    Alice   y          4       4.0
    Larry   n          4       3.2

Suppose we want to know the average GPA broken down by
year in school and residence status. The two columns, year and
resident, are the group-by columns. Each distinct value
combination (e.g., "3" and "y") defines a partition
of the input rows. The GPAs are averaged within each 
partition. Thus the output would be:

    1	n	3.0
    1	y	2.9
    2	n	3.7
    3	y	3.5
    4	y	4.0
    4	n	3.2

OUTPUT
    The output table contains each of the group-by columns
    (in the order specified) followed by one column for each aggregation 
    specifier (in the order specified). The output contains one row
    for each partition of the input table induced by the group-by columns.

'''
#----------------------------------------------------------------------
from TableTool import TableTool
from common import *
#----------------------------------------------------------------------
#
# CONSTANTS for the various aggregation function names
#
COUNT	= "count"
FIRST	= "first"
LAST	= "last"
LIST	= "list"
SUM	= "sum"
SUMSQ	= "sumsq"
MIN	= "min"
MAX	= "max"
MEAN	= "mean"
AVG	= "avg"
VAR	= "var"
SD	= "sd"

#_STAT_FUNCS = [SUM,SUMSQ,MIN,MAX,MEAN,AVG,VAR,SD]
_STAT_FUNCS = [SUM,SUMSQ,MIN,MAX,MEAN,AVG]

_ALL_FUNCS = [COUNT,LIST,FIRST,LAST] + _STAT_FUNCS

#----------------------------------------------------------------------
class TAggregate( TableTool) :
    USAGE=__doc__
    def __init__(self,argv):
	self.maxColIndex = 0
	self.currentLine = None
	self.currentLineNum = 0

	self.gbColumns = []		# list of integer col indexes
	self.accumulatorClasses = []	# list of Accumulator classes
	self.accumulatorColumns = []	# corresp. list of columns to accum
	self.accumulatorXtraArg = []	# corresp extra arg to accum constructor
	self.col2stats = {}		# maps col# to Statistics accum
	self.outSpecifiers = []		# 

	self.partitions = {}

	TableTool.__init__(self,1,argv)

    #---------------------------------------------------------
    # Parses the command line. Options and positional args
    # assigned to self.options and self.args, resp.
    #
    def initArgParser(self):
	TableTool.initArgParser(self)

	self.parser.add_option("-g", "--group-by", 
	    metavar="COLUMN(S)",
	    action="append", dest="groupByColumns", default=[], 
	    help=GBHELP)

	self.parser.add_option("-a", "--aggregate", 
	    metavar="FCN:COLUMN",
	    action="append", dest="aggSpecs", default=[], 
	    help=AGGHELP)

	self.parser.add_option("--stream", 
	    action="store_true", dest="streamMode", default=False, 
	    help=STREAMHELP)


    #---------------------------------------------------------
    def processOptions(self):
        #
        TableTool.processOptions(self)

	# group-by columns
	#
	for g in self.options.groupByColumns:
	    self.addGroupByColumn(g)

        # aggregation ops
        #
	for a in self.options.aggSpecs:
	    self.addAggregation(a)

    #----------------------------------------------------------------------
    def addGroupByColumn(self,g):
	#split on any string of non-digits
	gcols = re.split('[^0-9]+', g)
	for gc in gcols:
	    if gc=="":
		continue
	    igc = int(gc)
	    if igc not in self.gbColumns:
		self.gbColumns.append(igc)
		self.maxColIndex = max(self.maxColIndex, igc)


    #----------------------------------------------------------------------
    def addAggregation(self,arg):
	tokens = string.split(arg, COLON,2)
	func=tokens[0]
	colIndex = None
	if len(tokens) > 1:
	    colIndex = int(tokens[1])
	#xtra = ''
	xtra = None
	if len(tokens) > 2:
	    xtra = tokens[2]

	accClass = _FUNC2CLASS[func]
	if accClass is Statistics:
	    if not self.col2stats.has_key(colIndex):
		self.col2stats[colIndex] = len(self.accumulatorClasses)
		self.accumulatorClasses.append(Statistics)
		self.accumulatorColumns.append(colIndex)
		self.accumulatorXtraArg.append(None)
	    self.outSpecifiers.append( (self.col2stats[colIndex], func, xtra) )
	else:
	    self.outSpecifiers.append( (len(self.accumulatorClasses),None,None) )
	    self.accumulatorClasses.append(accClass)
	    self.accumulatorColumns.append(colIndex)
	    self.accumulatorXtraArg.append(xtra)

    #---------------------------------------------------------
    # Creates a new list of accumulator objects corresponding
    # to the command line specifications.
    #
    def newAccumulatorList(self):
	alist = []
	i=0
	for aclass in self.accumulatorClasses:
	    alist.append(aclass(self.accumulatorColumns[i], \
	    			self.accumulatorXtraArg[i]))
	    i=i+1
	return alist

    #---------------------------------------------------------
    def flush(self, gbcols, aggs):
        row = list(gbcols)
        for (i,arg,xtra) in self.outSpecifiers:
            if arg is None:
                apnd = aggs[i]
            else:
                apnd = aggs[i].getResult(arg,xtra)
            apnd = str(apnd)
            row.append(apnd)
        return row


    #---------------------------------------------------------
    def goStream(self):
        prevKey = None
        alist = None
        for row in self.t1:
            gbkey = self.makeKey(row,self.gbColumns)
            if gbkey != prevKey:
                if prevKey:
                    yield self.flush(prevKey, alist)
                alist = self.newAccumulatorList()
            for a in alist:
                a.nextRow(row)
            prevKey = gbkey    
        if prevKey:
            yield self.flush(prevKey, alist)

    #---------------------------------------------------------
    def goNoStream(self):
        for row in self.t1:
            gbkey = self.makeKey(row,self.gbColumns)
            if not self.partitions.has_key(gbkey):
                self.partitions[gbkey]=self.newAccumulatorList()
            for a in self.partitions[gbkey]:
                a.nextRow(row)

	for (part,aggs) in self.partitions.items():
            yield self.flush(part, aggs)

    #---------------------------------------------------------
    def go(self):
        if self.options.streamMode:
            return self.goStream()
        else:
            return self.goNoStream()

#----------------------------------------------------------------------
# Abstract superclass. An accumulator is something that processes
# a sequence of values and produces an output value.
#
class Accumulator:
    def __init__(self, colIndex, xtra=''):
	self.colIndex = colIndex
	self.xtra = xtra
	#print "Created " + self.__class__.__name__ + ", Column " + `colIndex`

    def debug(self, s):
	sys.stderr.write(s)
	sys.stderr.write("\n")

    def nextRow(self, row):
	if self.colIndex is None:
	    self.nextValue(None)
	else:
	    self.nextValue(row[self.colIndex])

    def nextValue(self, value):
	raise "UnimplementedAbstractMethod: nextValue", self

    def getResult(self,arg=None):
	raise "UnimplementedAbstractMethod: nextResult", self

    def __str__(self):
	return str(self.getResult())

#----------------------------------------------------------------------
# If instantiated with a column index, counts number of
# distinct values in that column (within its partition).
# If instantiated with column index == None, simply counts
# the number of rows in its partition.
#
class Counter( Accumulator ):

    def __init__(self, ci, xtra=''):
	Accumulator.__init__(self, ci, xtra)
	self.countValues = (ci is not None)
	self.count = 0
	self.values = {}

    def nextValue(self, value):
	self.count = self.count + 1
	if self.countValues:
	    self.values[value] = 1

    def getResult(self):
	if self.countValues:
	    return len(self.values)
	else:
	    return self.count


#----------------------------------------------------------------------
# Accumulates the values in a list.
#
class Concatenator( Accumulator ):

    def __init__(self, ci, xtra=''):
	Accumulator.__init__(self, ci, xtra)
	self.list = []
	self.separator = ','
	self.prefix = ''
	self.suffix = ''
	lx = -1
	if xtra != None:
	    lx = len(xtra)

	if lx == 0:
	    self.separator = ''
	    self.prefix = ''
	    self.suffix = ''
	elif lx==1:
	    self.separator = xtra
	    self.prefix = ''
	    self.suffix = ''
	elif lx==2:
	    self.separator = ''
	    self.prefix = xtra[0]
	    self.suffix = xtra[1]
	elif lx==3:
	    self.separator = xtra[1]
	    self.prefix = xtra[0]
	    self.suffix = xtra[2]

    def nextValue(self, value):
	self.list.append(value)

    def getResult(self):
	return self.list

    def __str__(self):
	return self.prefix + \
	    string.join( map(str,self.list), self.separator) + \
	    self.suffix


#----------------------------------------------------------------------
# Returns the first value
#
class FirstValue( Accumulator ):

    def __init__(self, ci, xtra=''):
	Accumulator.__init__(self, ci, xtra)
	self.value = None
	self.first = True

    def nextValue(self, value):
	if self.first:
	    self.value = value
	    self.first = False

    def getResult(self):
	return self.value

    def __str__(self):
	return str(self.value)

#----------------------------------------------------------------------
# Returns the last value
#
class LastValue( Accumulator ):

    def __init__(self, ci, xtra=''):
	Accumulator.__init__(self, ci, xtra)
	self.value = None

    def nextValue(self, value):
	self.value = value

    def getResult(self):
	return self.value

    def __str__(self):
	return str(self.value)


#----------------------------------------------------------------------
#----------------------------------------------------------------------
# Computes statistics over the sequence of values.
#
class Statistics(Accumulator):

    def __init__(self, ci, xtra=''):
	Accumulator.__init__(self, ci, xtra)
	self.n = None
	self.sum = None
	self.sumsq = None
	self.min = None
	self.max = None

    def nextValue(self, value):
	value = float(value)
	if self.n is None:
	    self.n = 1
	    self.sum = value
	    self.sumsq = value*value
	    self.min = value
	    self.max = value
	else:
	    self.sum = self.sum + value
	    self.sumsq = self.sumsq + value*value
	    self.n = self.n + 1
	    self.min = min(self.min, value)
	    self.max = max(self.max, value)

    def getResult(self,field=None,xtra=''):
	rval = {}
	rval[COUNT] = self.n
	rval[SUM] = self.sum
	rval[SUMSQ] = self.sumsq
	rval[MIN] = self.min
	rval[MAX] = self.max
	if self.n == 0:
	    rval[MEAN] = 0
	else:
	    rval[MEAN] = float(self.sum) / self.n
	rval[AVG] = rval[MEAN]
	'''
	if self.n > 1 and self.min != self.max:
	  try:
	    rval[VAR] = (self.n*(self.sumsq) - self.sum*self.sum)/(self.n*(self.n-1))
	    rval[SD] = rval[VAR] ** 0.5
	  except:
	    self.debug( "\n\n???????\n\n" )
	    self.debug( "COUNT=%g"%rval[COUNT] )
	    self.debug( "SUM=%g"%rval[SUM] )
	    self.debug( "SUMSQ=%g"%rval[SUMSQ] )
	    self.debug( "MIN=%g"%rval[MIN] )
	    self.debug( "MAX=%g"%rval[MAX] )
	    self.debug( "VAR=%g"%rval[VAR] )
	    self.debug( "SD=%g"%rval[SD] )
	else:
	    rval[VAR] = 0.0
	    rval[SD] = 0.0
	'''

	if field is None:
	    return rval
	else:
	    return rval[field]

_FUNC2CLASS = {
}

_FUNC2CLASS[COUNT] = Counter
_FUNC2CLASS[LIST] = Concatenator
_FUNC2CLASS[FIRST] = FirstValue
_FUNC2CLASS[LAST] = LastValue
_FUNC2CLASS[SUM] = Statistics
_FUNC2CLASS[SUMSQ] = Statistics
_FUNC2CLASS[MIN] = Statistics
_FUNC2CLASS[MAX] = Statistics
_FUNC2CLASS[MEAN] = Statistics
_FUNC2CLASS[AVG] = Statistics
_FUNC2CLASS[VAR] = Statistics
_FUNC2CLASS[SD] = Statistics

GBHELP='''Group-by column(s). Specifies the columns used to group the computation.
Each distinct value combination in the
input generates one row in the output. All input rows having the same 
values in the group-by columns are combined (aggregated) into a 
single output row.  If no group-by columns are specified, the entire input is
considered a single partition. The output consists of a single
row of table aggregates.
'''
AGGHELP='''Specifies an aggregation of an input column, adding a column
to the output. FCN has the form: func:arg:arg:..., where func is one of the aggregation
functions listed below, and arg's depend on the function.

    Aggregation Functions:
    -sum:<column>	- sum of values
    -sumsq:<column>     - sum of squared values
    -min:<column>       - minimum value
    -max:<column>       - minimum value
    -mean:<column>      - mean value
    -avg:<column>       - same as mean:<column>
    -var:<column>       - variance of values
    -sd:<column>        - standard deviation of values
    -count:<column>     - counts number of distinct values in this column in each partition
    -count              - counts number of input rows in each partition

    first:<column> - outputs column value for first member of partition

    last:<column> - outputs column value for last member of partition

    list:<column>[:<pss>] - concatenates input values into a string list
        By default, items are separated by a comma and no prefix/suffix
        is added. Optional <pss> explicitly specifies prefix, separator,
        and suffix as single characters.

        len(<pss>) :	Effect is:
        ============	==========
           1		separator = <pss>, prefix=suffix=''	
           2		prefix=pss[0], sep='', suffix=pss[1]
           3		prefix=pss[0], sep=pss[1], suffix=pss[2]
'''
STREAMHELP='''By default, all input rows are read before any output is generated. If the input is already sorted
on the group-by column(s), you can specify this option to cause output to be generated in stream-fashion, 
greatly reducing both memory usage and lag time. WARNING: If you specify this option and the input is *not* sorted properly,
you will get garbage.
'''
