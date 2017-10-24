#------------------------------------------------------------
#------------------------------------------------------------
#
# tb.py
#
'''
        tb - table bucketize
Table bucketize. Bucketizes a table containing a pair of ID columns.
Each row represents an edge in a bipartite association graph.
To represent unassociated IDs (i.e., those that end up in 1-0
or 0-1 buckets), a null-value string can be declared, and appear
in one ID column or the other. (Such as when performing an
outer-join...see tj.)
'''
#
#----------------------------------------------------------------------
from TableTool import TableTool
from common import *

BUCKETS = [
	"0-1",
	"1-0",
	"1-1",
	"n-1",
	"1-n",
	"n-m",
	]
#----------------------------------------------------------------------
#
class TBucketize( TableTool ):
    USAGE=__doc__
    def __init__(self,argv):
	self.kcols1 = []
	self.kcols2 = []
	self.rows = []
	self.graph = None
	self.bucketFiles = {}
	TableTool.__init__(self,1,argv)

    #---------------------------------------------------------
    def initArgParser(self):
	TableTool.initArgParser(self)
	self.parser.add_option("--k1", dest="k1", 
	    action="append", default = [],
	    metavar="COLUMN(S)",
	    help="Specifies column(s) of first ID.")

	self.parser.add_option("--k2", dest="k2", 
	    action="append", default = [],
	    metavar="COLUMN(S)",
	    help="Specifies column(s) of second ID.")

	self.parser.add_option("-n", "--null-string", dest="nullString", 
	    action="store", default = "", metavar="NULLSTR",
	    help="Specifies string for null values. (Default: empty string)")


    #---------------------------------------------------------
    #
    def processOptions(self):
        #
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
    def makeKey(self, row, cols, prepend):
	key = [prepend]
	for c in cols:
	    v = row[c]
	    if v == self.options.nullString:
		#return (prepend,None)
		return None
	    key.append( v )
	return tuple(key)

    #---------------------------------------------------------
    # Reads the input table and builds the corresponding
    # bipartite graph.
    #
    def buildGraph(self):
	g = BipartiteGraph()
        for inrow in self.t1:
	    k1 = self.makeKey(inrow, self.kcols1, "A")
	    k2 = self.makeKey(inrow, self.kcols2, "B")
	    g.add(k1,k2)
	    if k1 is not None:
		self.rows.append((k1,inrow))
	    else:
		self.rows.append((k2,inrow))
	self.graph = g

    #---------------------------------------------------------
    def computeCC(self):
    	self.cca = CCA(self.graph)
	self.cca.go()

    #---------------------------------------------------------
    def getBid(self, bucket):
	counts = bucket.split("-")
	if counts[0] not in ["0","1"]:
		counts[0] = "n"
	if counts[1] not in ["0","1"]:
		if counts[0] == "n":
		    counts[1] = "m"
		else:
		    counts[1] = "n"
	bid = counts[0] + '-' + counts[1]
	return bid

    #---------------------------------------------------------
    def getBfd(self, bucket):
	return self.bucketFiles[self.getBid(bucket)]

    #---------------------------------------------------------
    def go(self):
	self.buildGraph()
	self.computeCC()
    	for (k,r) in self.rows:
          (cid,bucket) = self.cca.getCid(k)
          row = [cid,bucket,self.getBid(bucket)] + r
          yield row

#----------------------------------------------------------------------
# Class for representing a bipartite graph. The a/b distinction
# is encoded enforced by the a and b parameters to the add method.
#
class BipartiteGraph:
    def __init__(self):
	self.nodes = {}

    def __getneighbors__(self, n, dict):
	if dict.has_key(n):
	    lst = dict[n]
	else:
	    lst = []
	    dict[n] = lst
	return lst

    def add(self, a, b):
	if a is not None:
	    ns = self.__getneighbors__(a, self.nodes)
	    if b is not None:
		if not b in ns:
		    ns.append(b)

	if b is not None:
	    ns = self.__getneighbors__(b, self.nodes)
	    if a is not None:
		if not a in ns:
		    ns.append(a)

    def __str__(self):
	return str(self.nodes)

#----------------------------------------------------------------------
# Class for doing connected component analysis on a bipartite graph.
#
class CCA:
    def __init__(self, bpg):
	self.graph = bpg
	self.visited = {}

	self.cc = {}
	self.cid = 0
	self.na = 0
	self.nb = 0

    def getCid(self, n):
	return self.visited[n]

    def reach(self, n):
	self.visited[n] = self.cid
	if n[1] is not None:
	    if n[0] == "A":
		self.na += 1
	    elif n[0] == "B":
		self.nb += 1
	self.cc[n] = n
	neighbors = self.graph.nodes[n]
	for n2 in neighbors:
	    if not self.visited.has_key(n2):
		self.reach(n2)

    def getCount(self, n):
	if n == 0:
	    return "0"
	elif n == 1:
	    return "1"
	else:
	    return "n"

    def getBucket(self):
	return str(self.na) + "-" + str(self.nb)

    def go(self):
	for n in self.graph.nodes.iterkeys():
	    if not self.visited.has_key( n ):
		self.cc = {}
		self.na = 0
		self.nb = 0
		self.cid += 1
		self.reach(n) 
		for cn in self.cc.iterkeys():
		    self.visited[cn] = (self.visited[cn], self.getBucket())

