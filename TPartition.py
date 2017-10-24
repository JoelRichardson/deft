#------------------------------------------------------------
#------------------------------------------------------------
#
# tp.py
#
'''
Table Partition. Routes each input row to one of n output files, based
on substituting a column value into a name template. 
'''
# 
#----------------------------------------------------------------------
#
from TableTool import TableTool
from common import *

class TPartition( TableTool ):
    USAGE=__doc__
    def __init__(self,argv):
	TableTool.__init__(self,1)
	self.pcols = []
	self.fname2ofd = {}
	self.pval2fname = {}
	self.parseCmdLine(argv)

    #---------------------------------------------------------
    def initArgParser(self):
	TableTool.initArgParser(self)
	self.parser.add_option("-p", dest="pcol", 
	    action="store", default = None, type="int",
	    metavar="COLUMN",
            help="Specifies column to partition on. Remember: column numbers start at 0!")

	self.parser.add_option("-L", "--limit", dest="limit", 
	    action="store", default = 100, type="int",
	    metavar="LIMIT",
	    help='''
Limits the number of files created, as a safety feature.
Default is 100.
Setting it to -1 makes this number unlimited (be careful).
            ''')

	self.parser.add_option("-t", dest="tmplt",
	    action="store", default = None,
	    metavar="FILE",
	    help= 
'''Output file name template. The output file for a given row is determined by substituting
the value of that row's partition column for the string "%s" in the template. 
Example: partition a GFF3 file into files by chromosome. The template might be something
like "./mygffdata.chr%s.gff3".
''')
	self.parser.add_option("-T", "--tee", dest="tee",
	    action="store_true", default = False,
	    help= 
'''
If true, the partition operator passes all input rows to its output. This allows a pipeline to
continue after a partitioning operator.
''')

    #---------------------------------------------------------
    #
    def processOptions(self):
	TableTool.processOptions(self)
        if self.options.tmplt is None:
            self.parser.error("No filename template specified.")
        if "%s" in self.options.tmplt and self.options.pcol is None:
            self.parser.error("No partition column specified.")
        if self.options.pcol is not None and "%s" not in self.options.tmplt: 
            self.parser.error("Partition column specified but template has no '%s'.")

    #---------------------------------------------------------
    def getOutputFileName(self, pval):
	if "%s" in self.options.tmplt:
	    return self.options.tmplt % str(pval)
	else:
	    return self.options.tmplt

    #---------------------------------------------------------
    def getOutputFile(self, pval):
	fname = self.getOutputFileName(pval)
	if fname == "-":
	    fd = sys.stdout
	elif self.fname2ofd.has_key(fname):
	    fd = self.fname2ofd[fname]
	else:
            if self.options.limit != -1 and 1+len(self.fname2ofd) > self.options.limit:
                raise RuntimeError("Too many files created. Limit=%d"%self.options.limit)
	    fd = open(fname, 'w')
	    self.fname2ofd[fname] = fd
	return fd

    #---------------------------------------------------------
    def processRow(self, r):
	pval = None
	if self.options.pcol is not None:
	    pval = r[self.options.pcol]
	fd = self.getOutputFile(pval)
	orow = r
	if orow is not None:
            if fd is sys.stdout:
              return orow
            else:
              self.writeOutput(orow,fd)
              return None


    #---------------------------------------------------------
    def go(self):
        for r in self.t1:
	    self.processRow(r)
            if self.options.tee:
                yield r

	for fd in self.fname2ofd.values():
	    fd.close()
