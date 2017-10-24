'''
TRead
'''
from common import *
from TableTool import TableTool
class TRead(TableTool):
    USAGE=__doc__
    def __init__(self, argv):
	#
	# 
	self.ncols = 0

	#
	self.lfd = sys.stderr

	#
	self.separatorChar	 = TAB
	self.commentChar	= HASH

	#
	self.fileName = None
	self.fileDesc = None
	#
	self.currentLine = None
	self.currentLineNum = 0
	#
	self.currentRow = None
	self.currentRowNum = 0
        #
        TableTool.__init__(self, 0,argv)

    #---------------------------------------------------------
    def initArgParser(self):
        TableTool.initArgParser(self)

	self.parser.add_option("-f", "--filename", dest="filename", default="-",
	    metavar="FILE",
	    help="Specifies file for input table. Default='-' (read from stdin).")

	self.parser.add_option("-s", "--separator", dest="sep", default=TAB,
	    metavar="CHAR",
	    help="Separator character (default=TAB).")

	self.parser.add_option("-c", "--comment", dest="com1", default=HASH,
	    metavar="CHAR",
	    help="Comment character (default=HASH). Lines beginning with CHAR are skipped.")

    #--------------------------------------------------
    def open(self):

        fname = self.options.filename
	if type(fname) is types.StringType:
	    if fname == "-":
		self.fileName = "<stdin>"
		self.fileDesc = sys.stdin
	    else:
		self.fileName = fname
		self.fileDesc = open(fname,'r')
	else:
	    self.fileName = "<???>"
	    self.fileDesc = fname

	#
	self.currentLineNum = 0
	self.currentLine = None
	#
	self.currentRowNum  = 0
	self.currentRow  = None

    #--------------------------------------------------
    def close(self):
	if self.fileDesc is not None \
	and self.fileDesc is not sys.stdin:
	    self.fileDesc.close()
	#
	self.fileDesc = None
	self.fileName = None

    #--------------------------------------------------
    # Returns next row from file, or None if there are
    # no more. Skips comment lines and blank lines.
    # Advances line and row counters.
    #
    def nextRow(self):
	self.currentLine = self.fileDesc.readline()
	while self.currentLine:
	    self.currentLineNum += 1
	    if self.currentLine == NL \
	    or self.currentLine.startswith(self.commentChar):
		self.currentLine = self.fileDesc.readline()
		continue

	    self.currentRowNum += 1
	    self.currentRow = string.split(self.currentLine, self.separatorChar)
	    self.currentRow[-1] = self.currentRow[-1][:-1] # remove newline from last col

	    if self.ncols == 0:
		self.ncols = len(self.currentRow)
	    elif self.ncols != len(self.currentRow):
		self.lfd.write(\
		  "WARNING: wrong number of columns (%d) in line %d. Expected %d. \n" % \
		  (len(self.currentRow), self.currentLineNum, self.ncols))
		self.lfd.write(self.currentLine)
		#self.currentLine = self.fileDesc.readline()
		#continue

	    return self.currentRow
	# end while-loop
	return None

    #--------------------------------------------------
    # If reading from a file, stat the file.
    # If reading from stdin or unnamed file descriptor,
    # return None.
    #
    def statFile( self ):
	if self.fileName[0] != "<":
	    return os.stat( self.fileName )
	return None

    #--------------------------------------------------
    # If reading from a file, return size of file in
    # bytes. If reading from stdin or unnamed file
    # descriptor, return -1.
    #
    def fileSize(self):
	stat = self.statFile()
	if stat is None:
	    return -1
	else:
	    return stat.st_size 

    #--------------------------------------------------
    # Returns the current line
    #
    def getCurrentLine(self):
	return self.currentLine

    #--------------------------------------------------
    # Returns the current line number. Line numbers 
    # start at 1.
    #
    def getCurrentLineNum(self):
	return self.currentLineNum

    #--------------------------------------------------
    # Returns the current row.
    #
    def getCurrentRow(self):
	return self.currentRow

    #--------------------------------------------------
    def getNCols(self):
	return self.ncols

    #--------------------------------------------------
    # Returns the current row number.
    #
    def getCurrentRowNum(self):
	return self.currentRowNum

    #--------------------------------------------------
    # Returns the file name this iterator is attached to.
    #
    def getFileName(self):
	return self.fileName

    #--------------------------------------------------
    # Returns next row, or throws StopIteration.
    #
    def go(self):
        self.open()
	r = self.nextRow()
        while r:
            yield r
            r = self.nextRow()
        self.close()
