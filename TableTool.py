#!/usr/local/bin/python
#
# TableTools.py
#
# A collection of command-line oriented tools for processing
# delimited text files. 
#
# These tools can be invoked from the command line, or from
# withn Python. Example of command line:
#
#	% TableTools.py ta -1 file1.txt -g1 -acount -o out.txt
#
# This command counts the number of distinct values in column 1 of
# the file file1.txt, and writes the result to out.txt.
# Here's the same command, from within Python:
#
#	from TableTools import TAggregate
#	...
#	args=[ "-1", "file1.txt", "-g1", "-acount", "-o", "out.txt"]
#	TAggregate(args).go()
#
# AUTHOR:
#   Joel Richardson, Ph.D.
#   Mouse Genome Informatics
#   The Jackson Laboratory
#   Bar Harbor, Maine 04609
#   jer@informatics.jax.org
# 
# DATE: 
#   April 2006 - Initial version
#   March 2007 - Modified to fit into MGI frameworks.
#
#

#------------------------------------------------------------
import sys
import types
import os
import string
import re
import math
from optparse import OptionParser
import time

from TableFileIterator import TableFileIterator
from common import *

#------------------------------------------------------------
NL	= "\n"
TAB	= "\t"
SP	= " "
COLON	= ":"
HASH	= "#"

#------------------------------------------------------------
# Superclass of all the command-line tools in this library.
# (The one exception is fjoin, which was developed independently.)
#
class TableTool:
    USAGE="usage: %prog [options] expression expression ... < input > output"
    def __init__(self, ninputs ):
	self.parser = OptionParser(self.USAGE)
	self.args = None
	self.options = None
	self.nOutputRows = 0
	self.functionContext = {}
	self.functions = []
	self.isFilter = []
	self.ninputs = ninputs

	self.t1 = None
	self.t2 = None

	# Output file
        self.ofd = None

	self.lfd = sys.stderr	# log file

    #---------------------------------------------------------
    # All TableTools are iterators.
    def __iter__(self):
        return self.go()

    #---------------------------------------------------------
    def initArgParser(self):

	self.parser.add_option("-1","-f","--file1", dest="file1", default="-",
	    metavar="FILE",
	    help="Specifies file for input table [default='-', read from stdin]")

	self.parser.add_option("--s1", "--separator1", dest="sep1", default=TAB,
	    metavar="CHAR",
	    help="Separator character (default=TAB).")
	self.parser.add_option("--c1", "--comment1", dest="com1", default=HASH,
	    metavar="CHAR",
	    help="Comment character (default=HASH). " + \
	    	 "Lines beginning with CHAR are skipped.")
	if self.ninputs == 2:
	    self.parser.add_option("-2","--file2", dest="file2", default="-",
		metavar="FILE",
		help="Specifies file for table T2 [default='-', read from stdin]")
	    self.parser.add_option("--s2","--separator2", dest="sep2", default=TAB,
		metavar="CHAR",
		help="Separator character for file 2 (default=TAB).")
	    self.parser.add_option("--c2","--comment2", dest="com2", default=HASH,
		metavar="CHAR",
		help="Comment character for file 2 (default=HASH). " + \
		     "Lines beginning with CHAR are skipped.")

        self.parser.add_option("-o","--out-file", dest="outFile", default="-",
            metavar="FILE",
            help="Specifies output file [default='-',write to stdout]")

	self.parser.add_option("-l", "--log-file", dest="logFile", default=None,
	    metavar="FILE",
	    help="Specifies log file [default=write to stderr]")

    #---------------------------------------------------------
    # Parses the command line. Options and positional args
    # assigned to self.options and self.args, resp.
    #
    def parseCmdLine(self, argv):
	self.initArgParser()
	(self.options, self.args) = self.parser.parse_args(argv)
	self.validate()

    #---------------------------------------------------------
    # Check options, open files, and generally get set up.
    #
    def validate(self):
	try:
	    self.openFiles()
	    self.processOptions()
	except:
	    self.errorExit()

    #---------------------------------------------------------
    def processOptions(self):
        pass

    #---------------------------------------------------------
    # Open all input and output files.
    #
    def openFiles(self):
        self.t1 = TableFileIterator(self.options.file1)
	self.t1.setCommentChar(self.options.com1)
	self.t1.setSeparatorChar(self.options.sep1)
        if self.ninputs==2:
	    self.t2 = TableFileIterator(self.options.file2)
            self.t2.setCommentChar(self.options.com2)
            self.t2.setSeparatorChar(self.options.sep2)
	if self.options.outFile and self.options.outFile != "-":
	    self.ofd = open(self.options.outFile, 'w')
	if self.options.logFile:
	    self.lfd = open(self.options.logFile, 'a')
	    sys.stderr = self.lfd
	self.t1.setLogFile(self.lfd)
	if self.ninputs==2 and self.options.file2:
	    self.t2.setLogFile(self.lfd)

    #---------------------------------------------------------
    def setLogFile(self, file):
	if type(file) is types.StringType:
	    self.lfd = open( file, 'a' )
        else:
	    self.lfd = file

    #---------------------------------------------------------
    def closeFiles(self):
    	self.t1.close()
	if(self.t2 is not None):
	    self.t2.close()
	if(self.ofd is not None):
	    self.ofd.close()

    #---------------------------------------------------------
    # Prints exception info, then dies.
    #
    def errorExit(self, message=None):
	(ex_type, ex_value, traceback) = sys.exc_info()
	self.debug("\nAn error has occurred.")
	if message is not None:
	    self.debug(message)
	if ex_type is not None:
	    self.debug("\nThe following exception was caught:")
	    sys.excepthook(ex_type, ex_value, traceback)
	    self.die()

    #---------------------------------------------------------
    # Writes an error message and dies. Exits with -1 status.
    #
    def die(self, error=None):
	if error:
	    self.debug(error)
	sys.exit(-1)

    #---------------------------------------------------------
    # Writes a message and a newline to the log file.
    #
    def debug(self, msg):
	self.lfd.write(str(msg))
	self.lfd.write(NL)

    #---------------------------------------------------------
    # Returns a tuple containing the indicated columns
    # from the given row.
    #
    def makeKey(self, row, colIndexes):
	t = []
	for c in colIndexes:
	    t.append( row[c] )
	return tuple(t)

    #---------------------------------------------------------
    # Parses a list of integers from val. val may
    # be a string or a list of strings. (If a list,
    # it's joined first.) Column numbers are separated
    # by commas. Returns list of all the
    # integers parsed from the string.
    #
    def parseIntList(self, val):
	if type(val) is types.ListType:
	    val = string.join(val, ",")
	val=re.split("[, ]+", val)
	return map(int, filter(None,val))

    #---------------------------------------------------------
    # Write the row to the output file.
    #
    def writeOutput(self, row, fd=None):
	if fd is None:
	    fd = self.ofd
	if fd is self.ofd:
	    self.nOutputRows += 1
	fd.write(string.join(map(str,row),TAB))
	fd.write(NL)

