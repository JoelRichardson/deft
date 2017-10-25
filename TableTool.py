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

from common import *

#------------------------------------------------------------
# Superclass of all the command-line tools in this library.
# (The one exception is fjoin, which was developed independently.)
#
class TableTool:
    USAGE="usage: %prog [options] expression expression ... < input > output"
    COUNT=0
    def __init__(self, ninputs, argv):
        self.id = TableTool.COUNT
        TableTool.COUNT += 1
        #print "Created %s(%d)" % (self.__class__.__name__, self.id)

	self.parser = OptionParser(self.USAGE)
	self.ninputs = ninputs

	self.args = None
	self.options = None

	self.t1 = None
	self.t2 = None

	self.lfd = sys.stderr	# for log messages

        #
	self.initArgParser()
	(self.options, self.args) = self.parser.parse_args(argv)
        self.processOptions()

    #---------------------------------------------------------
    # All TableTools are iterators.
    def __iter__(self):
        return self.go()

    #---------------------------------------------------------
    def initArgParser(self):

        if self.ninputs > 0:
            self.parser.add_option("-1", dest="in1", default="-",
                metavar="SRC",
                help="Specifies input source. Default='-' (read from stdin).")

        if self.ninputs > 1:
            self.parser.add_option("-2", dest="in2", default="-",
                metavar="SRC",
                help="Specifies second input source. Default='-' (read from stdin).")

    #---------------------------------------------------------
    def processOptions(self):
        if self.ninputs > 0:
            from TRead import TRead
            if type(self.options.in1) is types.StringType:
                self.t1 = TRead(["-f", self.options.in1])
            else:
                self.t1 = self.options.in1
        if self.ninputs > 1:
            if type(self.options.in2) is types.StringType:
                self.t2 = TRead(["-f", self.options.in2])
            else:
                self.t2 = self.options.in2


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

