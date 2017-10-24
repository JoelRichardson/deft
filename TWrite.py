'''
TWrite
'''
from common import *
from TableTool import TableTool
import sys

class TWrite(TableTool):
    USAGE=__doc__
    def __init__(self, argv):
        TableTool.__init__(self,1,argv)

    def initArgParser(self):
        TableTool.initArgParser(self)
        self.parser.add_option("-o","--output", dest="output", default="-",
            metavar="DST",
            help="Specifies output destination. Default='-' (write to stdout)")

        self.parser.add_option("-m","--mode", dest="mode", default="w",
            metavar="MODE",
            help="Specifies output mode. w=write, a=append. Default=w.")

    def go(self):
        if self.options.output == "-":
            self.ofd = sys.stdout
        else:
            self.ofd = open(self.options.output, 'w')
        #
        for row in self.t1:
            self.ofd.write(TAB.join(map(str,row)))
            self.ofd.write(NL)
            yield row


