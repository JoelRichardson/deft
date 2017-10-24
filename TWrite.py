'''
TWrite
'''
from common import *
from TableTool import TableTool
class TWrite(TableTool):
    USAGE=__doc__
    def __init__(self, argv):
        TableTool.__init__(self,1,argv)

    def initArgParser(self):
        TableTool.initArgParser(self)
        self.parser.add_option("-o","--output", dest="output", default="-",
            metavar="DST",
            help="Specifies output destination. Default='-' (write to stdout)")

