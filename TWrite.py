'''
TWrite
'''
from common import *
from TableTool import TableTool
class TWrite(TableTool):
    USAGE=__doc__
    def __init__(self, argv):
        TableTool.__init__(self,1,argv)
