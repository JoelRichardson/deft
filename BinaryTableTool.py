#------------------------------------------------------------
# Superclass of all command line tools 
# that take two input tables and produce
# one output table.
#
from common import *
from TableTool import TableTool
class BinaryTableTool( TableTool ):
    def __init__(self):
	TableTool.__init__(self,2)

