#------------------------------------------------------------
# Superclass of all command line tools 
# that take one input table and produce
# one output table.
#
from TableTool import TableTool
from common import *
class UnaryTableTool( TableTool ):
    def __init__(self):
	TableTool.__init__(self,1)

