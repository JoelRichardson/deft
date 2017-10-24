#------------------------------------------------------------
'''
        tc - table closure
Computes the transitive closure over a directed binary relation. 
The binary relation is defined by a pair of columns in the input, parent and child.
The output is a two-column table containing the columns ancestor and descendant.
'''
from TableTool import TableTool
from common import *

class TClosure ( TableTool ) :
    USAGE=__doc__
    def __init__(self,argv):
	TableTool.__init__(self,1,argv)

    #---------------------------------------------------------
    def initArgParser(self):
        TableTool.initArgParser(self)
        self.parser.add_option("-p", "--parent", dest="parent", 
          metavar="COL",
          type="int",
          help="Specify parent column")

        self.parser.add_option("-k", "--kid", dest="child", 
          metavar="COL",
          type="int",
          help="Specify child column")

        # TODO: Add symmetric=yes/no option to determine if output
        # is symmetric ior not. In a symmetric tc the output contains 
        # contains (x,x) for each x. 


    #---------------------------------------------------------
    def processOptions(self):
      TableTool.processOptions(self)
      if self.options.parent is None:
        self.parser.error("No parent index specified.")
      if self.options.child is None:
        self.parser.error("No child index specified.")

    #---------------------------------------------------------
    def go(self):
        pi = self.options.parent
        ci = self.options.child
        graph = {}
        #
        for inrow in self.t1:
            p = inrow[pi]
            c = inrow[ci]
            graph.setdefault(p, set()).add(c)
            graph.setdefault(c, set())
        #
        closure = {}
        visited=set()
        def reach(n):
          if n in visited: return closure[n]
          visited.add(n)
          closure[n] = cl = set([n])
          for c in graph[n]:
            cl.update(reach(c))
          return cl
        #
        for x in graph:
          reach(x)
        #
        for a in closure:
          for d in closure[a]:
            yield [a, d]

