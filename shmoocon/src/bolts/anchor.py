from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import uuid
class Anchor(Bolt):
    '''
    Used to anchor the data to a UUID
    '''

    def initialize(self, conf, ctx):
        self.config(conf)
    
    def config(self, conf): 
        try:
            self.emitSream = conf["shmoocon.bolts.anchor.emitStream"]
            self.idName = conf["shmoocon.bolts.anchor.id"] 
        except KeyError as e:
            self.log("Anchor bolt Config Missing key: %s"%(str(e)),"error")
            
    def process(self, tup):

        try:
            data = tup.values[0]
            u = uuid.uuid1()    
            id = str(u)  
            data[self.idName] = id
            self.log(data)      
            self.emit([data],stream=self.emitSream)    

        except Exception as e:
            self.log("Anchor Error: %s"%(str(e)),'error')