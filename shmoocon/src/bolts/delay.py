from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt

from Queue import Queue,Empty

class Delay(Bolt):
    def initialize(self, conf, ctx):
        self.queue = Queue()
        self.config(conf)
    
    def config(self, conf): 
        try:
            self.emitStream = conf["shmoocon.bolts.delay.emitStream"]
        except KeyError as e:
            self.log("Delay bolt Config Missing key: %s"%(str(e)),"error")
            
    def process(self, tup):
        try:
            data = tup.values[0]  
            self.queue.put(data) 
        except Exception as e:
            self.log("Delay Error: %s"%(str(e)),'error')
                        
    def process_tick(self, tup):
        try:
            data = self.queue.get(False)
            self.emit([data],stream=self.emitStream) 
        except Empty:
            pass
        except Exception as e:
            self.log("Delay Error: %s"%(str(e)),'error')