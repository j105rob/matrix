from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt

import json

class IpToDot(Bolt):
    def initialize(self, conf, ctx):
        self.convertFields = json.loads(conf["shmoocon.iptodot.fields"].replace("'",'"'))
        self.stream = conf["shmoocon.iptodot.emitStream"]
        self.transforms = self.convertFields['transform']

    def process(self, tup):
        try:    
            raw = tup.values[0]

            for key, newField in self.transforms.iteritems():
                raw[newField] = self.numIP2strIP(raw[key])
                
            self.emit([raw],self.stream)
            
        except Exception as e:
            self.log("IpToDot Error: %s"%(str(e)),'error')
            
    def numIP2strIP(self, ip):
        '''
        This function convert decimal ip to dot notation
        '''
        l = [str((ip >> 8*n) % 256) for n in range(4)]
        l.reverse()
        return ".".join(l)