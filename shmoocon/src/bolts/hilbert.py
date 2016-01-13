from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
from collections import namedtuple
import json

# srcloc & dstloc are arrays of Hilbert tuples
HilbertResult = namedtuple("HilbertResult",['uuid','srcloc','dstloc'])

class Hilbert(namedtuple('Hilbert','n d')):
    """
    Hilbert Space Filling Curve 
    """
    __slots__ = ()
    @property
    def xy2d(self,n,x,y):
        """
        Take a x,y and return the d value
        """
        rx = ry = d = 0
        s = n/2
    
        while (s>0):
            rx = (x & s) > 0
            ry = (y & s) > 0
            d += s * s * ((3 * rx) ^ ry)
            x,y = rot(s, x, y, rx, ry)
            s /=2
        return d
    @property
    def d2xy(self):
        """
        take a d value in [0, n**2 - 1] and map it to
        an x, y value (e.g. c, r).
        """
        try:
            assert(self.d <= self.n**2 - 1)
            t = self.d
            x = y = 0
            s = 1
            while (s < self.n):
                rx = 1 & (t / 2)
                ry = 1 & (t ^ rx)
                x, y = self.rot(s, x, y, rx, ry)
                x += s * rx
                y += s * ry
                t /= 4
                s *= 2
            return x, y
        except Exception as e:
            return ("ARGH!")
        
    
    def rot(self, n, x, y, rx, ry):
        """
        rotate/flip a quadrant appropriately
        """
        if ry == 0:
            if rx == 1:
                x = n - 1 - x
                y = n - 1 - y
            return y, x
        return x, y
    
class HilbertBolt(Bolt):
    
    '''
    This bolt will take a (src_ip,dst_ip) tuple and return 
    a tuple of tuples in the form of: Hilbert Tuple
    '''
    def initialize(self, conf, ctx):
        """
        Required configuration:
        
        :param analytics.hilbert.fields: Example: {'id':'uuid','src':'sip','dst':'dip'}
        :type analytics.hilbert.fields: str
        
        """
        self.hilbertFields = json.loads(conf["shmoocon.hilbert.fields"].replace("'",'"'))
        self.srcName = self.hilbertFields['src']
        self.dstName = self.hilbertFields['dst']
        self.idName = self.hilbertFields['id']
        self.emitStream = conf["shmoocon.hilbert.emitStream"]
        self.n = 16 #255 squares in a 16x16 grid

    def process(self, tup):
        try:
            raw = tup.values[0]
            srcHilberts = self.parseIp(int(raw[self.srcName]))
            dstHilberts = self.parseIp(int(raw[self.dstName]))
            hilbertResult = HilbertResult(raw[self.idName],srcHilberts,dstHilberts)
            #self.log(hilbertResult)
            raw['srchilbert'] = srcHilberts
            raw['dsthilbert'] = dstHilberts
            #self.log(raw)
            self.emit([raw],stream=self.emitStream)
        except Exception as e:
            self.log("Hilbert Error: %s"%(str(e)),'error')

    def parseIp(self,ip):             
        hilberts = []
        octets = [(ip >> 8*n) % 256 for n in range(4)]
        octets.reverse()
        for octet in octets:
            h = Hilbert(self.n,octet)
            t = (h.n,h.d,h.d2xy)
            hilberts.append(t)
        return hilberts
    
    
    