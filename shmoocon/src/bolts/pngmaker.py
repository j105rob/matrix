from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt

from PIL import Image, ImageDraw
import itertools
from subprocess import Popen, PIPE
import math
import scipy.stats as Stats
import numpy

from collections import deque

class PngMaker(Bolt):
    def initialize(self, conf, ctx):
        self.size = (272,272)
        
        self.mode = "RGBA"
        self.fill = 255 #black
        self.cnt = 0
        self.fps = 20
        self.duration = 60
        self.frames = deque(maxlen=(self.fps*self.duration))

        self.tick = 0
        self.sample = 30
        self.fileName =''
        
        self.compositeImage = Image.new(self.mode,(828,828),color=(self.fill,self.fill,self.fill,self.fill))
        
        self.img = [Image.new(self.mode,self.size,color=(self.fill,self.fill,self.fill,self.fill)),
                    Image.new(self.mode,self.size,color=(self.fill,self.fill,self.fill,self.fill)),
                    Image.new(self.mode,self.size,color=(self.fill,self.fill,self.fill,self.fill)),
                    Image.new(self.mode,self.size,color=(self.fill,self.fill,self.fill,self.fill)),
                    Image.new(self.mode,self.size,color=(self.fill,self.fill,self.fill,self.fill))]
        
        self.draw = [ImageDraw.Draw(self.img[0], self.mode),
                     ImageDraw.Draw(self.img[1], self.mode),
                     ImageDraw.Draw(self.img[2], self.mode),
                     ImageDraw.Draw(self.img[3], self.mode),
                     ImageDraw.Draw(self.img[4], self.mode)]
        self.a = {}   
        self.command = [ 'ffmpeg',
        '-re',
        '-f', 'rawvideo',
        '-vcodec','rawvideo',
        '-s', '16x16', # size of one frame
        '-pix_fmt', 'argb',
        '-r', str(self.fps), # frames per second
        '-i', '-', # The imput comes from a pipe
        '-an', # Tells FFMPEG not to expect any audio
        'http://ffserver.labs.g2-inc.net:8090/feed1.ffm' ]
        
        # ffmpeg -re -f rawvideo -vcodec rawvideo -s 16x16 -pix_fmt argb -r 20 -i /tmp/vid -an http://ffserver.labs.g2-inc.net:8090/feed1.ffm
        # sudo -re -f rawvideo -vcodec rawvideo -s 16x16 -pix_fmt argb -r 20 -i /tmp/vid -f s16le -i /dev/zero -flags +global_header -ar 44100 -ab 16k -s 320x180 -vcodec h264 -pix_fmt yuv420p -g 25 -vb 32k -profile:v baseline -r 30 -f flv "rtmp://a.rtmp.youtube.com/live2/Boyobejaminhere.jg3p-dsbt-hseu-23uq"
        #self.videoPipe = Popen(self.command, stdin=PIPE,bufsize=1024)  
        #self.videoPipe = open('/tmp/vid', 'w')
        
    def process(self, tup): 
        raw = tup.values[0]
        x = 0
        y = 0
        ip = None
        
        if raw['src_dot'].startswith('10'):
            raw['inbound'] = False
            x,y = raw['srchilbert'][3][2]
            ip = raw['src_dot']
            
        if raw['dst_dot'].startswith('10'):
            raw['inbound'] = True
            x,y = raw['dsthilbert'][3][2]
            ip = raw['dst_dot']
            
        if ip != None:
            if ip not in self.a:
                self.a[ip] = {
                              'q_pkts':deque(maxlen=self.sample),
                              'q_bytes':deque(maxlen=self.sample),
                              'total_pkts_in':deque(maxlen=self.sample),
                              'total_pkts_out':deque(maxlen=self.sample),
                              'sport': {},
                              'dport':{},
                              'dstip':{},
                              'flows':0,
                              'bytes':0
                              }    
             
            self.a[ip]['flows'] +=1     
            self.a[ip]['bytes'] +=raw['bytes_sent']   
            self.a[ip]['q_pkts'].append(raw['pkts_sent']) 
            self.a[ip]['q_bytes'].append(raw['bytes_sent']) 

            if raw['dst_addr'] in self.a[ip]['dstip']:
                self.a[ip]['dstip'][raw['dst_addr']]['flows'] +=1
                self.a[ip]['dstip'][raw['dst_addr']]['bytes'] +=raw['bytes_sent']
            else:
                self.a[ip]['dstip'][raw['dst_addr']] = {'flows':1,'bytes':raw['bytes_sent']}
            
            if raw['src_port'] in self.a[ip]['sport']:
                self.a[ip]['sport'][raw['src_port']]['flows'] +=1
                self.a[ip]['sport'][raw['src_port']]['bytes'] +=raw['bytes_sent']
            else:
                self.a[ip]['sport'][raw['src_port']] = {'flows':1,'bytes':raw['bytes_sent']}
                
            if raw['dst_port'] in self.a[ip]['dport']:
                self.a[ip]['dport'][raw['dst_port']]['flows'] +=1
                self.a[ip]['dport'][raw['dst_port']]['bytes'] +=raw['bytes_sent']
            else:
                self.a[ip]['dport'][raw['dst_port']] = {'flows':1,'bytes':raw['bytes_sent']} 
                              
            if raw['inbound']:
                self.a[ip]['total_pkts_in'].append(raw['pkts_sent'])
                
            else:
                self.a[ip]['total_pkts_out'].append(raw['pkts_sent'])

            #channels    
            channel1A = self.normalized(self.a[ip],'q_pkts') 
            channel1B = self.normalized(self.a[ip],'q_bytes') 
            channel2A = self.proportionOfInboundFlows(self.a[ip])      
            channel2B = self.proportionOfOutboundFlows(self.a[ip])   
              
            channel4AKurt ,channel4BKurt, channel4ASkew,channel4BSkew  = self.describe(self.a[ip]['dport'])
 
            channel5AKurt ,channel5BKurt ,channel5ASkew,channel5BSkew  = self.describe(self.a[ip]['sport'])
  
            channel7AKurt,channel7BKurt,aa,bb = self.describe(self.a[ip]['dstip'])
             
                        
            #need to create several frame buffers.
            
            pix = [(channel1B,channel5BKurt,channel4BKurt,channel1A),
                   (channel1B,channel5BKurt,channel4AKurt,channel1A),
                   (channel1B,channel2A,channel5BKurt,channel1A),
                   (channel1B,channel2B,channel4AKurt,channel1A),
                   (channel1B,channel7AKurt,channel7BKurt,channel1A)
                   ]
            
            composite = [(0,0),
                         (552,0),
                         (276,276),
                         (0,552),
                         (552,552)]

            for i in range(0,5): 
                #self.log("pix %s %i"%(pix[i],i)) 
                #Draw.rectangle(xy, fill=None, outline=None)  
                loc = [(x*16+x,y*16+y),(x*16+x+16,y*16+y+16)]  
                #self.log((x,y,loc))       
                self.draw[i].rectangle(loc, fill=pix[i])                
                self.compositeImage.paste(self.img[i], composite[i])
                
            z = self.compositeImage.tobytes()
            self.frames.append(z)

            #self.videoPipe.write(z)
            
            #self.videoPipe.stdin.write(z)
            #self.videoPipe.stdin.flush()
            #self.videoPipe.wait()
            #self.log(self.a)

    def describe(self,data):
        axis = [ [k,v['flows'],v['bytes']] for k,v in data.iteritems() ]
        a = numpy.array(axis)
        n, min_max, mean, var, skew, kurt = Stats.describe(a)
        
        resp = (int((abs(kurt[1]+3/3))*255) if int((abs(kurt[1]/3))*255) < 255 else 255,
               int((abs(kurt[2]+3/3))*255) if int((abs(kurt[2]/3))*255) < 255 else 255,
               int((abs(skew[1])/2)*255) if int((abs(skew[1])/2)*255) < 255 else 255,
               int((abs(skew[2])/2)*255) if int((abs(skew[2])/2)*255) < 255 else 255
                )
        
        return resp
            

        
    def proportionOfInboundFlows(self,data):  
        inbound = float(sum(data['total_pkts_in']))
        outbound = float(sum(data['total_pkts_out']))
        tot = float(inbound+outbound)
        f = float(inbound/tot)
        y = abs(f-0.5)
        val = (y/0.5)*255
        #self.log("%i %i %d %f %f %f"%(inbound,outbound,tot,val,f,y))
        return int(val)
    
    def proportionOfOutboundFlows(self,data):  
        inbound = float(sum(data['total_pkts_in']))
        outbound = float(sum(data['total_pkts_out']))
        tot = float(inbound+outbound)
        f = float(outbound/tot)
        y = abs(f-0.5)
        val = (y/0.5)*255              
        #self.log("%i %i %d %f %f %f"%(inbound,outbound,tot,val,f,y))
        return int(val)
        
    def normalized(self,data,qname):  
        #self.log(data)
        data['min'] = 9999999
        data['max'] = 0   
        for elem in data[qname]:
            if data['min'] > elem:
                data['min'] = elem        
            if data['max'] < elem:
                data['max'] = elem  
                  
        #self.log(data)
        s = sum(data[qname])
        alpha = s/len(data[qname])
        normalizedAlpha = 0
        
        try:
            normalizedAlpha = (255/data['max'])*alpha
        except ZeroDivisionError:
            pass 
        return normalizedAlpha
    
    def flush(self):
        self.a = {}
    
    def sendVideo(self):
        self.tick +=1
        self.fileName = "/Volumes/DEV01/matrix/shmoocon/vid/out_%i_%i.mp4"%(self.fps,self.tick)
        
        self.command = [ 'ffmpeg',
        '-y', # (optional) overwrite output file if it exists
        '-f', 'rawvideo',
        '-vcodec','rawvideo',
        '-s', '828x828', # size of one frame
        '-pix_fmt', 'argb',
        '-r', str(self.fps), # frames per second
        '-i', '-', # The imput comes from a pipe
        '-an', # Tells FFMPEG not to expect any audio
        self.fileName ]
        
        self.log("Frame Buffer Length: %i"%(len(self.frames)))
        p = Popen(self.command, stdin=PIPE)
        for i in range(0,self.fps * self.duration): 
            try:
                data = self.frames[i]    
                p.stdin.write(data)
            except IndexError:
                pass
        p.stdin.close()
        p.wait()   
        self.flush()  
                  
    def process_tick(self, tup):
        self.log("Tick")
        self.sendVideo()
  
if __name__ == '__main__':
    c = PngMaker()
    c.ff()
    
    
    
    
    
    