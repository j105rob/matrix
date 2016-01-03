from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt

from PIL import Image, ImageDraw
import random
from subprocess import Popen, PIPE
class PngMaker(Bolt):
    def initialize(self, conf, ctx):
        self.size = (16,16)
        self.mode = "RGBA"
        self.fill = 0 #black
        #putpixel
        #putdata
        self.cnt = 0
        self.fps = 5
        self.duration = 60
        self.command = [ 'ffmpeg',
        '-y', # (optional) overwrite output file if it exists
        '-f', 'rawvideo',
        '-vcodec','rawvideo',
        '-s', '16x16', # size of one frame
        '-pix_fmt', 'argb',
        '-r', str(self.fps), # frames per second
        '-i', '-', # The imput comes from a pipe
        '-an', # Tells FFMPEG not to expect any audio
        'out.mp4' ]
        
    def process(self, tup): 
        raw = tup.values[0]
        #u'srchilbert': [(16, 192, (15, 7)), (16, 168, (14, 14)), (16, 100, (4, 14)), (16, 129, (8, 9))]
        
    def process_tick(self, tup):
         pass
      
    def ff(self):
        p = Popen(self.command, stdin=PIPE)
        for i in range(0,self.fps * self.duration):           
            p.stdin.write(self.make())
        p.stdin.close()
        p.wait()   
             
    def make(self):
        img = Image.new(self.mode,self.size,self.fill)
        draw = ImageDraw.Draw(img, self.mode)        
        for i in range(img.size[0]):
            for j in range(img.size[1]):
                r = random.randint(0,255)
                g = random.randint(0,255)
                b = random.randint(0,255)
                a = random.randint(0,255)
                draw.point((i,j), (r,g,b,a))       
        return img.tobytes()
  
if __name__ == '__main__':
    c = PngMaker()
    c.ff()