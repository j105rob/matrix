from __future__ import absolute_import, print_function, unicode_literals
from streamparse.spout import Spout

import json
import uuid
import socket
import select
import Queue
import threading
import time
from Queue import Empty
import dpkt

class UdpSpout(Spout):
    def initialize(self, conf, context):
        try:
            self.count = 0
            self.config(conf)
            self.udpBuffer = Queue.Queue()
            self.udpSocket = UdpSocket(udpBuffer=self.udpBuffer, log=self.log)
            t = threading.Thread(target=self.udpSocket.start)
            t.start()
            
        except Exception as e:
            self.log("UDP Spout Init: %s"%(str(e)),"error")
    
    def config(self, conf):
        try:
            self.emitSream = conf["shmoocon.spouts.udpspout.emitStream"]
        except KeyError as e:
            self.log("UDP Spout Config Missing key: %s"%(str(e)),"error")
    
    def next_tuple(self):
        try:
            msg = self.udpBuffer.get(False)
            #self.log(msg)
            if msg == None:
                return
            else: 
                self.emit([json.loads(msg)],stream=self.emitSream)
                self.count +=1
                self.log("spout count:%i"%(self.count))
        except Empty:
            return
        except Exception as e:
            self.log("UDP Spout Next Tuple: %s"%(str(e)),"error")
            
class NetflowV5(object):      
    def format(self,data):
        try:
            nf = dpkt.netflow.Netflow5(data)
            for r in nf.data:
                r.sys_uptime = nf.sys_uptime
                r.unix_sec = nf.unix_sec
                r.unix_nsec = nf.unix_nsec
                r.flow_sequence = nf.flow_sequence                
            return nf.data
        except dpkt.NeedData:
            return None
        
class NetflowProtocol(object):
    def __init__(self,udpBuffer,log=None,):
        self.count = 0
        self.udpBuffer = udpBuffer
        self.log = self.logz if log == None else log
        self.netflowv5 = NetflowV5()   
             
    def logz(self,msg):
        print (msg)
                
    def datagramReceived(self, data):
        formatted = self.netflowv5.format(data)
        if formatted == None:
            self.count +=1
            self.log("Bad data:%i"%(self.count))
        try:
            for r in formatted:
                fields = "src_addr,dst_addr,next_hop,input_iface,output_iface,pkts_sent,bytes_sent,start_time,end_time,src_port,dst_port,ip_proto,tos,src_as,dst_as,src_mask,dst_mask,tcp_flags,sys_uptime,unix_sec,unix_nsec,flow_sequence"
                j = {key:getattr(r,key) for key in fields.split(',')}
                self.udpBuffer.put(json.dumps(j))  
        except Exception as e:
            self.log("Exception: %s"%(e))
                     
class UdpSocket(object): 
    def __init__(self, log=None, port=61613, udpBuffer=None):       
        
        self.log = self.logz if log == None else log
        self.udpBuffer = udpBuffer
        self.netflow = NetflowProtocol(self.udpBuffer,log=self.log)
        self.sessionId = ''
        self.state={
            'starting':False,
            'connected':False,
            'subscribed':False,
            'receiving':False,
            'terminating':False
                    }
        self.port = port
       
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
            self.sock.bind(('', self.port))
        except Exception as e:
            self.log("Error connecting to UDP Socket: %s"%(str(e)))
        
        self.inputs = [self.sock]
        self.outputs = []
        self.message_queues = {}
        self.message_queues['out'] = Queue.Queue()     
        self.log("UDP Created")
                
    def logz(self,msg):
        print (msg)

    def stop(self):
        self.log("UDP Socket Closing and Stopping")
        self.state['terminating'] = True
            
    def start(self):
        self.log("UDP Socket is Starting")
        self.state['starting'] = True
        self.listen()
            
    def listen(self):
        while self.inputs and not self.state['terminating']:
            readable, writable, exceptional = select.select(self.inputs, self.outputs, self.inputs, 1)
            
            if not (readable or writable or exceptional):
                continue     

            for s in readable:
                data = s.recv(8192)
                if data:
                    self.netflow.datagramReceived(data)
                    if s not in self.outputs:
                        self.outputs.append(s)
                else:
                    self.log(('closing', 'after reading no data'))
                    if s in self.outputs:
                        self.outputs.remove(s)
                    self.inputs.remove(s)
                    s.close() 

            for s in writable:
                try:
                    next_msg = self.message_queues['out'].get_nowait()
                except Queue.Empty:
                    self.outputs.remove(s)
                    pass
                else:
                    #self.log('sending "%s" to %s' % (next_msg, s.getpeername()))
                    s.send(next_msg)

            for s in exceptional:
                self.log(('handling exceptional condition for',s.getpeername()))
                self.inputs.remove(s)
                if s in self.outputs:
                    self.outputs.remove(s)
                s.close()
                               
if __name__ == '__main__':
    try:
        b = Queue.Queue()
        ss = UdpSocket(udpBuffer=b)
        t = threading.Thread(target=ss.start)
        t.start()
        for i in range(1,20):
            time.sleep(2)
            try:
                print(b.get(False))
            except Empty:
                pass
        
        ss.stop()
        
        print('Finished Waiting for worker threads')
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is not main_thread:
                t.join()
    except KeyboardInterrupt:
        ss.stop()
        print('Terminating Waiting for worker threads')
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is not main_thread:
                t.join()
        
    
    
    