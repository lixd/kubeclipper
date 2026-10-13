# Real NTP protocol fault fixture: report a source clock 60 seconds ahead.
# The client daemon uses -x, so this never sets the host clock.
import socket,struct,time
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.bind(('127.0.0.1',18123))
def stamp(t):
 t+=2208988800;return struct.pack('!II',int(t),int((t-int(t))*2**32))
while True:
 data,addr=s.recvfrom(1024)
 if len(data)<48:continue
 now=time.time()+60;p=bytearray(48);p[0]=0x24;p[1]=2;p[2]=4;p[3]=236
 p[4:8]=struct.pack('!I',655);p[8:12]=struct.pack('!I',655);p[12:16]=b'R45F';p[16:24]=stamp(now-1);p[24:32]=data[40:48];p[32:40]=stamp(now);p[40:48]=stamp(time.time()+60);s.sendto(p,addr)
