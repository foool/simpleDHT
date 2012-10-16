#!/usr/bin/env python
#encoding=utf-8

import os,sys
import socket

class Node:
	def __init__(self):
		self.port=10000
	def run(self):
		dsoc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		dsoc.bind(('localhost',self.port))
		print "listening at",self.port
		while True:
			dsoc.listen(1)
			conn,addr=dsoc.accept()
			print "connected",addr
			databuf=conn.recv(13)
			print "received",databuf
			if databuf.startswith('WRITFILE'):
				print "OPS == write file"
				dlist=databuf.split(',')
				fnamelen=int(dlist[1])
				conn.send('OK')
				print "filenamelen is",fnamelen
				filename=conn.recv(fnamelen)
				filename=filename[filename.rindex('\\')+1:]
				print "file is",filename
				fp=open(filename,'wb')
				while True:
					data=conn.recv(1024)
					if not data:break
					fp.write(data)
				fp.flush()
				fp.close()
				print "finished!",filename
			if databuf.startswith('FETCFILE'):
				print "OPS == fetch file"
				dlist=databuf.split(',')
				fnamelen=int(dlist[1])
				conn.send('OK')
				print "filenamelen is",fnamelen
				filename=conn.recv(fnamelen)
				filename=filename[filename.rindex('\\')+1:]
				print "file is",filename
				fp=open(filename,'rb')
				while True:
					data=fp.read(4096)
					if not data:
						break
					while len(data)>0:
						sent=conn.send(data)
						data=data[sent:]
				print "Fished to send ",filename
			if databuf.startswith('HELLNODE'):
				print "client say hello!"
			conn.close()
			
	def setPort(self,port):
		self.port=int(port)

if __name__=="__main__":
	nd=Node()
	nd.setPort(sys.argv[1])
	nd.run()

