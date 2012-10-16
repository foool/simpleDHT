#!/usr/bin/env python
#encoding=utf-8

import os
import socket
import hashlib
from conshash import HashRing

class Configuration:
	count=0
	clist=[]
	def __init__(self):
		"""read the configuration file"""
		self.readPath("machineconf.txt")
		
	def readPath(self,pStr):
		"""read the machines from the file"""
		self.pathStr=pStr
		fp=open(self.pathStr,'r')
		while True:
			line=fp.readline()
			if not line:
				break			
			line=line.rstrip()
			self.clist.append(line)
			self.count=self.count+1
			
	def get_count(self):
		"""return the number of the nodes"""
		return self.count
		
	def get_list(self):
		"""return the list of the nodes"""
		return self.clist

class Client:
	def __init__(self):
		"""get nodes"""
		self.malist=[]
		self.macnum=0
		conf=Configuration()
		self.malist=conf.get_list()
		self.macnum=conf.get_count()
		print "nodelist:",self.malist
		self.hring=HashRing(self.malist)
		
	def write(self,src):
		"""write src file into node"""
		print "File is",src
		toIP,toPort=self.src_to_node(src)
		print "Send to",toIP,toPort
		self.send(src,toIP,toPort)
		
	def src_to_node(self,src):
		des=self.hring.get_node_pos(src)
		iPort=des.split(':')
		return iPort[0],iPort[1]
		
	def send(self,src,ip,port):
		clsoc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		clsoc.connect((ip,int(port)))
		fp=open(src,'rb')
		formStr="WRITFILE,%04d"%len(src)
		print "formStr",formStr,len(formStr)
		clsoc.send(formStr)
		resdata=clsoc.recv(1024)
		if resdata.startswith('OK'):
			print "OK"
		print "sending....",src
		clsoc.send(src)
		print "sending data...."
		while True:
			data=fp.read(4096)
			if not data:
				break
			while len(data)>0:
				sent=clsoc.send(data)
				data=data[sent:]
		print "Fished to send ",src
		fp.close()
		
	def fetch(self,src):
		toIP,toPort=self.src_to_node(src)
		print "fetch from",toIP,toPort
		self.get(src,toIP,toPort)
		
	def get(self,src,ip,port):
		clsoc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		clsoc.connect((ip,int(port)))
		formStr="FETCFILE,%04d"%len(src)
		print "formStr",formStr
		clsoc.send(formStr)
		resdata=clsoc.recv(1024)
		if resdata.startswith('OK'):
			print "OK"
		print "sending....",src
		clsoc.send(src)
		print "fetching data...."
		ffile=src[src.rindex('\\')+1:]
		fp=open(ffile,'wb')
		while True:
			data=clsoc.recv(1024)
			if not data:break
			fp.write(data)
			print "fetching",data[-8:]
		fp.flush()
		fp.close()
		print "finished!",src
		
if __name__=="__main__":
	mac=Client()
	src="full path of your file"
	mac.write(src)
	#mac.fetch(src)