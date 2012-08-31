#!/usr/bin/env python


import zmq
import json
import time

class SuistoClient( object ):
	def __init__( self, addr = None ):
		if not addr:
			self.addr = "tcp://127.0.0.1:5555"
		else:
			self.addr = addr
		
		self.context = zmq.Context()
		self.socket = self.context.socket( zmq.REQ )
		self.socket.connect( self.addr )
	
	def create( self, name ):
		self.socket.send( "CREATE %s"%name )
		response = json.loads( self.socket.recv() )
		if 'error' in response:
			raise IndexError
		return True
	
	def push( self, stream, value ):
		self.socket.send( "PUSH %s %s"%( stream, value ) )
		response = json.loads( self.socket.recv() )
		if 'error' in response:
			raise IndexError
		return True
	
	def latest( self, stream ):
		self.socket.send( "LATEST %s"%( stream ) )
		response = json.loads( self.socket.recv() )
		if 'error' in response:
			raise IndexError
		return response
	
	def since( self, stream, ts ):
		self.socket.send( "SINCE %s %i"%( stream, ts ) )
		response = json.loads( self.socket.recv() )
		if 'error' in response:
			raise IndexError
		return response
	


t0 = time.time()

suisto = SuistoClient()
#suisto.create( "debug-stream" )

N = 5000
for i in range( N ):
	suisto.push( "debug-stream", "entry #%i"%i )

t1 = time.time()

print "dT:", t1 - t0, "rps: ", N/(t1-t0)
print ""

t0 = time.time()
N = 5000
for i in range( N ):
	suisto.latest( "debug-stream" )

t1 = time.time()

print "dT:", t1 - t0, "rps: ", N/(t1-t0)
