#!/usr/bin/env python


import zmq

context = zmq.Context()

socket = context.socket( zmq.REQ )
socket.connect( "tcp://127.0.0.1:5555")

try:
	while True:
		line = raw_input( ">" ).strip()
		socket.send( line )
		tmp = socket.recv()
		print tmp
except KeyboardInterrupt:
		pass

socket.close()

