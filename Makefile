CC=g++
FLAGS=-O3 -pedantic -Wall

all: libstream
	$(CC) $(FLAGS) suisto-server.cpp -o suisto-server -lzmq libstream.o

libstream:
	$(CC) $(FLAGS) libStream.cpp -o libstream.o -c

clean:
	rm suisto-server *.o *.pyc
