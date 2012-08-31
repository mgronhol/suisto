#include <iostream>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <stdint.h>

#include <vector>
#include <map>
#include <tr1/unordered_map>

#include <pthread.h>

#include <zmq.hpp>
#include "zhelpers.hpp"

#include "libStream.hpp"


std::deque <entry_t* > gc_queue;

double get_timestamp(){
	struct timeval tv;
	gettimeofday( &tv, NULL );
	return tv.tv_sec + tv.tv_usec * 1e-6 - 1346322025.0;
	}

uint64_t generate_timestamp( uint64_t id ){
	return (uint64_t)(get_timestamp()) * 1000 + (id % 1000);
	}

void split_string( std::vector<std::string> &parts, std::string &str, size_t N ){
	size_t last = 0;
 	
	for( size_t i = 0 ; ( i < str.size() ) && ( parts.size() < N ) ; ++i ){
		if( str[i] == ' ' ){
			parts.push_back( str.substr( last, i - last ) );
			last = i + 1;
			}
 		}

	 parts.push_back( str.substr( last, std::string::npos ) );

	 }

std::string escape_string( std::string str ){
	return str;	
	}


std::tr1::unordered_map< std::string, Stream  > streams;
std::tr1::unordered_map< uint64_t, std::string  > posts;

bool suisto_add_post( std::string stream_name, std::string post_content ){
	std::tr1::unordered_map< std::string, Stream  >::iterator it;

	it = streams.find( stream_name );

	if( it == streams.end() ){
		return false;
		}	
	
	entry_t *entry = new entry_t;
	entry->id = Clock::next();
	entry->timestamp = generate_timestamp( entry->id );

	posts[entry->id] = escape_string( post_content );

	(it->second).add( entry );

	return true;
	}

bool suisto_create_stream( std::string stream_name ){
	std::tr1::unordered_map< std::string, Stream  >::iterator it;

	it = streams.find( stream_name );

	if( it != streams.end() ){
		return false;
		}	
	
	
	streams[ stream_name ] = Stream();

	return true;
	}

bool suisto_get_latest( std::string stream_name, size_t max_entries, std::vector<entry_t*>& out ){	
	std::tr1::unordered_map< std::string, Stream  >::iterator it;

	it = streams.find( stream_name );

	if( it == streams.end() ){
		return false;
		}	
		
	out = (it->second).latest( max_entries );
		
	return true;
	}

bool suisto_get_since( std::string stream_name, uint64_t ts, std::vector<entry_t*>& out ){	
	std::tr1::unordered_map< std::string, Stream  >::iterator it;

	it = streams.find( stream_name );

	if( it == streams.end() ){
		return false;
		}	
	
	out = (it->second).since( ts );
		
	return true;
	}

std::string serialize_to_json( std::vector<entry_t *>& entries ){
	std::string response = "";
	if( entries.size() > 0 ){
		uint64_t next = entries[entries.size()-1]->timestamp;
		char buffer[32];
		memset( buffer, 0, 32 );
		sprintf( buffer, "%lu", next );
		response = "{\"next\": " + std::string( buffer )  + ", \"entries\": [";
		for( size_t i = 0 ; i < entries.size() ; ++i ){
			memset( buffer, 0, 32 );
			sprintf( buffer, "%lu", entries[i]->id );
			response += "{\"id\": "+std::string( buffer ) + ", ";
			memset( buffer, 0, 32 );
			sprintf( buffer, "%lu", entries[i]->timestamp );
			response += "\"ts\": "+std::string( buffer ) + ", ";
			response += "\"content\": \"" + posts[entries[i]->id] + "\"}";
			if( i != entries.size() - 1 ){
				response += ", ";
				}
			}
		response += "]}";

		}
		else{
			response = "{\"next\": 0, \"entries\": []}";
			}
	return response;
	}


void *suisto_worker( void *arg ){
	
	zmq::context_t *ctx = (zmq::context_t*)arg;

	zmq::socket_t s( *ctx, ZMQ_REP );

	s.connect( "inproc://workers" );
	std::cout << "Worker entering loop..." << std::endl;
	while( true ){
		//zmq::messages_t request;
		std::string response, request = s_recv( s );
		//std::cerr << "request: " << request << std::endl;
		// PUSH NAME CONTENT
		std::vector<std::string> parts;
		split_string( parts, request, 2 );
		/*std::cerr << "RECVD: ";
		for( size_t i = 0 ; i < parts.size() ; ++i ){
				std::cout << "'" << parts[i] << "' "; 
			}
		std::cerr << std::endl;
		

		s_send( s, std::string( "Ok." ) );
		*/

		
		response = "{\"error\": \"Invalid request.\" }";

		if( parts[0] == "CREATE" && parts.size() == 2){
			if( suisto_create_stream( parts[1] ) ){
				response = "{\"response\": \"Ok.\" }";
				}
			else{
				response = "{\"error\": \"Stream already exists.\" }";
				}
			}
		
		if( parts[0] == "PUSH" && parts.size() == 3){
			if( suisto_add_post( parts[1], parts[2] ) ){
				response = "{\"response\": \"Ok.\" }";
				}
			else{
				response = "{\"error\": \"Invalid stream.\" }";
				}
			}

		if( parts[0] == "LATEST" && parts.size() == 2){
			std::vector<entry_t*> entries;
			if( suisto_get_latest( parts[1], 100, entries ) ){
				
				response = serialize_to_json( entries );
				
				}
		
			else{
				response = "{\"error\": \"Invalid stream.\" }";
				}
			
			}
		
		
		if( parts[0] == "SINCE" && parts.size() == 3){
			std::vector<entry_t*> entries;
			unsigned long int since;
			since = atol( parts[2].c_str() );

			if( suisto_get_since( parts[1], since, entries ) ){
				
				response = serialize_to_json( entries );
				
				}
		
			else{
				response = "{\"error\": \"Invalid stream.\" }";
				}
			
			}
		
		s_send( s, response );

		}
	
	
	return NULL;
	}


int main( int argc, char **argv ){
	
	zmq::context_t ctx(2);
	
	zmq::socket_t workers( ctx, ZMQ_XREQ );
	
	workers.bind( "inproc://workers" );

	zmq::socket_t clients( ctx, ZMQ_XREP );
	clients.bind( "tcp://*:5555" );

	std::cout << "Starting worker threads..." << std::endl;
	for( size_t i = 0 ; i < 10 ; ++i ){ 
		pthread_t worker;
		int rc = pthread_create( &worker, NULL, suisto_worker, (void*)&ctx );
		}
	
	std::cout << "Starting queue..." << std::endl << std::endl;
	
	zmq::device( ZMQ_QUEUE, clients, workers );

	return 0;
	}
