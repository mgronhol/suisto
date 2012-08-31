#include "libStream.hpp"

/*
double get_time(){
	struct timeval tv;
	gettimeofday( &tv, NULL );
	return tv.tv_sec + tv.tv_usec * 1e-6;
	}
*/


uint64_t Clock :: current_ts = 0;

uint64_t Clock :: next(){
	return Clock::current_ts++;
	}




Stream :: Stream() {
	max_entries = 100;
	largest_timestamp = 0;
	}

Stream :: Stream( size_t size ){
	max_entries = size;
	largest_timestamp = 0;
	}

void Stream :: cleanup(){
	if( entries.size() <= max_entries ){
		return;
		}

	size_t count = entries.size() - max_entries;
	std::map< uint64_t, entry_t*>::iterator it = entries.begin();
	//std::cout << "Erasing " << count << " elements" << std::endl;
	for( size_t i = 0 ; i < count ; ++i){
		(it->second)->ref_count -= 1;
		if( (it->second)->ref_count < 1 ){
			gc_queue.push_back( it->second );
			}
		it++;  
		}
	
	entries.erase( entries.begin(), it );
	}

void Stream :: add( entry_t* entry ){
	entries[ entry->timestamp ] = entry;
	entry->ref_count += 1;
	if( entry->timestamp > largest_timestamp ){
		largest_timestamp = entry->timestamp;
		}
	cleanup();
	}

uint64_t Stream::head(){
	return largest_timestamp;
	}

std::vector< entry_t* > Stream :: latest( size_t N ){
	std::vector< entry_t* > out;
	std::map< uint64_t, entry_t*  >::reverse_iterator rit;
	
	for( rit = entries.rbegin() ; rit != entries.rend() && out.size() < N ; rit++ ){
		out.push_back( rit->second );
		}
	std::reverse( out.begin(), out.end() );
	return out;
	}

std::vector< entry_t * > Stream :: since( uint64_t ts ){
	std::vector< entry_t* > out;
	std::map< uint64_t, entry_t* >::iterator it;
	it = entries.upper_bound( ts );
	for( ; it != entries.end() ; it++ ){
		out.push_back( it->second );
		}

	return out;
	}


void Stream :: merge( Stream &other ){
	merge( other, 0 );
	}

void Stream :: merge( Stream &outer, uint64_t ts ){
	std::vector< entry_t *> out = outer.since( ts );
	for( size_t i = 0 ; i < out.size() ; ++i ){
		add( out[i] );
		}
	}


/*
int main( int argc, char **argv ){

	Stream stream( 10 );
	Stream stream2( 5 );
	

	for( size_t i = 0 ; i < 25 ; ++i ){
		entry_t *entry = new entry_t;
		entry->timestamp = Clock::next();
		entry->id = i;
		if( i % 3 == 0 ){
			entry->id = 1000;
			stream2.add( entry );
			}
		else {
			stream.add( entry );
			}
		}
		
	std::vector< entry_t* > latest = stream.latest( 32 );
	
	printf( "Latest:\n");
	for( size_t i = 0 ; i < latest.size() ; ++i ){
		printf( "\t%i: ts->%lu id->%lu \n", i, latest[i]->timestamp, latest[i]->id );
		
		}
		
	latest = stream.since( 10 );
	
	printf( "Since 10:\n");
	for( size_t i = 0 ; i < latest.size() ; ++i ){
		printf( "\t%i: ts->%lu id->%lu \n", i, latest[i]->timestamp, latest[i]->id );
		
		}
	
	stream.merge( stream2 );
	latest = stream.since( 10 );
	
	printf( "Since 10:\n");
	for( size_t i = 0 ; i < latest.size() ; ++i ){
		printf( "\t%i: ts->%lu id->%lu \n", i, latest[i]->timestamp, latest[i]->id );
		
		}


	Stream stream0( 10000 );
	Stream stream1( 10000 );

	double t0, t1;
	size_t N = 100000;

	t0 = get_time();
	for( size_t i = 0 ; i < N ; ++i ){
		entry_t *entry = new entry_t;
		entry->id = i;
		entry->timestamp = Clock::next();
		stream0.add( entry );
		}	
	t1 = get_time();

	
	for( size_t i = 0 ; i < N ; ++i ){
		entry_t *entry = new entry_t;
		entry->id = i + 200000;
		entry->timestamp = Clock::next();
		stream1.add( entry );
		}	
	
	printf( "It took %.3f msecs to add %lu entries to stream. \n\n", (t1-t0)*1000, N );

	t0 = get_time();

	for( size_t i = 0 ; i < 100 ; ++i ){
		Stream tmp(1000);
		tmp.merge( stream0 );
		tmp.merge( stream1 );
		//stream0.merge( stream1 );
		}
	t1 = get_time();

	printf( "It took %.3f msecs to combine two streams. \n\n", (t1 - t0)/100 * 1000 );

	std::vector<entry_t *> result;

	t0 = get_time();

	for( size_t i = 0 ; i < 100 ; ++i ){
		result = stream0.latest( 100 );
		}
	t1 = get_time();

	printf( "It took %.3f msecs to query 100 latest entries. \n\n", (t1-t0)/100 * 1000 );

	t0 = get_time();

	for( size_t i = 0 ; i < 100 ; ++i ){
		result = stream0.since( stream0.head() - 500  );
		}
	t1 = get_time();

	printf( "It took %.3f msecs to query %lu latest entries. \n\n", (t1-t0)/100 * 1000, result.size() );
	
	
	return 0;
	i}

	*/
