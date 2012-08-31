#ifndef _LIBSTREAM_HPP_
#define _LIBSTREAM_HPP_

#include <cstdio>
#include <cstdlib>
#include <iostream>

#include <stdint.h>

#include <map>
#include <vector>

#include <deque>

#include <iterator>
#include <algorithm>


#include <time.h>
#include <sys/time.h>
 

typedef struct {
	uint64_t timestamp;
	uint64_t id;
	size_t ref_count;
	} entry_t;


extern std::deque <entry_t* > gc_queue;

class Clock {
	public:
		static uint64_t next();
		static uint64_t current_ts;
	};


class Stream {
	public:
		Stream();
		Stream( size_t );

		void add( entry_t * );
		
		std::vector<entry_t *> latest( size_t );
		std::vector<entry_t *> since( uint64_t );

		void merge( Stream& );
		void merge( Stream&, uint64_t );

		uint64_t head();			
	
	private:

		void cleanup();

		std::map< uint64_t, entry_t* > entries;
		size_t max_entries;
		uint64_t largest_timestamp;
	
	};

#endif
