#ifndef __EVENT_H_
#define __EVENT_H_

#include <climits>
#include <string.h>
#include <iostream>
#include <string.h>
#include <vector>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lock_types.hpp>
#include <boost/thread/shared_mutex.hpp>

using namespace boost;

struct event
{
   uint64_t ts;
   char data[VALUESIZE];
   event& operator=(const struct event&e1)
   {
	ts = e1.ts;
	memcpy(data,e1.data,VALUESIZE);
	return *this; 
   }
};

struct atomic_buffer
{
   boost::shared_mutex m;
   std::vector<struct event> *buffer;
};

#endif
