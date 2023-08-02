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
   std::vector<char> data;
   event& operator=(const struct event&e1)
   {
	ts = e1.ts;
	data.assign(e1.data.begin(),e1.data.end());
	return *this; 
   }
   std::string pack_event()
   {
	std::string p = std::to_string(ts);
	p += std::string(data.data());
	return p;
   }
};

struct atomic_buffer
{
   boost::shared_mutex m;
   std::atomic<int> buffer_size;
   std::vector<struct event> *buffer;
};

struct timestampdata
{
  uint64_t ts;
  char *data;

};

#endif
