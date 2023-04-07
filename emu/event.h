#ifndef __EVENT_H_
#define __EVENT_H_

#include <climits>
#include <string.h>
#include <iostream>
#include <string.h>
#include <vector>

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
};

struct event_hdf
{
   uint64_t ts;
   char data[40];
   double data_d;
};

#endif
