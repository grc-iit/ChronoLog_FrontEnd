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
	memcpy(data.data(),e1.data.data(),e1.data.size());
	return *this; 
   }
};

#endif
