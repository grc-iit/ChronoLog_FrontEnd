#ifndef __EVENT_H_
#define __EVENT_H_

#include <climits>
#include <string.h>
#include <iostream>
#include <string.h>

struct event
{
   uint64_t ts;
   char data[100];

   event& operator=(const struct event&e1)
   {
	ts = e1.ts;
	memcpy(data,e1.data,100);
	return *this; 
   }
};

#endif
