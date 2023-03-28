#ifndef __EVENT_H_
#define __EVENT_H_

#include <climits>
#include <string.h>
#include <iostream>
#include <string.h>

#define DATASIZE 1000

struct event
{
   uint64_t ts;
   char data[DATASIZE];

   event& operator=(const struct event&e1)
   {
	ts = e1.ts;
	memcpy(data,e1.data,DATASIZE);
	return *this; 
   }
};

#endif
