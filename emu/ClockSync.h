#ifndef __CLOCK_SYNC_
#define __CLOCK_SYNC_

#include <chrono>
#include <time.h>
#include <unistd.h>
#include <climits>
#include <mpi.h>
#include <vector>
#include <iostream>
#include <cassert>

class ClocksourceCStyle
{
public:
    ClocksourceCStyle()
    {}
    ~ClocksourceCStyle()
    {}    
    uint64_t getTimestamp()
    {
        struct timespec t{};
        clock_gettime(CLOCK_TAI, &t);
        return t.tv_sec;
    }
};
 
class ClocksourceCPPStyle
{
public:
    ClocksourceCPPStyle()
    {}
    ~ClocksourceCPPStyle()
    {}    
    uint64_t getTimestamp()
    {
        auto t = std::chrono::high_resolution_clock::now().time_since_epoch().count();
	return t;
    }
};


template<class Clocksource>
class ClockSynchronization
{
 
   private:
     Clocksource *clock;
     uint64_t maxError;
     int myrank;
     int numprocs;
     uint64_t myoffset;
     uint64_t unit;
     uint64_t epsilon;
     uint64_t delay;
     bool is_less;
   public:
     ClockSynchronization(int n1,int n2,std::string &q) : myrank(n1), numprocs(n2)
     {
	 if(q.compare("second")==0) unit = 1000000000;
	 else if(q.compare("microsecond")==0) unit = 1000;
         else if(q.compare("millisecond")==0) unit = 1000000;
	 else unit = 1;
	 myoffset = 0;
	 maxError = 0;
	 epsilon = 400000;  //100 microseconds (scheduling, measurement errors)
	 delay = 500000; // 200 microseconds network delay for reasonably large messages
	 delay = delay/unit;
	 epsilon = epsilon/unit;
	 is_less = false;
     }
     ~ClockSynchronization()
     {}
     
     uint64_t Timestamp()
     {
	 if(!is_less)
	 return clock->getTimestamp()/unit+myoffset;
	 else
	  return clock->getTimestamp()/unit-myoffset;
     }

     bool NearTime(uint64_t ts)
     {
	uint64_t myts = Timestamp();
	uint64_t diff = UINT64_MAX;
	if(myts < ts) diff = ts-myts;
	else diff = myts-ts;
	if(diff <= 2*maxError+delay+epsilon) return true;
	else 
	{
		return false;
	}
     }
     void SynchronizeClocks();
     void ComputeErrorInterval();

};

#include "clocksync.cpp"

#endif
