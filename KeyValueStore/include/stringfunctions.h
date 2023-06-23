#ifndef __STRING_FUNCTIONS_H_
#define __STRING_FUNCTIONS_H_

#include "city.h"
#include <string>

struct stringhash
{

   uint64_t operator()(std::string &s)
   {
        uint64_t hashvalue = CityHash64(s.c_str(),64);
        return hashvalue;
   }

};

struct stringequal
{

   bool operator()(std::string &s1,std::string &s2)
   {
        return (s1.compare(s2)==0);
   }
};


#endif
