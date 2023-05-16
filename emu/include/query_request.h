#ifndef __QUERY_REQUEST_H_
#define __QUERY_REQUEST_H_


#include <string>

struct query_req
{
   
 std::string name;
 uint64_t minkey;
 uint64_t maxkey;
 bool collective;
 int op;
};

#endif
