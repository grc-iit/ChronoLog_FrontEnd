#ifndef __QUERY_REQUEST_H_
#define __QUERY_REQUEST_H_


#include <string>

struct query_req
{
   
 std::string name;
 uint64_t minkey;
 uint64_t maxkey;
 int id;
 int sender;
 bool from_nvme;
 bool sorted;
 bool collective;
 bool single_point;
 bool output_file;
 int op;
};

#endif
