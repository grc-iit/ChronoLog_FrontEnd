#ifndef __EXTERNAL_SORT_H_
#define __EXTERNAL_SORT_H_

#include "rw.h"



class hdf5_sort
{
   private: 
	int numprocs;
	int myrank;



  public: 
       hdf5_sort(int n,int p) : numprocs(p), myrank(p)
       {

       }
       ~hdf5_sort()
       {

       }

       std::string sort_on_secondary_key(std::string &,std::string &);
       std::string merge_datasets(std::string &,std::string &);
       std::string merge_stream_with_dataset(std::string &,std::vector<struct event>*);
       std::string merge_multiple_dataset(std::vector<std::string>&);
};

#endif
