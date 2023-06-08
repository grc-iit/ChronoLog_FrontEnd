#ifndef __POST_PROCESSING_H_
#define __POST_PROCESSING_H_


#include "external_sort.h"
#include "inverted_list.h"
#include <mpi.h>
#include <iostream>

class file_post_processing
{

   private :
   	    int myrank;
 	    int numprocs;	    
  	    hdf5_sort *hs; 
	    hdf5_invlist *iv;	    

   public :
	    file_post_processing(int n,int p) : numprocs(n),myrank(p)
	    {

                H5open();
		hs = new hdf5_sort(numprocs,myrank);
		iv = new hdf5_invlist(numprocs,myrank);
	    }

	    void sort_on_secondary_key()
	    {

		std::string s = "table0";

		std::string attrname = "attr"+std::to_string(0);
                std::string attr_type = "integer";
                std::string outputfile = hs->sort_on_secondary_key<int>(s,attrname,0,0,UINT64_MAX,attr_type);

                hs->merge_tree<int>(s,0);
	    }

	    void create_invlist(std::string &s,int maxsize,int kt)
	    {
		if(kt==0)
		iv->create_invlist<int>(s,maxsize);
		else if(kt==1)
		iv->create_invlist<float>(s,maxsize);
		else if(kt==2)
		iv->create_invlist<double>(s,maxsize);
	    }

	    ~file_post_processing()
	    {
		delete hs;
		delete iv;
		H5close();
	    }

	    
};

#endif
