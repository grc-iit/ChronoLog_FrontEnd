#ifndef __INVERTED_LIST_H_
#define __INVERTED_LIST_H_

#include "rw.h"

template<class KeyT,class ValueT, class HashFcn=std::hash<KeyT>,class EqualFcn=std::equal_to<KeyT>>
struct invnode
{
   BlockMap<KeyT,ValueT,HashFcn,EqualFcn> *bm;
   memory_pool<KeyT,ValueT,HashFcn,EqualFcn> *ml; 
};

struct head_node
{
  int keytype;
  int maxsize;
  struct invnode<int,int> *inttable;
  struct invnode<float,int> *floattable;
  struct invnode<double,int> *doubletable;
};

class hdf5_invlist
{

   private:
	   int numprocs;
	   int myrank;
	   std::unordered_map<std::string,struct head_node*> invlists;
	   int tag;
   public:
	   hdf5_invlist(int n,int p) : numprocs(n), myrank(p)
	   {
	     tag = 20000;

	   }
	   ~hdf5_invlist()
	   {
		std::unordered_map<std::string,struct head_node*>::iterator t;

		for(t = invlists.begin(); t != invlists.end(); ++t)
		{
		   auto r = t->first;
		   auto rv = t->second;
		   
		   if(rv->inttable != nullptr) delete rv->inttable;
		   if(rv->floattable != nullptr) delete rv->floattable;
		   if(rv->doubletable != nullptr) delete rv->doubletable;
		   delete rv;
		}


	   }

	   inline int nearest_power_two(int n)
	   {
		int c = 1;

		while(c < n)
		{
		   c = 2*c;
		}
		return c;
	   }

	   template<typename T>
	   void create_invlist(std::string &,int);
	   template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
	   void fill_invlist_from_file(std::string&,int);
	   template<typename T,class hashfcn=std::hash<T>>
	   int partition_no(T &k);		  
	   template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
	   void add_entries_to_tables(std::string&,std::vector<struct event>*,int,int); 
	   template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
	   void get_entries_from_tables(std::string &,std::vector<std::vector<T>>&,std::vector<std::vector<int>>&,int&,int&);
};

#include "../srcs/inverted_list.cpp"

#endif
