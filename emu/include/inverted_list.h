#ifndef __INVERTED_LIST_H_
#define __INVERTED_LIST_H_

#include "rw.h"

template<typename T>
struct list_node
{
   T key;
   uint64_t offset;
   struct list_node<T> *next;
};

struct head_node
{
   int maxsize;
   std::string filename;
   std::vector<struct list_node<int> *> *intslots;
   std::vector<struct list_node<float>*> *floatslots;
   std::vector<struct list_node<double>*> *doubleslots;
};

class hdf5_invlist
{

   private:
	   int numprocs;
	   int myrank;
	   std::unordered_map<std::string,struct head_node*> invlists;

   public:
	   hdf5_invlist(int n,int p) : numprocs(n), myrank(p)
	   {


	   }
	   ~hdf5_invlist()
	   {
		std::unordered_map<std::string,struct head_node*>::iterator t;

		for(t = invlists.begin(); t != invlists.end(); ++t)
		{
		   auto r = t->first;
		   auto rv = t->second;
		   
		   if(rv->intslots != nullptr) delete rv->intslots;
		   if(rv->floatslots != nullptr) delete rv->floatslots;
		   if(rv->doubleslots != nullptr) delete rv->doubleslots;
		}


	   }

	   template<typename T>
	   void create_invlist(std::string &,int);
	   template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
	   void fill_invlist_from_file(std::string&);
	   template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
	   void add_entry_to_list(std::string &,T &key,int);
	   template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
	   std::vector<int> fetch_offsets_from_list(std::string&,T&key);
};

#include "../srcs/inverted_list.cpp"

#endif
