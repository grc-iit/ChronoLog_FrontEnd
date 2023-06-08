#include "inverted_list.h"

template<typename T>
void hdf5_invlist::create_invlist(std::string &s,int maxsize)
{

	struct head_node *h  = new struct head_node();

	h->maxsize = maxsize;
	h->intslots = nullptr;
	h->floatslots = nullptr;
	h->doubleslots = nullptr;

	if(std::is_same<T,int>::value)
	{
	     h->intslots = new std::vector<struct list_node<int>*> ();
	     h->intslots->resize(maxsize);
	}

	else if(std::is_same<T,float>::value)
	{
	   h->floatslots = new std::vector<struct list_node<float>*> ();
	   h->floatslots->resize(maxsize);
	}
	else if(std::is_same<T,double>::value)
	{
	   h->doubleslots = new std::vector<struct list_node<double>*> ();
	   h->doubleslots->resize(maxsize);
	}

	std::pair<std::string,struct head_node*> p;
	p.first.assign(s);
	p.second = h;
	invlists.insert(p);

}


template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
void hdf5_invlist::fill_invlist_from_file(std::string &s)
{




}

template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
void hdf5_invlist::add_entry_to_list(std::string &,T &key,int offset)
{






}

template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
std::vector<int> hdf5_invlist::fetch_offsets_from_list(std::string &,T &key)
{








}
