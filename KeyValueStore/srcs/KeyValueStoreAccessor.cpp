
template<typename T,typename N>
int KeyValueStoreAccessor::add_new_inverted_list(std::string &attr_name,int size,N &emptykey,std::string &pre,data_server_client *d)
{
      T *invlist = new T(numprocs,myrank,size,emptykey,pre,d); 
      invlist->bind_functions();

      std::pair<std::string,void*> sp;
      sp.first = attr_name;
      sp.second = (void*)invlist;
      lists.push_back(sp);
      std::pair<std::string,int> ip;
      ip.first = attr_name;
      ip.second = lists.size()-1;
      secondary_attributes.insert(ip);
      return lists.size()-1;
}
template<typename T>
bool KeyValueStoreAccessor::delete_inverted_list(int n)
{
    if(n < lists.size())
    {
	T *invlist = reinterpret_cast<T*>(lists[n].second);
	delete invlist;
	return true;
    }
    return false;
}

template<typename T,typename N>
bool KeyValueStoreAccessor::insert_entry(int pos, N&key,uint64_t &ts)
{
   if(pos >= lists.size()) return false;

   T *invlist = reinterpret_cast<T*>(lists[pos].second);


}


