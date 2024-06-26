
template<typename T,typename N>
int KeyValueStoreAccessor::add_new_inverted_list(std::string &table,std::string &attr_name,int size,int ntables,N &emptykey,data_server_client *d,KeyValueStoreIO *io,Interface_Queues *q,int c,int data_size)
{
      T *invlist = new T(numprocs,myrank,size,ntables,emptykey,table,attr_name,d,io,q,c,data_size);

      std::string type = md.get_type(attr_name);
      int keytype = 0;
      if(type.compare("int")==0) keytype = 0;
      else if(type.compare("unsignedlong")==0) keytype=1;
      else if(type.compare("float")==0) keytype=2;
      else if(type.compare("double")==0) keytype=3;
      
      kio->add_query_service(keytype,(void*)invlist); 
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
   bool b = invlist->put_entry(key,ts);
   return b;
}

template<typename T,typename N,typename M>
uint64_t KeyValueStoreAccessor::Put_ts(int pos,std::string &s,N &key,M &value)
{
  if(pos >= lists.size()) return UINT64_MAX;
  int ksize = sizeof(N);
  std::string data = value;
  assert (data.length()==value.length());

  std::vector<uint64_t> ts;
  
  do
  {
     ts = if_q->PutEmulatorEvent(s,data,myrank);
  }while(ts[0]==UINT64_MAX && ts[1]==0);

  bool b = false;
  if(ts[0] != UINT64_MAX)
  {
	T *invlist = reinterpret_cast<T*>(lists[pos].second);
	b = invlist->put_entry(key,ts[0]);
  }
  return ts[0];
}
template<typename T,typename N,typename M>
bool KeyValueStoreAccessor::Put(int pos,std::string &s,N &key, M &value)
{
   if(pos >= lists.size()) return false;

   int ksize = sizeof(N);

   std::string data = value;

   assert(data.length()==value.length());

   std::vector<uint64_t> ts;
   do
   {
      ts = if_q->PutEmulatorEvent(s,data,myrank);
   }while(ts[0]==UINT64_MAX && ts[1]==0);

   bool b = false;
   if(ts[0] != UINT64_MAX)
   {
	T *invlist = reinterpret_cast<T*>(lists[pos].second);
	b = invlist->put_entry(key,ts[0]);
	compute_summary<N>(key);
	if(b) inserts++;
	return true;
   }
   else if(ts[1]==2) return false;
   else return true;
}


template<typename T,typename N>
bool KeyValueStoreAccessor::Get(int pos,std::string &s,N &key,int id)
{
   if(pos >= lists.size()) return false;

   uint64_t ts = UINT64_MAX;
   bool b = false;
   std::vector<uint64_t> values;

   T *invlist = reinterpret_cast<T*>(lists[pos].second);
   int pid = invlist->get_entry(key,values);

   if(values.size()>0)
   {
     ts = values[values.size()-1];
     std::string eventstring = if_q->GetEmulatorEvent(s,ts,myrank);
     if(eventstring.length() != 0)
	invlist->add_event_file(eventstring);
     else
     {
       bool bp = false;
       invlist->AddPending(key,values,bp,id,pid);
     }
   }
   else
   {
	pid = invlist->partition_no(key);
	if(!invlist->CheckLocalFileExists())
	{
	   std::string filename = "file";
	   filename +=  s + ".h5";
	   if(if_q->CheckFileExistence(filename,myrank))
	   {
		invlist->LocalFileExists();
	   }
	}
	bool bp = false;
	invlist->AddPending(key,values,bp,id,pid);	

   }
   return true;

}

template<typename T,typename N>
bool KeyValueStoreAccessor::Get_resp(int pos,std::string &s,N &key,int id)
{
   if(pos >= lists.size()) return false;
   uint64_t ts = UINT64_MAX;
   bool b = false;
   std::vector<uint64_t> values;

   T *invlist = reinterpret_cast<T*>(lists[pos].second);
   int pid = invlist->get_entry(key,values);
   if(values.size()>0)
   {
     ts = values[values.size()-1];
     std::string eventstring = if_q->GetEmulatorEvent(s,ts,myrank);
     if(eventstring.length()==0)
     {
        if(!invlist->CheckLocalFileExists())
        {
           std::string filename = "file";
           filename += s+".h5";
           if(if_q->CheckFileExistence(filename,myrank))
           {
                invlist->LocalFileExists();
           }
        }
        bool bp = true;

        invlist->AddPending(key,values,bp,id,pid);
     }
     else invlist->add_response(key,eventstring,id);

   }
   else
   {
        pid = invlist->partition_no(key);

        if(!invlist->CheckLocalFileExists())
        {
           std::string filename = "file";
           filename +=  s + ".h5";
           if(if_q->CheckFileExistence(filename,myrank))
           {
                invlist->LocalFileExists();
           }
        }
        bool bp = true;
        invlist->AddPending(key,values,bp,id,pid);
   }

   return true;
}

template<typename T,typename N>
std::vector<std::pair<int,std::string>> KeyValueStoreAccessor::Completed_Gets(int pos,std::string &s)
{
   std::vector<std::pair<int,std::string>> ids;
   if(pos >= lists.size()) return ids;

   T *invlist = reinterpret_cast<T*>(lists[pos].second);
   ids = invlist->get_completed_ids();
   return ids;
}

template<typename T,typename N>
std::vector<uint64_t> KeyValueStoreAccessor::get_entry(int pos,N &key)
{
   std::vector<uint64_t> values;
   if(pos >= lists.size()) return values;

   T *invlist = reinterpret_cast<T*>(lists[pos].second);
   int ret = invlist->get_entry(key,values);
   return values;
}

template<typename T,typename N>
void KeyValueStoreAccessor::create_summary(int rows,int cols)
{
  sa.average = 0;
  sa.count = 0;
  sa.min_sketch_table.resize(rows);

  for(int i=0;i<rows;i++)
  {
	sa.min_sketch_table[i].resize(cols);
	std::fill(sa.min_sketch_table[i].begin(),sa.min_sketch_table[i].end(),0);
  }

  sa.nrows = rows;
  sa.ncols = cols;
  sa.numleaves = 10;
  sa.ranges.resize(sa.numleaves);
  double min = 0; double max = INT_MAX;
  double res = (max-min)/sa.numleaves;
  double start = min;
  for(int i=0;i<sa.numleaves;i++)
  {
	std::pair<double,double> p;
	p.first = i*res;
	p.second = (i+1)*res;
	if(i==sa.numleaves-1) p.second = max;
	sa.ranges[i].first = p.first;
	sa.ranges[i].second = p.second;
	sa.min_sketch_tables.push_back(sa.min_sketch_table);
  }
}

template<typename T,typename N>
void KeyValueStoreAccessor::compute_summary(N &key)
{
        double sum = sa.average*sa.count;
	sum += (double)key;
	sa.count++;
	sa.average = sum/(double)sa.count;

	for(int i=0;i<sa.min_sketch_table.size();i++)
	{
	    uint64_t seed = (uint64_t)i;
	    boost::hash_combine(seed,key);
	    uint64_t key_v = seed;
            uint64_t pos = key_v %sa.min_sketch_table[i].size();
	    sa.min_sketch_table[i][pos]++; 
	}

	int leafno = -1;
	for(int i=0;i<sa.ranges.size();i++)
	{
	  if(key >= sa.ranges[i].first && key <sa.ranges[i].second)
	  {
		leafno = i; break;
	  }

	}

	for(int i=0;i<sa.min_sketch_tables[leafno].size();i++)
	{
	   uint64_t seed = (uint64_t)i;
	   boost::hash_combine(seed,key);
	   uint64_t key_v = seed;
	   uint64_t pos = key_v%sa.min_sketch_tables[leafno][i].size();
	   sa.min_sketch_tables[leafno][i][pos]++;
	}
}

template<typename T,typename N>
void KeyValueStoreAccessor::collect_summary(int tag)
{
  MPI_Request *reqs = new MPI_Request[2*numprocs];

  std::vector<double> avg_count;
  avg_count.resize(2);
  avg_count[0]=sa.average;
  avg_count[1]=sa.count;

  std::vector<double> recv_values;
  recv_values.resize(2*numprocs);
  std::fill(recv_values.begin(),recv_values.end(),0);

  int nreq = 0;

  for(int i=0;i<numprocs;i++)
  {
     MPI_Isend(avg_count.data(),2,MPI_DOUBLE,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
     nreq++;
     MPI_Irecv(&recv_values[2*i],2,MPI_DOUBLE,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
     nreq++;
  }

  MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

  double sum = 0;
  double count_t = 0;
  for(int i=0;i<numprocs;i++)
  {
	sum += recv_values[2*i]*recv_values[2*i+1];
        count_t += recv_values[2*i+1];
  }

  double average = sum/count_t;

  std::vector<double> send_buffer;
  std::vector<double> recv_buffer;

  send_buffer.resize(sa.nrows*sa.ncols);
  recv_buffer.resize(sa.nrows*sa.ncols*numprocs);
  std::fill(send_buffer.begin(),send_buffer.end(),0);
  std::fill(recv_buffer.begin(),recv_buffer.end(),0);

  for(int i=0;i<sa.min_sketch_table.size();i++)
  {
     for(int j=0;j<sa.min_sketch_table[i].size();j++)
     {
        send_buffer[i*sa.ncols+j] = sa.min_sketch_table[i][j];
     }
  }

  
  nreq = 0;
  for(int i=0;i<numprocs;i++)
  {
      MPI_Isend(send_buffer.data(),sa.nrows*sa.ncols,MPI_DOUBLE,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
      nreq++;
      MPI_Irecv(&recv_buffer[i*sa.nrows*sa.ncols],sa.nrows*sa.ncols,MPI_DOUBLE,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
      nreq++;
  }

  MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);
  
  sa_t.average = average;
  sa_t.count = count_t;
  sa_t.nrows = sa.nrows;
  sa_t.ncols = sa.ncols;

  sa_t.min_sketch_table.resize(sa.nrows);
  for(int i=0;i<sa.nrows;i++)
  {
	sa_t.min_sketch_table[i].resize(sa.ncols);
	std::fill(sa_t.min_sketch_table[i].begin(),sa_t.min_sketch_table[i].end(),0);
  } 

  
  for(int i=0;i<numprocs;i++)
  {
     int d = i*sa_t.nrows*sa_t.ncols;
     for(int j=0;j<sa_t.nrows;j++)
     {
	  for(int k=0;k<sa_t.ncols;k++)
	  {
	     int p = j*sa_t.ncols+k;
	     sa_t.min_sketch_table[j][k] += recv_buffer[d+p];
	  }
     }
  }

  delete reqs;
}

template<typename T,typename N>
int KeyValueStoreAccessor::count_min(N &key)
{
    int min_value = INT_MAX;

    for(int i=0;i<sa_t.min_sketch_table.size();i++)
    {
         uint64_t seed = (uint64_t)i;
	 uint64_t k = (uint64_t)key;
         boost::hash_combine(seed,k);
	 uint64_t hash_value = seed;
	 int pos = hash_value%sa_t.min_sketch_table[i].size();
	 double v = sa_t.min_sketch_table[i][pos];
	 if(min_value > v) min_value = v;
    }
    return min_value;
}

template<typename T>
void KeyValueStoreAccessor::flush_invertedlist(std::string &attr_name,bool p)
{
    int offset = md.locate_offset(attr_name);

    std::string tname = md.db_name();

    if(offset==-1) return;

    int pos = get_inverted_list_index(attr_name); 

    if(pos==-1) return;

    T *invlist = reinterpret_cast<T*>(lists[pos].second);

    if(!invlist->CheckLocalFileExists())
    {
           std::string filename = "file";
           filename += tname+".h5";
           if(if_q->CheckFileExistence(filename,myrank))
           {
                invlist->LocalFileExists();
           }
    }
    
    struct sync_request *r = new struct sync_request();
    std::string type = md.get_type(attr_name);
    int keytype = 0;
    if(type.compare("int")==0) keytype = 0;
    else if(type.compare("unsignedlong")==0) keytype=1;
    else if(type.compare("float")==0) keytype=2;
    else if(type.compare("double")==0) keytype=3;

    r->funcptr = (void*)invlist;
    r->name = md.db_name();
    r->attr_name = attr_name; 
    r->offset = offset;
    r->keytype = keytype;
    r->flush = true;
    r->persist = p;

    bool ret = kio->LocalPutSyncRequest(r);

}

