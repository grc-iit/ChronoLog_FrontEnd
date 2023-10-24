#include "rw.h"

#define DATASETNAME1 "Data1"

typedef int DATATYPE;

bool read_write_process::create_buffer(int &num_events,std::string &s)
{
    int datasize = 0;
    int index = -1;
    event_metadata em;
    m1.lock();
    auto r = write_names.find(s);
    if(r != write_names.end())
    {		
	index = (r->second).first;
	em = (r->second).second;
    }
    m1.unlock();
    datasize = em.get_datasize();
    assert(index != -1 && datasize > 0 && num_events > 0);

    atomic_buffer *ab = dm->get_atomic_buffer(index);
    ab->buffer_size.store(0);
    try
    {
	ab->buffer->resize(num_events);
	ab->datamem->resize(num_events*datasize);
	for(int i=0;i<ab->valid->size();i++)
		(*ab->valid)[i].store(0);
    }
    catch(const std::exception &except)
    {
	std::cout<<except.what()<<std::endl;
	exit(-1);
    }
    return true;
}

std::vector<uint64_t> read_write_process::add_event(std::string &s,std::string &data)
{
    int index = -1;
    event_metadata em;

    m1.lock();
    auto r = write_names.find(s);
    if(r != write_names.end())
    {
	index = (r->second).first;
	em = (r->second).second;
    }
    m1.unlock();

    int datasize = em.get_datasize();
 
   if(data.length()!=datasize) std::cout <<" data length = "<<data.length()<<" datasize = "<<datasize<<std::endl;  
    assert (data.length() == datasize);

    atomic_buffer *ab = dm->get_atomic_buffer(index);

    uint64_t ts = UINT64_MAX;

    std::vector<uint64_t> res;

    int b = 0;

    boost::shared_lock<boost::shared_mutex> lk(ab->m);
    {
      ts = CM->Timestamp();
      b = dm->add_event(index,ts,data,em);
      if(b!=1) ts = UINT64_MAX;
    }
    res.push_back(ts);
    res.push_back(b);

    return res;
}

void read_write_process::create_events(int num_events,std::string &s,double arrival_rate)
{
    int datasize = 0;
    int index = -1;
    event_metadata em;
    m1.lock();
    auto r = write_names.find(s);
    if(r != write_names.end())
    {
      index = (r->second).first;
      em = (r->second).second;
    }
    m1.unlock();

    datasize = em.get_datasize();
  
    assert(index != -1 && datasize >  0 && num_events > 0);

    atomic_buffer *ab = dm->get_atomic_buffer(index);

    ab->buffer_size.store(0);
    try
    {
      ab->buffer->resize(num_events);
      ab->datamem->resize(num_events*datasize);
      for(int i=0;i<ab->valid->size();i++)
	      (*ab->valid)[i].store(0);
    }
    catch(const std::exception &except)
    {
	std::cout <<except.what()<<std::endl;
	exit(-1);
    }

    int num_dropped = 0;

    boost::shared_lock<boost::shared_mutex> lk(ab->m); 

    for(int i=0;i<num_events;i++)
    {
	event e;
	uint64_t ts = CM->Timestamp();

	e.ts = ts;
	      
	bool b = dm->add_event(e,index,em);
	if(!b) num_dropped++;
	usleep(20000);
    }

}


void read_write_process::sort_events(std::string &s)
{
      m1.lock();
      auto r = write_names.find(s);
      int index = -1;
      int datasize = -1;
      event_metadata em;
      if(r != write_names.end())
      {
        index = (r->second).first;
        em = (r->second).second;
      }
      m1.unlock();
      datasize = em.get_datasize();
      int nm_index = nm->buffer_index(s);

      if(index == -1 || nm_index == -1)
      {
	throw std::runtime_error("write stream or nvme file does not exist");
      }

      while(nm->get_buffer(nm_index,nm_index,1)==false);
      
      boost::unique_lock<boost::shared_mutex> lk1(myevents[index]->m);
      { 
        ds->get_unsorted_data(myevents[index]->buffer,myevents[index]->datamem,index);
        uint64_t min_v,max_v;
        int numevents = myevents[index]->buffer_size.load();
	int maxevents = myevents[index]->buffer->size();
        myevents[index]->buffer_size.store(0);
        if(ds->sort_data(index,index,numevents,min_v,max_v,em))
        myevents[index]->buffer_size.store(myevents[index]->buffer->size());

        uint64_t minv = std::min(min_v,(*write_interval)[index].first.load());
        (*write_interval)[index].first.store(minv);
        (*write_interval)[index].second.store(max_v);
      
        nm->copy_to_nvme(s,myevents[index]->buffer,myevents[index]->buffer_size.load());
      
        clear_write_events(index,min_v,max_v);
	myevents[index]->buffer->resize(maxevents);
	myevents[index]->datamem->resize(maxevents*datasize);

      }
      
      nm->release_buffer(nm_index);
}

void read_write_process::clear_write_events(int index,uint64_t& min_k,uint64_t& max_k)
{
   if(index != -1)
   {
	dm->clear_write_buffer_no_lock(index);
	uint64_t min_n = max_k+1;
	uint64_t max_n = UINT64_MAX;
	dm->set_valid_range(index,min_n,max_n);
	num_dropped[index] = 0;
   }

}

void read_write_process::spawn_write_stream(int index,std::string &sname)
{
    t_args[index].tid = index;   
    t_args[index].endsession = false;
    t_args[index].name = sname;

    std::function<void(struct thread_arg_w*)> DataFunc(
    std::bind(&read_write_process::data_stream,this,std::placeholders::_1));

    std::thread t{DataFunc,&t_args[index]};
    workers[index] = std::move(t);
}

void read_write_process::spawn_write_streams(std::vector<std::string> &snames,std::vector<int> &total_events,int nbatches)
{

	int num_threads = snames.size();
        t_args.resize(num_threads);
        workers.resize(num_threads);

        for(int i=0;i<snames.size();i++)
        {
               t_args[i].tid = i;
	       t_args[i].endsession = false;
               int numevents = total_events[i];
               int events_per_proc = numevents/numprocs;
               int rem = numevents%numprocs;
               if(myrank < rem) t_args[i].num_events = events_per_proc+1;
               else t_args[i].num_events = events_per_proc;
               t_args[i].name = snames[i];
         }

	 std::function<void(struct thread_arg_w *)> DataFunc(
         std::bind(&read_write_process::data_stream,this,std::placeholders::_1));

         for(int j=0;j<num_threads;j++)
         {
              std::thread t{DataFunc,&t_args[j]};
              workers[j] = std::move(t);
         }

}

void read_write_process::pwrite_extend_files(std::vector<std::string>&sts,std::vector<hsize_t>&total_records,std::vector<hsize_t>&offsets,std::vector<std::pair<std::vector<struct event>*,std::vector<char>*>>&data_arrays,std::vector<uint64_t>&minkeys,std::vector<uint64_t>&maxkeys,bool clear_nvme,std::vector<int>&bcounts,std::vector<std::vector<std::vector<int>>>&blockcounts)
{
    hid_t       fid;
    hid_t       acc_tpl;
    hid_t       xfer_plist;
    hid_t       sid;
    hid_t       file_dataspace;
    hid_t       mem_dataspace;
    hid_t       dataset1;
    hid_t       datatype;
    hsize_t dims[1];
    const char* attr_name[1];
    hsize_t adims[1];
    hsize_t maxsize;

    size_t num;
    hbool_t op_failed = false;

    adims[0] = 0;

    hsize_t attr_size[1];
    attr_size[0] = MAXBLOCKS*4+4;
    attr_name[0] = "Datasizes";

    std::vector<hid_t> event_ids;
    std::vector<hid_t> filespaces;
    std::vector<hid_t> memspaces;
    std::vector<hid_t> gids;
    std::vector<hid_t> fids;
    std::vector<hid_t> dset_ids;
    std::vector<hid_t> type_ids;
    std::vector<event_metadata> metadata;
    std::vector<int> valid_id;

    for(int i=0;i<sts.size();i++)
    {
   
    //hid_t es_id = H5EScreate();

    hid_t async_fapl = H5Pcreate(H5P_FILE_ACCESS);
    hid_t async_dxpl = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_fapl_mpio(async_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    H5Pset_dxpl_mpio(async_dxpl, H5FD_MPIO_COLLECTIVE);
    std::string filename = "file"+sts[i]+".h5";
    //fid = H5Fopen_async(filename.c_str(), H5F_ACC_RDWR, async_fapl,es_id);
    fid = H5Fopen(filename.c_str(), H5F_ACC_RDWR, async_fapl);

    event_metadata em;
    int index = -1;
    m1.lock();
    auto r = write_names.find(sts[i]);
    if(r != write_names.end())
    {
       em = (r->second).second;
       index = (r->second).first;
    }
    m1.unlock();

    if(index==-1) 
    {
	throw std::runtime_error("data stream buffer does not exist");
	return;
    }

    int datasize = em.get_datasize();
    int keyvaluesize = sizeof(uint64_t)+datasize;
    metadata.push_back(em);

    if(data_arrays[i].second == nullptr || total_records[i]==0) continue;

    adims[0] = datasize;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,keyvaluesize);
    H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"data",HOFFSET(struct event,data),s1);
    //type_ids.push_back(s2);
    //type_ids.push_back(s1);

    //hid_t grp_id = H5Gopen_async(fid, grp_name.c_str(),gapl, es_id); 
    
    //dataset1 = H5Dopen_async(fid, DATASETNAME1, H5P_DEFAULT,es_id);
    dataset1 = H5Dopen(fid, DATASETNAME1, H5P_DEFAULT);

    //hid_t attr_id = H5Aopen_async(dataset1,attr_name[0],H5P_DEFAULT,es_id);
    hid_t attr_id = H5Aopen(dataset1,attr_name[0],H5P_DEFAULT);
    std::vector<uint64_t> attrs;
    attrs.resize(attr_size[0]);

    int ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

    dims[0] = (hsize_t)(total_records[i]+attrs[0]);
    maxsize = H5S_UNLIMITED;
    H5Dset_extent(dataset1, dims);
    file_dataspace = H5Dget_space(dataset1);
  
    hsize_t offset_w = attrs[0];

    hsize_t offset_p=0;

    for(int j=0;j<bcounts[i];j++)
    {

        hsize_t block_size = blockcounts[i][j][myrank];

        hid_t memdataspace = H5Screate_simple(1,&block_size,&maxsize);
  
        hsize_t offset_t = offset_w;	
	for(int k=0;k<myrank;k++)
		offset_t += blockcounts[i][j][k];
    
	hsize_t blocktotal = 0;
	for(int k=0;k<numprocs;k++)
		blocktotal += blockcounts[i][j][k];

	char *data_p = data_arrays[i].second->data()+offset_p*keyvaluesize;

        hsize_t one = 1;
        ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offset_t,NULL,&one,&block_size);
    
        //ret = H5Dwrite_async(dataset1,s2, memdataspace, file_dataspace,async_dxpl,data_p,es_id);
        ret = H5Dwrite(dataset1,s2, memdataspace, file_dataspace,async_dxpl,data_p);

	offset_p += block_size;
	offset_w += blocktotal;
	H5Sclose(memdataspace);
	//memspaces.push_back(memdataspace);
    }

    attrs[0] += total_records[i];
    int pos = attrs[3];
    pos = 4+pos*4;
    attrs[3] += 1;
    attrs[pos] = minkeys[i];
    pos++;
    attrs[pos] = maxkeys[i];
    pos++;
    attrs[pos] = attrs[3];
    pos++;
    attrs[pos] = total_records[i];

    //ret = H5Awrite_async(attr_id,H5T_NATIVE_UINT64,attrs.data(),es_id);
    ret = H5Awrite(attr_id,H5T_NATIVE_UINT64,attrs.data());

    //ret = H5Aclose_async(attr_id,es_id);
    ret = H5Aclose(attr_id);
    //event_ids.push_back(es_id);
    //H5Dclose_async(dataset1,es_id);
    H5Sclose(file_dataspace);
    H5Dclose(dataset1);
    //H5Gclose_async(grp_id,es_id);
    //H5Fclose_async(fid,es_id);
    H5Tclose(s2);
    H5Tclose(s1);
    H5Pclose(async_fapl);
    H5Pclose(async_dxpl);
    H5Fclose(fid);
    //filespaces.push_back(file_dataspace);
    valid_id.push_back(i);
    }

    int prefix = 0;
    for(int i=0;i<valid_id.size();i++)
    {
        //H5ESwait(event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
	//H5ESclose(event_ids[i]);
	int d = valid_id[i];
        //H5Sclose(filespaces[d]);
	//H5Tclose(type_ids[2*d]);
	//H5Tclose(type_ids[2*d+1]);
	//for(int j=0;j<bcounts[d];j++)
        //H5Sclose(memspaces[prefix+j]);
	std::string filename = "file"+sts[d]+".h5";
	int ps = -1;
	m1.lock();
	auto r = std::find(file_names.begin(),file_names.end(),filename);
	if(r != file_names.end())
	ps = std::distance(file_names.begin(),r);
        m1.unlock();
	if(ps !=-1)
	{
	  (*file_interval)[ps].second.store(maxkeys[d]);
	}

	if(clear_nvme) 
	{
	   int nm_index = nm->buffer_index(sts[d]);
	   if(nm_index==-1)
	   {
		throw std::runtime_error("nvme file does not exist");
	   }
	   else
	   {
	     int tag_p = 100;
	     int keyvaluesize = sizeof(uint64_t)+metadata[d].get_datasize();
	     while(nm->get_buffer(nm_index,tag_p,2)==false);
	     nm->erase_from_nvme(sts[d],data_arrays[d].second->size()/keyvaluesize,bcounts[d]);
	     nm->release_buffer(nm_index);
	   }
	}
	if(data_arrays[d].second != nullptr) delete data_arrays[d].second;
	prefix += bcounts[d];
    }


}

bool read_write_process::pread(std::vector<std::vector<struct io_request*>>&my_requests,int maxstreams)
{


   for(int i=0;i<my_requests.size();i++)
   {
     std::string s = t_args[i].name;
     std::string filename = "file";
     filename += s+".h5";
     bool end = false;
     m1.lock();
     auto r =  std::find(file_names.begin(),file_names.end(),filename);
     if(r == file_names.end()) end = true;
     m1.unlock();

     int file_exists = (end == true) ? 0 : 1;

     int file_exists_t = 0;
     MPI_Allreduce(&file_exists,&file_exists_t,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
     
     if(file_exists_t==numprocs)
     {
    
       hid_t       fid;                                              
       hid_t       acc_tpl;                                         
       hid_t       xfer_plist;                                       
       hid_t       file_dataspace;                                   
       hid_t       mem_dataspace;                                    
       hid_t       dataset1;

       hsize_t attr_space[1];
       attr_space[0] = MAXBLOCKS*4+4;
       const char *attr_name[1];

       std::string filename_r = s+"results"+std::to_string(myrank)+".txt";

       std::ofstream ost(filename_r.c_str(),std::ios::app);

       xfer_plist = H5Pcreate(H5P_DATASET_XFER);
       hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
       H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
       H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_INDEPENDENT);
 
       fid = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, fapl);
       attr_name[0] = "Datasizes";

       std::string dstring = "Data1";
       dataset1 = H5Dopen(fid, dstring.c_str(), H5P_DEFAULT);

       event_metadata em;
       m1.lock();
       auto r1 = write_names.find(s);
       if(r1 != write_names.end())
       {
	  em = (r1->second).second;
       }
       m1.unlock();

       hid_t attr_id = H5Aopen(dataset1,attr_name[0],H5P_DEFAULT);
       std::vector<uint64_t> attrs;
       attrs.resize(attr_space[0]);

       
        int ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());
        int datasize = em.get_datasize(); 
	int keydatasize = sizeof(uint64_t)+datasize;
        hsize_t adims[1];
        adims[0] = datasize;
        hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
        hid_t s2 = H5Tcreate(H5T_COMPOUND,keydatasize);
        H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
        H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);
    
        file_dataspace = H5Dget_space(dataset1);
        int total_k = attrs[0];
        int k_size = attrs[1];
        int data_size = attrs[2];
        int numblocks = attrs[3];

        int pos = 4;

	std::vector<char> *data_buffer = new std::vector<char> ();

	for(int n=0;n<my_requests[i].size();n++)
	{
             uint64_t mints = my_requests[i][n]->mints;
	     uint64_t maxts = my_requests[i][n]->maxts;
   
	     std::vector<int> blockids;

	     for(int j=0;j<numblocks;j++)
             {
	       uint64_t bmin = attrs[pos+j*4+0];
	       uint64_t bmax = attrs[pos+j*4+1];
	      if(mints >= bmin && mints <= bmax ||
	      maxts >= bmin && maxts <= bmax ||
	      mints < bmin && bmax < maxts)
	      {
	         blockids.push_back(j);
	      }
            }

	    int total_records = 0;
	    hsize_t offset = 0;
            for(int j=0;j<blockids.size();j++)
            {
	         hsize_t block_size = attrs[pos+blockids[j]*4+3];
	         total_records += block_size;
            }
            
	    if(total_records>0)
	    {
              data_buffer->resize(total_records*keydatasize);
	      int data_buffp = 0;
	      char *databuff = data_buffer->data();
	      for(int j=0;j<blockids.size();j++)
	      {
		int bid = blockids[j];
		hsize_t block_size = attrs[pos+bid*4+3];
		offset = 0;
		for(int k=0;k<bid;k++)
		  offset += attrs[pos+k*4+3];
                ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,&offset,NULL,&block_size,NULL);
                mem_dataspace = H5Screate_simple(1,&block_size, NULL);
                ret = H5Dread(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist,databuff);
                H5Sclose(mem_dataspace); 
		databuff += block_size*keydatasize;
	      }
            }
	    for(int j=0;j<total_records*keydatasize;j+=keydatasize)
	    {
		uint64_t ts = *(uint64_t*)(&((*data_buffer)[j]));
		if(ts >= my_requests[i][n]->mints && ts <= my_requests[i][n]->maxts)
		{
		   std::string eventstring;
		   eventstring.resize(keydatasize);
		   for(int k=0;k<keydatasize;k++)
			eventstring[k] = (*data_buffer)[j+k];
		  ost << eventstring << std::endl; 
		}
	    }
	    delete my_requests[i][n];
	    my_requests[i][n] = nullptr;
	}
	     
       delete data_buffer;	       
       H5Aclose(attr_id);
       H5Sclose(file_dataspace);
       H5Dclose(dataset1);
       H5Tclose(s2);
       H5Tclose(s1);
       H5Pclose(xfer_plist);
       H5Pclose(fapl);
       H5Fclose(fid);  
       if(ost.is_open()) ost.close();
     }
   }
    return true;
}

std::pair<std::vector<struct event>*,std::vector<char>*> read_write_process::create_data_spaces(std::string &s,hsize_t &poffset,hsize_t &trecords,uint64_t &minkey,uint64_t &maxkey,bool from_nvme,int &nblocks,std::vector<std::vector<int>> &blockcounts)
{

   std::vector<int> num_events_recorded_l,num_events_recorded;
   num_events_recorded_l.resize(numprocs);
   num_events_recorded.resize(numprocs);
   std::fill(num_events_recorded_l.begin(),num_events_recorded_l.end(),0);
   std::fill(num_events_recorded.begin(),num_events_recorded.end(),0);

   auto r = write_names.find(s);

   if(r==write_names.end()) 
   {
	throw std::runtime_error("stream does not exist"); 
	std::pair<std::vector<struct event>*,std::vector<char>*> p;
	p.first = nullptr;
	p.second = nullptr;
	return p;
   }

   event_metadata em = (r->second).second;
   int datasize = em.get_datasize();

   int keyvaluesize = sizeof(uint64_t)+datasize*sizeof(char);
   std::vector<char> *datamem = new std::vector<char> ();

   if(from_nvme)
   {
     int index;
     int tag_p = 100;
     int nm_index = nm->buffer_index(s);
     if(nm_index == -1)
     {
	delete datamem;
	throw std::runtime_error("nvme file does not exist");
	std::pair<std::vector<struct event>*,std::vector<char>*> p;
	p.first = nullptr; p.second = nullptr;
	return p;
     }
     while(nm->get_buffer(nm_index,tag_p,3)==false);
     nm->fetch_buffer(datamem,s,index,tag_p,nblocks,blockcounts);
     //nm->erase_from_nvme(s,data_array->size(),nblocks);
     nm->release_buffer(nm_index);
   }
   else
   {
   
   }

   num_events_recorded_l[myrank] = datamem->size()/keyvaluesize;

   MPI_Allreduce(num_events_recorded_l.data(),num_events_recorded.data(),numprocs,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
   
   hsize_t total_records = 0;
   for(int j=0;j<num_events_recorded.size();j++) total_records += (hsize_t)num_events_recorded[j];

   uint64_t num_records = total_records;
   uint64_t total_size = num_records*datasize+num_records*sizeof(uint64_t);

   int record_size = datasize+sizeof(uint64_t);

   hsize_t offset = 0;
   for(int i=0;i<myrank;i++)
        offset += (hsize_t)num_events_recorded[i];

   poffset = offset;
   trecords = total_records;

   uint64_t min_key=UINT64_MAX, max_key=0;


   if(num_events_recorded[myrank]>0)
   {
     min_key = *(uint64_t*)(&((*datamem)[0]));
     int p = (num_events_recorded[myrank]-1)*keyvaluesize;
     max_key = *(uint64_t*)(&((*datamem)[p]));
   }

   MPI_Allreduce(&min_key,&minkey,1,MPI_UINT64_T,MPI_MIN,MPI_COMM_WORLD);

   MPI_Allreduce(&max_key,&maxkey,1,MPI_UINT64_T,MPI_MAX,MPI_COMM_WORLD);

   if(myrank==0) std::cout <<" total_records = "<<total_records<<" minkey = "<<minkey<<" maxkey = "<<maxkey<<std::endl;

   std::pair<std::vector<struct event>*,std::vector<char>*> p;
   p.first = nullptr;
   p.second = datamem;
   return p;
}

void read_write_process::pwrite_files(std::vector<std::string> &sts,std::vector<hsize_t>&total_records,std::vector<hsize_t> &offsets,std::vector<std::pair<std::vector<struct event>*,std::vector<char>*>> &data_arrays,std::vector<uint64_t>& minkeys,std::vector<uint64_t>&maxkeys,bool clear_nvme,std::vector<int>&bcounts,std::vector<std::vector<std::vector<int>>> &blockcounts)
{


  size_t num;
  hbool_t op_failed = false;

  const char *attr_name[1];
  hsize_t adims[1];
  adims[0] = 0;
  hsize_t attr_size[1];
  attr_size[0] = MAXBLOCKS*4+4;
  hid_t attr_space[1];
  attr_name[0] = "Datasizes";

  std::vector<hid_t> event_ids;
  std::vector<hid_t> dset_ids;
  std::vector<hid_t> gids;
  std::vector<hid_t> fids;
  std::vector<hid_t> filespaces;
  std::vector<hid_t> memspaces;
  std::vector<hid_t> lists;
  std::vector<hid_t> type_ids;
  std::vector<event_metadata> metadata;
  std::vector<int> valid_id;

  for(int i=0;i<sts.size();i++)
  {

        hid_t async_fapl = H5Pcreate(H5P_FILE_ACCESS);
        hid_t async_dxpl = H5Pcreate(H5P_DATASET_XFER);
        H5Pset_fapl_mpio(async_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
        H5Pset_dxpl_mpio(async_dxpl, H5FD_MPIO_COLLECTIVE);
        attr_space[0] = H5Screate_simple(1, attr_size, NULL);
        std::string filename = "file"+sts[i]+".h5";
        hsize_t chunkdims[1];
        chunkdims[0] = total_records[i];
        hsize_t maxdims[1];
        maxdims[0] = (hsize_t)H5S_UNLIMITED;

	int index = -1;
	event_metadata em;
	m1.lock();
	auto r = write_names.find(sts[i]);
	if(r != write_names.end())
	{
	   index = (r->second).first;
	   em = (r->second).second;
	}
	m1.unlock();


	if(index == -1)
	{
	   throw std::runtime_error("data stream buffer does not exist");
	   return;
	}

	if(data_arrays[i].second==nullptr||total_records[i]==0)
	{
	    continue;
	}
	int datasize = em.get_datasize();
	chunkdims[0] = em.get_chunksize();
	metadata.push_back(em);

	hid_t dataset_pl = H5Pcreate(H5P_DATASET_CREATE);

        int keyvaluesize = sizeof(uint64_t)+datasize;	
	adims[0] = (hsize_t)datasize;
	hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
  	hid_t s2 = H5Tcreate(H5T_COMPOUND,keyvaluesize);
  	H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
  	H5Tinsert(s2,"data",HOFFSET(struct event,data),s1);
	//type_ids.push_back(s1);
	//type_ids.push_back(s2);
	
        std::string grp_name = "async_g"+sts[i];
        int ret = H5Pset_chunk(dataset_pl,1,chunkdims);

        hid_t file_dataspace = H5Screate_simple(1,&total_records[i],maxdims);

	filespaces.push_back(file_dataspace);

	hsize_t boffset = 0;
        //hid_t es_id = H5EScreate();
        //hid_t fid = H5Fcreate_async(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl, es_id);
        hid_t fid = H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl);
        //hid_t grp_id = H5Gcreate_async(fid, grp_name.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);
        //hid_t dataset1 = H5Dcreate_async(fid, DATASETNAME1,s2,file_dataspace, H5P_DEFAULT,dataset_pl, H5P_DEFAULT,es_id);
        hid_t dataset1 = H5Dcreate(fid, DATASETNAME1,s2,file_dataspace, H5P_DEFAULT,dataset_pl, H5P_DEFAULT);

	hsize_t dims[1];
	dims[0] = total_records[i];
	H5Dset_extent(dataset1, dims);

	hsize_t block_w = 0;
	
	for(int j=0;j<bcounts[i];j++)
	{

           hsize_t block_count = blockcounts[i][j][myrank];
           hid_t mem_dataspace = H5Screate_simple(1,&block_count, NULL);
           //memspaces.push_back(mem_dataspace);

  	   char *data_c = data_arrays[i].second->data()+block_w*keyvaluesize;	   

           hsize_t boffset_p = boffset;
	   for(int k=0;k<myrank;k++)
		   boffset_p += blockcounts[i][j][k];
	   hsize_t blocktotal = 0;
	   for(int k=0;k<numprocs;k++) blocktotal += blockcounts[i][j][k];

           ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&boffset_p,NULL,&block_count,NULL);
           //ret = H5Dwrite_async(dataset1,s2, mem_dataspace,file_dataspace,async_dxpl,data_c,es_id);
           ret = H5Dwrite(dataset1,s2, mem_dataspace,file_dataspace,async_dxpl,data_c);
	   boffset += blocktotal;
	   block_w += block_count;
	   H5Sclose(mem_dataspace);
	}
        std::vector<uint64_t> attr_data;
	attr_data.resize(attr_size[0]);
        attr_data[0] = total_records[i];
        attr_data[1] = 8;
        attr_data[2] = datasize;
	attr_data[3] = 1;
	
	int pos = 4;
	attr_data[pos] = minkeys[i];
	pos++;
	attr_data[pos] = maxkeys[i];
	pos++;
	attr_data[pos] = 1;
	pos++;
	attr_data[pos] = total_records[i];

	hid_t attr_id[1];
        //attr_id[0] = H5Acreate_async(dataset1, attr_name[0], H5T_NATIVE_UINT64, attr_space[0], H5P_DEFAULT, H5P_DEFAULT,es_id);
        attr_id[0] = H5Acreate(dataset1, attr_name[0], H5T_NATIVE_UINT64, attr_space[0], H5P_DEFAULT, H5P_DEFAULT);

        //ret = H5Awrite_async(attr_id[0], H5T_NATIVE_UINT64, attr_data.data(),es_id);
        ret = H5Awrite(attr_id[0], H5T_NATIVE_UINT64, attr_data.data());

        //ret = H5Aclose_async(attr_id[0],es_id);

	//H5Dclose_async(dataset1,es_id);
	H5Sclose(attr_space[0]);
        H5Aclose(attr_id[0]);
	H5Sclose(file_dataspace);
	H5Dclose(dataset1);
	H5Pclose(dataset_pl);
        //H5Gclose_async(grp_id,es_id);
        //H5Fclose_async(fid,es_id);
	H5Tclose(s2);
	H5Tclose(s1);
	H5Pclose(async_fapl);
	H5Pclose(async_dxpl);
        H5Fclose(fid);
        //event_ids.push_back(es_id);
	valid_id.push_back(i);
    }

    int prefix = 0;
    for(int i=0;i<valid_id.size();i++)
    {
	int d = valid_id[i];
        //H5ESwait(event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
        //H5ESclose(event_ids[i]);
	///H5Sclose(filespaces[d]);
	for(int j=0;j<bcounts[d];j++)
	{
           //H5Sclose(memspaces[prefix+j]);
	}
	//H5Tclose(type_ids[2*d]);
	//H5Tclose(type_ids[2*d+1]);
	std::string filename = "file"+sts[d]+".h5";
	int ps = -1;
	m1.lock();
        file_names.insert(filename);
	ps = std::distance(file_names.begin(),std::find(file_names.begin(),file_names.end(),filename));
	m1.unlock();
	if(ps!=-1)
	{
	  (*file_interval)[ps].first.store(minkeys[d]);
	  (*file_interval)[ps].second.store(maxkeys[d]);
	}
	if(clear_nvme) 
	{
	   int keyvaluesize = sizeof(uint64_t)+metadata[d].get_datasize();
	   int nm_index = nm->buffer_index(sts[d]);
	   if(nm_index == -1)
	   {
		throw std::runtime_error("nvme file does not exist");
	   }
	   else
	   {
	     int tag_p = 100;
	     while(nm->get_buffer(nm_index,tag_p,2)==false);
	     nm->erase_from_nvme(sts[d],data_arrays[d].second->size()/keyvaluesize,bcounts[d]);
	     nm->release_buffer(nm_index);
	   }
	}
	if(data_arrays[d].second != nullptr) delete data_arrays[d].second;
	prefix += bcounts[d];
    }
   
}

void read_write_process::pwrite(std::vector<std::string>& sts,std::vector<hsize_t>& total_records,std::vector<hsize_t>& offsets,std::vector<std::pair<std::vector<struct event>*,std::vector<char>*>>& data_arrays,std::vector<uint64_t>&minkeys,std::vector<uint64_t>&maxkeys,bool clear_nvme,std::vector<int> &bcounts,std::vector<std::vector<std::vector<int>>> &blockcounts)
{

   std::vector<std::string> sts_n, sts_e;
   std::vector<hsize_t> trec_n, trec_e;
   std::vector<hsize_t> off_n, off_e;
   std::vector<std::pair<std::vector<struct event>*,std::vector<char>*>> darray_n, darray_e;
   std::vector<uint64_t> minkeys_n, minkeys_e;
   std::vector<uint64_t> maxkeys_n, maxkeys_e;

   for(int i=0;i<sts.size();i++)
   {
	std::string fname = "file"+sts[i]+".h5";
        auto r = std::find(file_names.begin(),file_names.end(),fname);

        if(r == file_names.end())
        {
	   sts_n.push_back(sts[i]);
	   trec_n.push_back(total_records[i]);
	   off_n.push_back(offsets[i]);
	   darray_n.push_back(data_arrays[i]);
	   minkeys_n.push_back(minkeys[i]);
	   maxkeys_n.push_back(maxkeys[i]);
        }
	else
	{
	   sts_e.push_back(sts[i]);	
	   trec_e.push_back(total_records[i]);
	   off_e.push_back(offsets[i]);
	   darray_e.push_back(data_arrays[i]);
   	   minkeys_e.push_back(minkeys[i]);
	   maxkeys_e.push_back(maxkeys[i]);	   
	}
   }

   try
   {
      pwrite_files(sts_n,trec_n,off_n,darray_n,minkeys_n,maxkeys_n,clear_nvme,bcounts,blockcounts);
      pwrite_extend_files(sts_e,trec_e,off_e,darray_e,minkeys_e,maxkeys_e,clear_nvme,bcounts,blockcounts);
   }
   catch(const std::exception &except)
   {
	std::cout <<except.what()<<std::endl;
	exit(-1);
   }

}

int read_write_process::endsessioncount(int tag_p)
{
     int send_v = end_of_session.load();
     std::vector<int> recv_v(numprocs);
     std::fill(recv_v.begin(),recv_v.end(),0);
     MPI_Request *reqs = new MPI_Request[2*numprocs];

     int nreq = 0;
     for(int i=0;i<numprocs;i++)
     {
	MPI_Isend(&send_v,1,MPI_INT,i,tag_p,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
	MPI_Irecv(&recv_v[i],1,MPI_INT,i,tag_p,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
     }

     MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

    std::free(reqs);     

    int count = 0;
    for(int i=0;i<numprocs;i++) count += recv_v[i];
    return count;

}

void read_write_process::data_stream(struct thread_arg_w *t)
{

   auto t1 = std::chrono::high_resolution_clock::now();
   bool b = true; 

   int numrounds = 0;

   while(true)
   {
      int nprocs = endsessioncount(t->tid);
      t1 = std::chrono::high_resolution_clock::now();
      if(nprocs==numprocs) 
      {
	  t->endsession = true;
	  break;
      }

      if(numrounds == numloops[t->tid]) 
      {
	struct io_request *r = new struct io_request();
        r->name = t->name;
        r->from_nvme = true;
	r->tid = t->tid;
	r->read_op = false;
        io_queue_async->push(r);

        w_reqs_pending[t->tid].store(1);
        while(w_reqs_pending[t->tid].load()!=0);
	numrounds = 0;
      }

      if(myrank==0)
      for(;;)
      {
        auto t2 = std::chrono::high_resolution_clock::now();
        if(std::chrono::duration<double>(t2-t1).count() > loopticks[t->tid] && b) 
        {
	   b = false;
	   break;
        }
      }

      MPI_Request *reqs = new MPI_Request[2*numprocs];
      int nreq = 0;
      int send_v = 1,recv_v = 0;
      if(myrank==0)
      {
	for(int i=0;i<numprocs;i++)
	{
	  MPI_Isend(&send_v,1,MPI_INT,i,t->tid,MPI_COMM_WORLD,&reqs[nreq]);
	  nreq++;
	}
      }

      MPI_Irecv(&recv_v,1,MPI_INT,0,t->tid,MPI_COMM_WORLD,&reqs[nreq]);
      nreq++;

      MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

      delete reqs;

      if(enable_stream[t->tid].load()==1)
      {
        try
        {
	  sort_events(t->name);
	  numrounds++;
        }
        catch(const std::exception &except)
        {
	  std::cout <<except.what()<<std::endl;
	  exit(-1);
        }
      }
      b = true;
   }

   if(enable_stream[t->tid].load()==1)
   {
     try
     {
       sort_events(t->name);
     }
     catch(const std::exception &except)
     {
	std::cout <<except.what()<<std::endl;
	exit(-1);
     }
     struct io_request *r = new struct io_request();
     r->name = t->name;
     r->from_nvme = true;
     r->tid = t->tid;
     r->read_op = false;
     io_queue_async->push(r);

     w_reqs_pending[t->tid].store(1);
     while(w_reqs_pending[t->tid].load()!=0);
     numrounds = 0;

   }
   end_of_stream_session[t->tid].store(1);
}

void read_write_process::io_polling(struct thread_arg_w *t)
{
    std::vector<std::string> snames;
    std::vector<std::pair<std::vector<struct event>*,std::vector<char>*>> data;
    std::vector<hsize_t> total_records, offsets,numrecords;
    std::vector<uint64_t> minkeys, maxkeys;
    std::vector<int> bcounts;
    std::vector<std::vector<std::vector<int>>> blockcounts; 


    bool clear_nvme = false;

   while(true)
   {

     std::atomic_thread_fence(std::memory_order_seq_cst);

     while(cstream.load()==0 && end_of_session.load()==0);

     int end_io = end_of_session.load();

     for(int i=0;i<cstream.load();i++)
     {
	if(end_of_stream_session[i].load()==0) end_io = 0;
     }

     int end_sessions = 0;
     if(end_io == 1 && qe_ended.load()==1) end_sessions = 1; 

     if(!io_queue_async->empty()) end_sessions = 0;

     int empty_all = 0;

     MPI_Allreduce(&end_sessions,&empty_all,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

     if(empty_all == numprocs) 
     {
	     break;
     }

     int valid_stream = cstream.load();
     int active_valid_stream = 0;
     MPI_Allreduce(&valid_stream,&active_valid_stream,1,MPI_INT,MPI_MIN,MPI_COMM_WORLD);

     if(active_valid_stream > 0)
     {
     std::vector<int> pending_streams(active_valid_stream);
     for(int i=0;i<active_valid_stream;i++)
	   pending_streams[i]=w_reqs_pending[i].load();

     std::vector<int> pending_read_streams(active_valid_stream);
     for(int i=0;i<active_valid_stream;i++)
	  pending_read_streams[i] = r_reqs_pending[i].load();

     std::vector<int> active_reqs(active_valid_stream);
     std::fill(active_reqs.begin(),active_reqs.end(),0);
     MPI_Allreduce(pending_streams.data(),active_reqs.data(),active_valid_stream,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

     std::vector<std::vector<io_request*>> read_reqs;
     read_reqs.resize(active_valid_stream);
    
     std::vector<io_request*> write_reqs;
     write_reqs.resize(active_valid_stream);

     for(int i=0;i<active_valid_stream;i++) write_reqs[i] = nullptr;

     int num_req = 0;
     for(int i=0;i<active_valid_stream;i++)
	  if(active_reqs[i]==numprocs) num_req++;

     snames.clear(); data.clear(); total_records.clear(); offsets.clear(); numrecords.clear();
     minkeys.clear(); maxkeys.clear();
     bcounts.clear(); blockcounts.clear();

     std::vector<io_request*> inactive_reqs;

     if(num_req > 0)
     {
	  std::pair<uint64_t,uint64_t> p = CM->SynchronizeClocks();
	  CM->ComputeErrorInterval(p.first,p.second);
     }

     while(!io_queue_async->empty())
     {
         struct io_request *r=nullptr;

         io_queue_async->pop(r);

         if(r != nullptr)
         {
           if(r->tid < active_valid_stream && active_reqs[r->tid]==numprocs && r->read_op==false)
	   {
	       write_reqs[r->tid] = r;
	   }
	   else if(r->tid < active_valid_stream && r->read_op==true)
	   {
		read_reqs[r->tid].push_back(r);
	   }
	   else inactive_reqs.push_back(r);
	 }
       }
        
       for(int i=0;i<write_reqs.size();i++)
       {
	  io_request *r = write_reqs[i];

	  if(r != nullptr)
	  {
            hsize_t trecords, offset, numrecords;
	    uint64_t min_key,max_key;
	    int nblocks;
	    std::vector<std::vector<int>> blockc;
	    std::pair<std::vector<struct event>*,std::vector<char>*> data_rp;
	    try
	    {
	     data_rp = create_data_spaces(r->name,offset,trecords,min_key,max_key,true,nblocks,blockc);
	    }
	    catch(const std::exception &except)
	    {
		std::cout <<except.what()<<std::endl;
		exit(-1);
	    }
            snames.push_back(r->name);
            total_records.push_back(trecords);
            offsets.push_back(offset);
	    minkeys.push_back(min_key);
	    maxkeys.push_back(max_key);
            data.push_back(data_rp);
	    bcounts.push_back(nblocks);
 	    blockcounts.push_back(blockc);
	    clear_nvme = true;
	    delete r;
	    w_reqs_pending[r->tid].store(0);
	  }
      }

      for(int i=0;i<inactive_reqs.size();i++)
	      io_queue_async->push(inactive_reqs[i]);

      pwrite(snames,total_records,offsets,data,minkeys,maxkeys,clear_nvme,bcounts,blockcounts);
      
      snames.clear();
      data.clear();
      total_records.clear();
      offsets.clear();
      numrecords.clear();
      bcounts.clear();
      blockcounts.clear();

     pread(read_reqs,active_valid_stream);

      for(int i=0;i<active_valid_stream;i++)
           for(int j=0;j<read_reqs[i].size();j++)
                 if(read_reqs[i][j] != nullptr)
                   io_queue_async->push(read_reqs[i][j]);

     }

  }

  session_ended.store(1); 

}

std::string read_write_process::GetEvent(std::string &s,uint64_t &ts,int s_id)
{
 if(ipaddrs[s_id].compare(myipaddr)==0)
 {
    tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[s_id]);
    tl::remote_procedure rp = thallium_shm_client->define("EmulatorGetEvent");
    return rp.on(ep)(s,ts);
 }
 else
 {
   tl::remote_procedure rp = thallium_client->define("EmulatorGetEvent");
   return rp.on(serveraddrs[s_id])(s,ts);
 }
}

std::string read_write_process::GetNVMEEvent(std::string &s,uint64_t &ts,int s_id)
{

   if(ipaddrs[s_id].compare(myipaddr)==0)
   {
	tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[s_id]);
	tl::remote_procedure rp = thallium_shm_client->define("EmulatorGetNVMEEvent");
	return rp.on(ep)(s,ts);
   }
   else
   {
	tl::remote_procedure rp = thallium_client->define("EmulatorGetNVMEEvent");
	return rp.on(serveraddrs[s_id])(s,ts);
   }
}

std::string read_write_process::FindEvent(std::string &s,uint64_t &ts)
{
   std::string eventstring;
   int pid = get_event_proc(s,ts);
   if(pid != -1)
   {

	 return GetEvent(s,ts,pid);
   }
   else
   {
	pid = get_nvme_proc(s,ts);
	if(pid != -1) 
	{
          return GetNVMEEvent(s,ts,pid);
	}
   }
   return eventstring;
}

std::string read_write_process::FindEventFile(std::string &s,uint64_t &ts)
{
   std::string eventstring;
   int index = -1;
   m1.lock();
   auto r1 = write_names.find(s);
   if(r1 != write_names.end()) index = (r1->second).first;
   m1.unlock();

   if(index == -1) return eventstring;

   int pid1 = get_event_proc(s,ts);
   if(pid1 != -1)
   {
	eventstring = GetEvent(s,ts,pid1);
   }
   if(eventstring.length()==0)
   {
     int pid2 = get_nvme_proc(s,ts);
     if(pid2 != -1)
     {
	eventstring = GetNVMEEvent(s,ts,pid2);
     }
   }
   
   if(eventstring.length()==0)
   {
	struct io_request *r = new struct io_request();
        r->name = s;
        r->from_nvme = false;
	r->read_op = true;
        r->tid = index;
	r->mints = ts;
	r->maxts = ts;
        io_queue_async->push(r);
   }
   return eventstring;
}
