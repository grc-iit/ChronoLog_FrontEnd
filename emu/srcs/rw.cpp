#include "rw.h"

#define DATASETNAME1 "Data1"

typedef int DATATYPE;

void read_write_process::create_events(int num_events,std::string &s,double arrival_rate)
{
    int datasize = 0;
    m1.lock();
    auto r = write_names.find(s);
    int index = (r->second).first;
    event_metadata em = (r->second).second;
    m1.unlock();
    datasize = em.get_datasize();
   
    atomic_buffer *ab = dm->get_atomic_buffer(index);

    boost::shared_lock<boost::shared_mutex> lk(ab->m); 

    for(int i=0;i<num_events;i++)
    {
	event e;
	uint64_t ts = CM->Timestamp();

	e.ts = ts;
	      
	dm->add_event(e,index);
	usleep(20000);
    }

}

void read_write_process::clear_write_events(int index,uint64_t& min_k,uint64_t& max_k)
{
   if(index != -1)
   {
	dm->clear_write_buffer_no_lock(index);
	uint64_t min_n = max_k+1;
	uint64_t max_n = UINT64_MAX;
	dm->set_valid_range(index,min_n,max_n);
   }

}

void read_write_process::clear_read_events(std::string &s)
{
   int index = -1;
   m2.lock();
   auto r = read_names.find(s);
   if(r != read_names.end()) index = (r->second).first;
   m2.unlock();

   if(index==-1) return;

   boost::upgrade_lock<boost::shared_mutex> lk(readevents[index]->m);
   m2.lock();

   readevents[index]->buffer->clear();
   auto r1 = read_interval.find(s);
   if(r1 != read_interval.end())
   {
	r1->second.first = UINT64_MAX;
	r1->second.second = 0;
   }
   m2.unlock();
}

void read_write_process::pwrite_extend_files(std::vector<std::string>&sts,std::vector<hsize_t>&total_records,std::vector<hsize_t>&offsets,std::vector<std::vector<struct event>*>&data_arrays,std::vector<uint64_t>&minkeys,std::vector<uint64_t>&maxkeys)
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

    hid_t async_fapl = H5Pcreate(H5P_FILE_ACCESS);
    hid_t async_dxpl = H5Pcreate(H5P_DATASET_XFER);

    H5Pset_fapl_mpio(async_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    H5Pset_dxpl_mpio(async_dxpl, H5FD_MPIO_COLLECTIVE);

    size_t num;
    hbool_t op_failed = false;

    adims[0] = (hsize_t)VALUESIZE;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event));
    H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);

    hsize_t attr_size[1];
    attr_size[0] = 100*4+4;
    hid_t attr_space[1];
    attr_name[0] = "Datasizes";
    attr_space[0] = H5Screate_simple(1, attr_size, NULL);

    std::vector<hid_t> event_ids;
    std::vector<hid_t> filespaces;
    std::vector<hid_t> memspaces;
    std::vector<hid_t> gids;
    std::vector<hid_t> fids;
    std::vector<hid_t> dset_ids;
    
    for(int i=0;i<sts.size();i++)
    {
   
    hid_t es_id = H5EScreate();

    std::string filename = "file"+sts[i]+".h5";
    fid = H5Fopen_async(filename.c_str(), H5F_ACC_RDWR, async_fapl,es_id);

    hid_t gapl = H5Pcreate(H5P_GROUP_ACCESS);
    std::string grp_name = "async_g"+sts[i];
    hid_t grp_id = H5Gopen_async(fid, grp_name.c_str(),gapl, es_id); 
    
    dataset1 = H5Dopen_async(fid, DATASETNAME1, H5P_DEFAULT,es_id);

    hid_t attr_id = H5Aopen_async(dataset1,attr_name[0],H5P_DEFAULT,es_id);
    std::vector<uint64_t> attrs;
    attrs.resize(attr_size[0]);

    int ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

    dims[0] = (hsize_t)(total_records[i]+attrs[0]);
    hsize_t block_size = data_arrays[i]->size();

    maxsize = H5S_UNLIMITED;
    H5Dset_extent(dataset1, dims);
    
    file_dataspace = H5Dget_space(dataset1);

    mem_dataspace = H5Screate_simple(1,&block_size,&maxsize);
   
    
    hsize_t one = 1;
    offsets[i] += attrs[0];
    ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offsets[i],NULL,&one,&block_size);
    
    ret = H5Dwrite_async(dataset1,s2, mem_dataspace, file_dataspace,async_dxpl,data_arrays[i]->data(),es_id);

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

    ret = H5Awrite_async(attr_id,H5T_NATIVE_UINT64,attrs.data(),es_id);

    ret = H5Aclose_async(attr_id,es_id);
    /*
    dset_ids.push_back(dataset1);
    fids.push_back(fid);
    gids.push_back(grp_id);*/
    event_ids.push_back(es_id);
    H5Dclose_async(dataset1,es_id);
    H5Gclose_async(grp_id,es_id);
    H5Pclose(gapl);
    H5Fclose_async(fid,es_id);
    filespaces.push_back(file_dataspace);
    memspaces.push_back(mem_dataspace);
    }

    for(int i=0;i<event_ids.size();i++)
    {
        H5ESwait(event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
	H5ESclose(event_ids[i]);
        H5Sclose(filespaces[i]);
        H5Sclose(memspaces[i]);
	nm->erase_from_nvme(sts[i],data_arrays[i]->size());
	delete data_arrays[i];
    }

    H5Sclose(attr_space[0]);
    H5Pclose(async_fapl);
    H5Pclose(async_dxpl);

}

void read_write_process::preadfileattr(const char *filename)
{
    hid_t       fid;
    hid_t       acc_tpl;
    hid_t       xfer_plist;
    hid_t       file_dataspace;
    hid_t       mem_dataspace;

    hsize_t attr_space[1];
    attr_space[0] = 100*4+4;
    const char *attr_name[1];

    std::string filestring(filename);

    m1.lock();

    auto r1 = std::find(file_names.begin(),file_names.end(),filestring);

    m1.unlock();

    if(r1==file_names.end()) return;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    //H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fopen(filename, H5F_ACC_RDONLY, fapl);

    hid_t ret = H5Pclose(fapl);

    attr_name[0] = "Datasizes";

    hid_t dataset1 = H5Dopen2(fid, DATASETNAME1, H5P_DEFAULT);
    file_dataspace = H5Dget_space(dataset1);
    hsize_t nchunks = 0;
    H5Dget_num_chunks(dataset1,file_dataspace, &nchunks);

    hid_t attr_id = H5Aopen(dataset1,attr_name[0],H5P_DEFAULT);
    std::vector<uint64_t> attrs;
    attrs.resize(attr_space[0]);

    std::string fname(filename);
    auto r = file_minmax.find(fname);
    ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

    std::cout <<" rank = "<<myrank<<" num_records = "<<attrs[0]<<std::endl;

    /*if(r==file_minmax.end())
    {
	std::pair<std::string,std::pair<uint64_t,uint64_t>> p;
	p.first = fname;
	p.second.first = attrs[3];
	p.second.second = attrs[4];
	file_minmax.insert(p);
    }
    else
    {
	r->second.first = attrs[3];
	r->second.second = attrs[4];
    }*/

    H5Sclose(file_dataspace);
    ret = H5Aclose(attr_id);
    ret = H5Dclose(dataset1);
    H5Fclose(fid);

}
void read_write_process::preaddata(const char *filename,std::string &name)
{

    hid_t       fid;                                              
    hid_t       acc_tpl;                                         
    hid_t       xfer_plist;                                       
    hid_t       file_dataspace;                                   
    hid_t       mem_dataspace;                                    
    hid_t       dataset1, dataset2, dataset5, dataset6, dataset7;

    hsize_t attr_space[1];
    attr_space[0] = 100*4+4;
    const char *attr_name[1];
    size_t   num_points;    
    int      i, j, k;

    m1.lock();
    std::string filenamestring(filename);
    auto f = std::find(file_names.begin(),file_names.end(),filenamestring);
    m1.unlock();

    if(f == file_names.end()) return;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    fid = H5Fopen(filename, H5F_ACC_RDONLY, fapl);

    hid_t ret = H5Pclose(fapl);


    attr_name[0] = "Datasizes";

    dataset1 = H5Dopen2(fid, DATASETNAME1, H5P_DEFAULT);

    file_dataspace = H5Dget_space(dataset1);
    hid_t attr_id = H5Aopen(dataset1,attr_name[0],H5P_DEFAULT);
    std::vector<uint64_t> attrs;
    attrs.resize(attr_space[0]);

    ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

    int total_k = attrs[0];
    int k_size = attrs[1];
    int data_size = attrs[2];
    int numblocks = attrs[3];


    hsize_t offset = 0;

    int block_id = 0;
    int pos = 4;

    pos = 4;

    offset = 0;
    for(int i=0;i<block_id;i++)
	offset += attrs[pos+block_id*4+3];

    hsize_t block_size = attrs[pos+block_id*4+3];

   int size_per_proc = block_size/numprocs;
   int rem = block_size%numprocs;

   for(int i=0;i<myrank;i++)
   {
        int size_p;
	if(i < rem) size_p = size_per_proc+1;
	else size_p = size_per_proc;
	offset += size_p;
   } 
   
    
   hsize_t blocksize = 0;
   if(myrank < rem) blocksize = size_per_proc+1;
   else blocksize = size_per_proc;
     
    event_metadata em1;
    em1.set_numattrs(625);
    for(int i=0;i<625;i++)
    {
           std::string a="attr"+std::to_string(i);
           int vsize = sizeof(double);
           bool is_signed = false;
           bool is_big_endian = true;
           em1.add_attr(a,vsize,is_signed,is_big_endian);
    }
        
    create_read_buffer(name,em1);

    m2.lock();
    auto r = read_names.find(name);
    int index = (r->second).first;
    event_metadata em = (r->second).second;
    m2.unlock();

    int datasize = VALUESIZE;

    boost::upgrade_lock<boost::shared_mutex> lk(readevents[index]->m);

    readevents[index]->buffer->resize(blocksize);

    hsize_t adims[1];
    adims[0] = VALUESIZE;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event));
    H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);
 
    file_dataspace = H5Dget_space(dataset1);
    ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,&offset,NULL,&blocksize,NULL);
    mem_dataspace = H5Screate_simple(1,&blocksize, NULL);
    xfer_plist = H5Pcreate(H5P_DATASET_XFER);
    //ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_INDEPENDENT);
    ret = H5Dread(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist,readevents[index]->buffer->data());
  
    
    uint64_t minkey = (*readevents[index]->buffer)[0].ts;
    uint64_t maxkey = (*readevents[index]->buffer)[blocksize-1].ts;

    
    m2.lock();

    auto r1 = read_interval.find(name);
    if(r1 == read_interval.end())
    {
	std::pair<std::string,std::pair<uint64_t,uint64_t>> p2;
	p2.first.assign(name);
	p2.second.first = minkey;
	p2.second.second = maxkey;
	read_interval.insert(p2);
    } 
    else 
    {
	(r1->second).first = minkey; (r1->second).second = maxkey;
    }
    m2.unlock();


    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);

    H5Aclose(attr_id);

    H5Dclose(dataset1);

    H5Fclose(fid);  

}

std::vector<struct event>* read_write_process::create_data_spaces(std::string &s,hsize_t &poffset,hsize_t &trecords,uint64_t &minkey,uint64_t &maxkey,bool from_nvme)
{

   std::vector<int> num_events_recorded_l,num_events_recorded;
   num_events_recorded_l.resize(numprocs);
   num_events_recorded.resize(numprocs);
   std::fill(num_events_recorded_l.begin(),num_events_recorded_l.end(),0);
   std::fill(num_events_recorded.begin(),num_events_recorded.end(),0);

   std::vector<struct event> *data_array = nullptr;

   if(from_nvme)
   {
     int index;
     data_array = nm->fetch_buffer(s,index);
   }
   else
   {
	m1.lock();
	auto r = write_names.find(s);
	int index = (r->second).first;
	m1.unlock();

	myevents[index]->m.lock();
	data_array = new std::vector<struct event> ();
	data_array->assign(myevents[index]->buffer->begin(),myevents[index]->buffer->end());
	myevents[index]->buffer->clear();
	myevents[index]->m.unlock();
   }

   num_events_recorded_l[myrank] = data_array->size();

   MPI_Allreduce(num_events_recorded_l.data(),num_events_recorded.data(),numprocs,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
   
   hsize_t total_records = 0;
   for(int j=0;j<num_events_recorded.size();j++) total_records += (hsize_t)num_events_recorded[j];

   uint64_t num_records = total_records;
   uint64_t total_size = num_records*VALUESIZE+num_records*sizeof(uint64_t);

   int record_size = VALUESIZE+sizeof(uint64_t);

   hsize_t offset = 0;
   for(int i=0;i<myrank;i++)
        offset += (hsize_t)num_events_recorded[i];

   poffset = offset;
   trecords = total_records;

   uint64_t min_key, max_key;

   if(myrank==0) min_key = (*data_array)[0].ts;
   if(myrank==numprocs-1) max_key = (*data_array)[data_array->size()-1].ts;

   MPI_Bcast(&min_key,1,MPI_UINT64_T,0,MPI_COMM_WORLD);

   MPI_Bcast(&max_key,1,MPI_UINT64_T,numprocs-1,MPI_COMM_WORLD); 

   minkey = min_key;
   maxkey = max_key;

   if(myrank==0) std::cout <<" total_records = "<<total_records<<" minkey = "<<minkey<<" maxkey = "<<maxkey<<std::endl;

   return data_array;
}

void read_write_process::pwrite_files(std::vector<std::string> &sts,std::vector<hsize_t>&total_records,std::vector<hsize_t> &offsets,std::vector<std::vector<struct event>*> &data_arrays,std::vector<uint64_t>& minkeys,std::vector<uint64_t>&maxkeys)
{

  hid_t async_fapl = H5Pcreate(H5P_FILE_ACCESS);
  hid_t async_dxpl = H5Pcreate(H5P_DATASET_XFER);

  H5Pset_fapl_mpio(async_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
  H5Pset_dxpl_mpio(async_dxpl, H5FD_MPIO_COLLECTIVE);

  size_t num;
  hbool_t op_failed = false;

  const char *attr_name[1];
  hsize_t adims[1];
  adims[0] = (hsize_t)VALUESIZE;
  hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
  hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event));
  H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
  H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);

  hsize_t attr_size[1];
  attr_size[0] = 100*4+4;
  hid_t attr_space[1];
  attr_name[0] = "Datasizes";
  attr_space[0] = H5Screate_simple(1, attr_size, NULL);

  std::vector<hid_t> event_ids;
  std::vector<hid_t> dset_ids;
  std::vector<hid_t> gids;
  std::vector<hid_t> fids;
  std::vector<hid_t> filespaces;
  std::vector<hid_t> memspaces;
  std::vector<hid_t> lists;

  for(int i=0;i<sts.size();i++)
  {

        std::string filename = "file"+sts[i]+".h5";
        hsize_t chunkdims[1];
        chunkdims[0] = total_records[i];
        hsize_t maxdims[1];
        maxdims[0] = (hsize_t)H5S_UNLIMITED;

	hid_t dataset_pl = H5Pcreate(H5P_DATASET_CREATE);

        std::string grp_name = "async_g"+sts[i];
        int ret = H5Pset_chunk(dataset_pl,1,chunkdims);

        hid_t file_dataspace = H5Screate_simple(1,&total_records[i],maxdims);

        hsize_t block_count = data_arrays[i]->size();
        hid_t mem_dataspace = H5Screate_simple(1,&block_count, NULL);

	filespaces.push_back(file_dataspace);
        memspaces.push_back(mem_dataspace);

        ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offsets[i],NULL,&block_count,NULL);

        hid_t es_id = H5EScreate();
        hid_t fid = H5Fcreate_async(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl, es_id);
        hid_t grp_id = H5Gcreate_async(fid, grp_name.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);

        hid_t dataset1 = H5Dcreate_async(fid, DATASETNAME1,s2,file_dataspace, H5P_DEFAULT,dataset_pl, H5P_DEFAULT,es_id);

        ret = H5Dwrite_async(dataset1,s2, mem_dataspace,file_dataspace,async_dxpl,data_arrays[i]->data(),es_id);

        std::vector<uint64_t> attr_data;
	attr_data.resize(attr_size[0]);
        attr_data[0] = total_records[i];
        attr_data[1] = 8;
        attr_data[2] = VALUESIZE;
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
        attr_id[0] = H5Acreate_async(dataset1, attr_name[0], H5T_NATIVE_UINT64, attr_space[0], H5P_DEFAULT, H5P_DEFAULT,es_id);

        ret = H5Awrite_async(attr_id[0], H5T_NATIVE_UINT64, attr_data.data(),es_id);

        ret = H5Aclose_async(attr_id[0],es_id);

	H5Dclose_async(dataset1,es_id);
	H5Pclose(dataset_pl);
        H5Gclose_async(grp_id,es_id);
        H5Fclose_async(fid,es_id);
        event_ids.push_back(es_id);
	/*lists.push_back(dataset_pl);*/
    }

    for(int i=0;i<event_ids.size();i++)
    {
        //int err = H5Dclose_async(dset_ids[i],event_ids[i]);
        //H5Gclose_async(gids[i],event_ids[i]);
        //H5Fclose_async(fids[i],event_ids[i]);
        H5ESwait(event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
        H5ESclose(event_ids[i]);
        //H5Pclose(lists[i]);
	H5Sclose(filespaces[i]);
        H5Sclose(memspaces[i]);
	nm->erase_from_nvme(sts[i],data_arrays[i]->size());
	delete data_arrays[i];
	std::string filename = "file"+sts[i]+".h5";
	m1.lock();
	file_names.insert(filename);
	m1.unlock();
    }

    H5Sclose(attr_space[0]);
    H5Pclose(async_fapl);
    H5Pclose(async_dxpl);

}

void read_write_process::pwrite(std::vector<std::string>& sts,std::vector<hsize_t>& total_records,std::vector<hsize_t>& offsets,std::vector<std::vector<struct event>*>& data_arrays,std::vector<uint64_t>&minkeys,std::vector<uint64_t>&maxkeys)
{

   std::vector<std::string> sts_n, sts_e;
   std::vector<hsize_t> trec_n, trec_e;
   std::vector<hsize_t> off_n, off_e;
   std::vector<std::vector<struct event>*> darray_n, darray_e;
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

   pwrite_files(sts_n,trec_n,off_n,darray_n,minkeys_n,maxkeys_n);
   pwrite_extend_files(sts_e,trec_e,off_e,darray_e,minkeys_e,maxkeys_e);

}

void read_write_process::data_stream(struct thread_arg_w *t)
{
   int niter = 4;
   for(int i=0;i<niter;i++)
   {
        create_events(t->num_events,t->name,1);
        sort_events(t->name);
   }

}

void read_write_process::io_polling_seq(struct thread_arg_w *t)
{

}

void read_write_process::io_polling(struct thread_arg_w *t)
{
    std::vector<std::string> snames;
    std::vector<std::vector<struct event>*> data;
    std::vector<hsize_t> total_records, offsets,numrecords;
    std::vector<uint64_t> minkeys, maxkeys;

   while(true)
   {

     std::atomic_thread_fence(std::memory_order_seq_cst);

     while(num_streams.load()==0 && end_of_session.load()==0 && io_queue_sync->empty());

     int sync_async[3];
     int sync_empty = io_queue_sync->empty() ? 0 : 1;
     sync_async[0] = sync_empty;
     int async_empty = num_streams.load() == 0 ? 0 : 1;
     sync_async[1] = async_empty;
     int end_sessions = end_of_session.load()==0 ? 0 : 1;
     sync_async[2] = end_sessions;

     int sync_empty_all[3];

     MPI_Allreduce(&sync_async,&sync_empty_all,3,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

     if(sync_empty_all[2]==numprocs) break;

     if(sync_empty_all[0]==numprocs)
     {
     }


     if(sync_empty_all[1]==numprocs)
     {
       snames.clear(); data.clear(); total_records.clear(); offsets.clear(); numrecords.clear();
       minkeys.clear(); maxkeys.clear();

       while(!io_queue_async->empty())
       {
         struct io_request *r=nullptr;

         io_queue_async->pop(r);

         if(r != nullptr)
         {
           if(r->from_nvme)
           {
              hsize_t trecords, offset, numrecords;
	      uint64_t min_key,max_key;
              std::vector<struct event> *data_r = nullptr;
              data_r = create_data_spaces(r->name,offset,trecords,min_key,max_key,true);
              snames.push_back(r->name);
              total_records.push_back(trecords);
              offsets.push_back(offset);
	      minkeys.push_back(min_key);
	      maxkeys.push_back(max_key);
           //t->numrecords.push_back(numrecords);
              data.push_back(data_r);
          }

	  delete r;
         }
      }

      num_streams.store(0);
      std::atomic_thread_fence(std::memory_order_seq_cst);
      pwrite(snames,total_records,offsets,data,minkeys,maxkeys);

      snames.clear();
      data.clear();
      total_records.clear();
      offsets.clear();
      numrecords.clear();
     }

  }
}
