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
    
    auto ab = dm->get_atomic_buffer(index);

    boost::upgrade_lock<boost::shared_mutex> lk(ab->m);

    for(int i=0;i<num_events;i++)
    {
	event e;
	uint64_t ts = CM->Timestamp();

	e.ts = ts;
	      
	dm->add_event(e,index);
	//sleep(ceil(arrival_rate));
    }

}

void read_write_process::clear_events(std::string &s)
{
   m2.lock();
   auto r = read_names.find(s);
   int index = -1;
   if(r!=read_names.end())
   {
	index = (r->second).first;
   }
   m2.unlock();
   
   if(index != -1)
   {
	boost::upgrade_lock<boost::shared_mutex> lk(readevents[index]->m);
	readevents[index]->buffer->clear();
   }
   index = -1;
   m1.lock();
   r = write_names.find(s); 
   uint64_t min_k, max_k;
   if(r != write_names.end())
   {
	index = (r->second).first;
	auto r1 = write_interval.find(s);
	min_k = r1->second.second+1;
	max_k = UINT64_MAX;
	r1->second.first = UINT64_MAX; r1->second.second = 0;
   }
   m1.unlock();
   if(index != -1)
   {
	dm->clear_write_buffer(index);
	dm->set_valid_range(index,min_k,max_k);
   }

}

void read_write_process::pwrite_extend_files_from_nvme(std::vector<std::string>&sts,std::vector<hsize_t>&total_records,std::vector<hsize_t>&offsets,std::vector<std::vector<struct event>*>&data_arrays)
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

    std::string grp_name = "async_g";

    size_t num;
    hbool_t op_failed = false;

    adims[0] = (hsize_t)VALUESIZE;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event));
    H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);

    hsize_t attr_size[1];
    attr_size[0] = 5;
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

    hid_t grp_id = H5Gcreate_async(fid, grp_name.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id); 
    
    dataset1 = H5Dopen_async(fid, DATASETNAME1, H5P_DEFAULT,es_id);

    hid_t attr_id = H5Aopen_async(dataset1,attr_name[0],H5P_DEFAULT,es_id);
    std::vector<uint64_t> attrs;
    attrs.resize(5);

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

    ret = H5Awrite_async(attr_id,H5T_NATIVE_UINT64,attrs.data(),es_id);

    ret = H5Aclose_async(attr_id,es_id);

    dset_ids.push_back(dataset1);
    fids.push_back(fid);
    gids.push_back(grp_id);
    event_ids.push_back(es_id);
    filespaces.push_back(file_dataspace);
    memspaces.push_back(mem_dataspace);

    }

    for(int i=0;i<event_ids.size();i++)
    {
        H5Dclose_async(dset_ids[i],event_ids[i]);
        H5Gclose_async(gids[i],event_ids[i]);
        H5Fclose_async(fids[i],event_ids[i]);
        H5ESwait(event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
        H5Sclose(filespaces[i]);
        H5Sclose(memspaces[i]);
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

    const char *attr_name[1];

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fopen(filename, H5F_ACC_RDONLY, fapl);

    hid_t ret = H5Pclose(fapl);

    attr_name[0] = "DataSizes";

    hid_t dataset1 = H5Dopen2(fid, DATASETNAME1, H5P_DEFAULT);

    hid_t attr_id = H5Aopen(dataset1,attr_name[0],H5P_DEFAULT);
    std::vector<uint64_t> attrs;
    attrs.resize(5);

    std::string fname(filename);
    auto r = file_minmax.find(fname);
    ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

    if(r==file_minmax.end())
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
    }

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

    const char *attr_name[1];
    size_t   num_points;    
    int      i, j, k;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fopen(filename, H5F_ACC_RDONLY, fapl);

    hid_t ret = H5Pclose(fapl);

    attr_name[0] = "DataSizes";

    dataset1 = H5Dopen2(fid, DATASETNAME1, H5P_DEFAULT);

    hid_t attr_id = H5Aopen(dataset1,attr_name[0],H5P_DEFAULT);
    std::vector<uint64_t> attrs;
    attrs.resize(5);

    ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

    int total_k = attrs[0];
    int k_size = attrs[1];
    int data_size = attrs[2];

    int k_per_process = total_k/numprocs;
    int rem = total_k%numprocs;

    hsize_t offset = 0;

    for(int i=0;i<myrank;i++)
    {
	    if(i < rem) offset += k_per_process+1;
	    else offset += k_per_process;
    }

    int num_events = 0;
    if(myrank < rem) num_events = k_per_process+1;
    else num_events = k_per_process;

    hsize_t block_size = (hsize_t)num_events;

    event_metadata em1;
    em1.set_numattrs(5);
    for(int i=0;i<5;i++)
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

    int datasize = em.get_datasize();

    boost::upgrade_lock<boost::shared_mutex> lk(readevents[index]->m);

    readevents[index]->buffer->resize(num_events);

    hsize_t adims[1];
    adims[0] = datasize;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event));
    H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);


    file_dataspace = H5Dget_space(dataset1);
    ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,&offset,NULL,&block_size,NULL);
    mem_dataspace = H5Screate_simple(1,&block_size, NULL);
    xfer_plist = H5Pcreate(H5P_DATASET_XFER);
    ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);
    ret = H5Dread(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist, readevents[index]->buffer->data());
     
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);

    ret = H5Aclose(attr_id);
    ret = H5Dclose(dataset1);

   H5Fclose(fid);  

}


void read_write_process::pwrite(const char *filename,std::string &s)
{
   std::string fname(filename);

   /*
   auto r = std::find(file_names.begin(),file_names.end(),fname);
   if(r == file_names.end())
   {
	pwrite_new(filename,s);
   }
   else pwrite_extend(filename,s);
*/
}


hsize_t read_write_process::create_data_spaces_from_memory(std::string &s,hsize_t &poffset, hsize_t &numrecords)
{

   m1.lock();
   auto r = write_names.find(s);
   int index = (r->second).first;
   event_metadata em = (r->second).second;
   m1.unlock();
   int datasize = em.get_datasize();

   std::vector<int> num_events_recorded_l,num_events_recorded;
   num_events_recorded_l.resize(numprocs);
   num_events_recorded.resize(numprocs);
   std::fill(num_events_recorded_l.begin(),num_events_recorded_l.end(),0);
   std::fill(num_events_recorded.begin(),num_events_recorded.end(),0);

   myevents[index]->m.lock();

   num_events_recorded_l[myrank] = myevents[index]->buffer->size();

   MPI_Request *reqs = (MPI_Request*)malloc(3*numprocs*sizeof(MPI_Request));
   MPI_Status *stats = (MPI_Status*)malloc(3*numprocs*sizeof(MPI_Status));

   int nreq = 0;
   for(int i=0;i<numprocs;i++)
   {
       MPI_Isend(&num_events_recorded_l[myrank],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
       nreq++;
   }

   for(int i=0;i<numprocs;i++)
   {
       MPI_Irecv(&num_events_recorded[i],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
       nreq++;
   }
   MPI_Waitall(nreq,reqs,stats);

   hsize_t total_records = 0;
   for(int j=0;j<num_events_recorded.size();j++) total_records += (hsize_t)num_events_recorded[j];
   if(myrank==0)
   {
       std::cout <<" datasize = "<<VALUESIZE<<" total_records = "<<total_records<<std::endl;
   }

   uint64_t num_records = total_records;
   uint64_t total_size = num_records*datasize+num_records*sizeof(uint64_t);

   int record_size = datasize+sizeof(uint64_t);

   hsize_t offset = 0;
   for(int i=0;i<myrank;i++)
        offset += (hsize_t)num_events_recorded[i];
   poffset = offset;
   numrecords = (hsize_t)num_records;
   free(reqs); free(stats);
   return total_records;
}

std::vector<struct event>* read_write_process::create_data_spaces_from_nvme(std::string &s,hsize_t &poffset,hsize_t &trecords)
{

   std::vector<int> num_events_recorded_l,num_events_recorded;
   num_events_recorded_l.resize(numprocs);
   num_events_recorded.resize(numprocs);
   std::fill(num_events_recorded_l.begin(),num_events_recorded_l.end(),0);
   std::fill(num_events_recorded.begin(),num_events_recorded.end(),0);

   int index;
   std::vector<struct event> *data_array = nm->fetch_buffer(s,index);

   num_events_recorded_l[myrank] = data_array->size();

   MPI_Request *reqs = (MPI_Request*)malloc(3*numprocs*sizeof(MPI_Request));
   MPI_Status *stats = (MPI_Status*)malloc(3*numprocs*sizeof(MPI_Status));

   int nreq = 0;
   for(int i=0;i<numprocs;i++)
   {
       MPI_Isend(&num_events_recorded_l[myrank],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
       nreq++;
   }

   for(int i=0;i<numprocs;i++)
   {
       MPI_Irecv(&num_events_recorded[i],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
       nreq++;
   }
   MPI_Waitall(nreq,reqs,stats);

   hsize_t total_records = 0;
   for(int j=0;j<num_events_recorded.size();j++) total_records += (hsize_t)num_events_recorded[j];
   if(myrank==0)
   {
       std::cout <<" datasize = "<<VALUESIZE<<" total_records = "<<total_records<<std::endl;
   }

   uint64_t num_records = total_records;
   uint64_t total_size = num_records*VALUESIZE+num_records*sizeof(uint64_t);

   int record_size = VALUESIZE+sizeof(uint64_t);

   hsize_t offset = 0;
   for(int i=0;i<myrank;i++)
        offset += (hsize_t)num_events_recorded[i];

   free(reqs); free(stats);

   poffset = offset;
   trecords = total_records; 

   free(reqs); free(stats);
   return data_array;
}

void read_write_process::pwrite_files_from_memory(std::vector<std::string> &sts,std::vector<hsize_t>&total_records,std::vector<hsize_t>&offsets,std::vector<hsize_t>&numrecords)
{

    hid_t async_fapl = H5Pcreate(H5P_FILE_ACCESS);
    hid_t async_dxpl = H5Pcreate(H5P_DATASET_XFER);

    H5Pset_fapl_mpio(async_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    H5Pset_dxpl_mpio(async_dxpl, H5FD_MPIO_COLLECTIVE);

    std::string grp_name = "async_g";
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
     attr_size[0] = 5;
     hid_t attr_space[1];
     attr_name[0] = "Datasizes";
     attr_space[0] = H5Screate_simple(1, attr_size, NULL);

     std::vector<hid_t> event_ids;
     std::vector<hid_t> dset_ids;
     std::vector<hid_t> gids;
     std::vector<hid_t> fids;
     std::vector<int> indices;

     std::vector<hid_t> spaces;
     std::vector<hid_t> filespaces;
     std::vector<hid_t> memspaces;

    for(int i=0;i<sts.size();i++)
    {

	hid_t es_id = H5EScreate();
	m1.lock();
	auto r = write_names.find(sts[i]);
	int index = (r->second).first;
	m1.unlock();
	indices.push_back(index);
	std::string filename = "file"+sts[i]+".h5";
   	hsize_t dims[1];
   	dims[0] = numrecords[i];
   	hsize_t chunkdims[1];
   	chunkdims[0] = numrecords[i];
   	hsize_t maxdims[1];
   	maxdims[0] = (hsize_t)H5S_UNLIMITED;

   	hid_t dataset_pl = H5Pcreate(H5P_DATASET_CREATE);
   	int ret = H5Pset_chunk(dataset_pl,1,chunkdims);

   	hid_t file_dataspace = H5Screate_simple(1,&total_records[i],NULL);

   	hsize_t block_count = numrecords[i];
   	hid_t mem_dataspace = H5Screate_simple(1,&block_count, NULL);

	filespaces.push_back(file_dataspace);
	memspaces.push_back(mem_dataspace);

   	ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offsets[i],NULL,&block_count,NULL);

	hid_t fid = H5Fcreate_async(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, async_fapl, es_id);

        hid_t grp_id = H5Gcreate_async(fid, grp_name.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);

	hid_t dataset1 = H5Dcreate_async(fid, DATASETNAME1,s2,file_dataspace, H5P_DEFAULT,dataset_pl, H5P_DEFAULT,es_id);

	ret = H5Dwrite_async(dataset1,s2, mem_dataspace,file_dataspace,async_dxpl,myevents[index]->buffer->data(),es_id);

	std::vector<uint64_t> attr_data;
    	attr_data.push_back(total_records[i]);
    	attr_data.push_back(8);
    	attr_data.push_back(VALUESIZE);

	hid_t attr_id[1];
	attr_id[0] = H5Acreate_async(dataset1, attr_name[0], H5T_NATIVE_UINT64, attr_space[0], H5P_DEFAULT, H5P_DEFAULT,es_id);

	ret = H5Awrite_async(attr_id[0], H5T_NATIVE_UINT64, attr_data.data(),es_id);

	ret = H5Aclose_async(attr_id[0],es_id);

	dset_ids.push_back(dataset1);
	gids.push_back(grp_id);
	fids.push_back(fid);
	event_ids.push_back(es_id);
    }


    for(int i=0;i<event_ids.size();i++)
    {
	int err = H5Dclose_async(dset_ids[i],event_ids[i]);
	H5Gclose_async(gids[i],event_ids[i]);
	H5Fclose_async(fids[i],event_ids[i]);
	H5ESwait(event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
	H5ESclose(event_ids[i]);
	H5Sclose(filespaces[i]);
	H5Sclose(memspaces[i]);
	myevents[indices[i]]->m.unlock();
	clear_events(sts[i]);
    }

    H5Sclose(attr_space[0]);
    H5Pclose(async_fapl);
    H5Pclose(async_dxpl);
}

void read_write_process::pwrite_files_from_nvme(std::vector<std::string> &sts,std::vector<hsize_t>&total_records,std::vector<hsize_t> &offsets,std::vector<std::vector<struct event>*> &data_arrays)
{

  hid_t async_fapl = H5Pcreate(H5P_FILE_ACCESS);
  hid_t async_dxpl = H5Pcreate(H5P_DATASET_XFER);

  H5Pset_fapl_mpio(async_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
  H5Pset_dxpl_mpio(async_dxpl, H5FD_MPIO_COLLECTIVE);

  std::string grp_name = "async_g";
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
  attr_size[0] = 5;
  hid_t attr_space[1];
  attr_name[0] = "Datasizes";
  attr_space[0] = H5Screate_simple(1, attr_size, NULL);

  std::vector<hid_t> event_ids;
  std::vector<hid_t> dset_ids;
  std::vector<hid_t> gids;
  std::vector<hid_t> fids;
  std::vector<hid_t> filespaces;
  std::vector<hid_t> memspaces;

  for(int i=0;i<sts.size();i++)
  {

        std::string filename = "file"+sts[i]+".h5";
        hsize_t dims[1];
        dims[0] = data_arrays[i]->size();
        hsize_t chunkdims[1];
        chunkdims[0] = data_arrays[i]->size();
        hsize_t maxdims[1];
        maxdims[0] = (hsize_t)H5S_UNLIMITED;

        hid_t dataset_pl = H5Pcreate(H5P_DATASET_CREATE);
        int ret = H5Pset_chunk(dataset_pl,1,chunkdims);

        hid_t file_dataspace = H5Screate_simple(1,&total_records[i],NULL);

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
        attr_data.push_back(total_records[i]);
        attr_data.push_back(8);
        attr_data.push_back(VALUESIZE);
 
	hid_t attr_id[1];
        attr_id[0] = H5Acreate_async(dataset1, attr_name[0], H5T_NATIVE_UINT64, attr_space[0], H5P_DEFAULT, H5P_DEFAULT,es_id);

        ret = H5Awrite_async(attr_id[0], H5T_NATIVE_UINT64, attr_data.data(),es_id);

        ret = H5Aclose_async(attr_id[0],es_id);

        dset_ids.push_back(dataset1);
        gids.push_back(grp_id);
        fids.push_back(fid);
        event_ids.push_back(es_id);
    }

    for(int i=0;i<event_ids.size();i++)
    {
        int err = H5Dclose_async(dset_ids[i],event_ids[i]);
        H5Gclose_async(gids[i],event_ids[i]);
        H5Fclose_async(fids[i],event_ids[i]);
        H5ESwait(event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
        H5ESclose(event_ids[i]);
        H5Sclose(filespaces[i]);
        H5Sclose(memspaces[i]);
	delete data_arrays[i];
    }

    H5Sclose(attr_space[0]);
    H5Pclose(async_fapl);
    H5Pclose(async_dxpl);

}


void read_write_process::pwrite_extend_files_from_memory(std::vector<std::string> &sts,std::vector<hsize_t>&total_records,std::vector<hsize_t>&offsets,std::vector<hsize_t>&numrecords)
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

    std::string grp_name = "async_g";

    size_t num;
    hbool_t op_failed = false;

    adims[0] = (hsize_t)VALUESIZE;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event));
    H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);

    hsize_t attr_size[1];
    attr_size[0] = 5;
    hid_t attr_space[1];
    attr_name[0] = "Datasizes";
    attr_space[0] = H5Screate_simple(1, attr_size, NULL);

    std::vector<hid_t> event_ids;
    std::vector<hid_t> spaces;
    std::vector<hid_t> filespaces;
    std::vector<hid_t> memspaces;
    std::vector<int> indices;
    std::vector<hid_t> gids;
    std::vector<hid_t> fids;
    std::vector<hid_t> dset_ids;

    for(int i=0;i<sts.size();i++)
    {	    

      hid_t es_id = H5EScreate();
      m1.lock();
      auto r = write_names.find(sts[i]);
      int index = r->second.first;
      m1.unlock();

      indices.push_back(index);

      std::string filename = "file"+sts[i]+".h5";

      fid = H5Fopen_async(filename.c_str(), H5F_ACC_RDWR, async_fapl,es_id);
      
      hid_t grp_id = H5Gcreate_async(fid, grp_name.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, es_id);

      dataset1 = H5Dopen_async(fid, DATASETNAME1, H5P_DEFAULT,es_id);

      hid_t attr_id = H5Aopen_async(dataset1,attr_name[0],H5P_DEFAULT,es_id);
      std::vector<uint64_t> attrs;
      attrs.resize(5);

      int ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

      hsize_t block_size = (hsize_t)numrecords[i];

      dims[0] = (hsize_t)(total_records[i]+attrs[0]);

      maxsize = H5S_UNLIMITED;
      H5Dset_extent(dataset1, dims);
      file_dataspace = H5Dget_space(dataset1);
      mem_dataspace = H5Screate_simple(1,&block_size,&maxsize);

      hsize_t one = 1;
      offsets[i] += attrs[0];
      ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offsets[i],NULL,&one,&block_size);

      ret = H5Dwrite_async(dataset1,s2, mem_dataspace, file_dataspace,async_dxpl, myevents[index]->buffer->data(),es_id);

      attrs[0] += total_records[i];

      ret = H5Awrite_async(attr_id,H5T_NATIVE_UINT64,attrs.data(),es_id);

      ret = H5Aclose_async(attr_id,es_id);

      event_ids.push_back(es_id);
      filespaces.push_back(file_dataspace);
      memspaces.push_back(mem_dataspace);
      gids.push_back(grp_id);
      fids.push_back(fid);
      dset_ids.push_back(dataset1);
    }

    for(int i=0;i<event_ids.size();i++)
    {
	H5Dclose_async(dset_ids[i],event_ids[i]);
	H5Gclose_async(gids[i],event_ids[i]);
	H5Fclose_async(fids[i],event_ids[i]);
	H5ESwait(event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
	H5Sclose(filespaces[i]);
	H5Sclose(memspaces[i]);
	myevents[indices[i]]->m.unlock();
	clear_events(sts[i]);
    }

    H5Sclose(attr_space[0]);
    H5Pclose(async_fapl);
    H5Pclose(async_dxpl);

}

void read_write_process::pwrite_from_file(const char *filename,std::string &s,hid_t& meta,hid_t &meta_e,hid_t &dtag)
{

   /*std::string fname(filename);

   auto r = std::find(file_names.begin(),file_names.end(),fname);

   if(r == file_names.end())
   {
	pwrite_new_from_file(filename);
   }
   else pwrite_extend_from_file(filename,s);
*/
}
