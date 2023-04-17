#include "rw.h"

#define DATASETNAME1 "Data1"

typedef int DATATYPE;

void read_write_process::create_events(int num_events,std::string &s)
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
	e.data.resize(datasize);
	uint64_t ts = CM->Timestamp();

	e.ts = ts;
	      
	dm->add_event(e,index);
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

void read_write_process::pwrite_new(const char *filename,std::string &name)
{

    hid_t       fid;                                    
    hid_t       acc_tpl;                                
    hid_t       xfer_plist;                            
    hid_t       sid;                                   
    hid_t       file_dataspace;                         
    hid_t       mem_dataspace;                          
    hid_t       dataset1, dataset2, dataset3, dataset4;
    hid_t       dataset5, dataset6, dataset7;           
    hid_t       datatype;

    const char *attr_name[1];
    hsize_t     dims[1];                             
    hid_t attr_id[1];
    hsize_t attr_size[1];
    hid_t attr_space[1];
    hsize_t maxdims[1];
    hsize_t chunkdims[1];

    hsize_t start[1];               
    hsize_t count[1], stride[1]; 
    hsize_t block[1];              

    //std::string driverb(driverbuf);
    //std::cout <<" driver = "<<driverb<<std::endl;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT,fapl);
    
    if(fid < 0) std::cout <<" file not created"<<std::endl;

    H5Pclose(fapl);

    std::string fname(filename);
    file_names.insert(fname);

    m1.lock();
    auto r = write_names.find(name);
    int index = (r->second).first;
    event_metadata em = (r->second).second;
    m1.unlock();
    int datasize = em.get_datasize();

    std::vector<int> num_events_recorded_l,num_events_recorded;
    num_events_recorded_l.resize(numprocs);
    num_events_recorded.resize(numprocs);
    std::fill(num_events_recorded_l.begin(),num_events_recorded_l.end(),0);
    std::fill(num_events_recorded.begin(),num_events_recorded.end(),0);

    boost::shared_lock<boost::shared_mutex> lk(myevents[index]->m);

    num_events_recorded_l[myrank] = myevents[index]->buffer->size();

    MPI_Allreduce(num_events_recorded_l.data(),num_events_recorded.data(),numprocs,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

    int total_records = 0;
    for(int i=0;i<num_events_recorded.size();i++) total_records += num_events_recorded[i];

    if(myrank==0) std::cout <<" total records = "<<total_records<<std::endl;

    uint64_t num_records = total_records;
    uint64_t total_size = num_records*datasize+num_records*sizeof(uint64_t);

    int record_size = datasize+sizeof(uint64_t);
    attr_size[0] = 5;

    std::vector<uint64_t> attr_data;
    attr_data.push_back(total_records);
    attr_data.push_back(8);
    attr_data.push_back(datasize);

    uint64_t block_size = num_events_recorded[myrank]*record_size;
    hsize_t adims[1];
    adims[0] = (hsize_t)datasize;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event_hdf));
    H5Tinsert(s2,"key",HOFFSET(struct event_hdf,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event_hdf,data),s1);
     
    
    dims[0] = (hsize_t)num_records;
    chunkdims[0] = (hsize_t)num_records;
    maxdims[0] = (hsize_t)H5S_UNLIMITED;
    sid     = H5Screate_simple(1, dims, maxdims);

    hid_t dataset_pl = H5Pcreate(H5P_DATASET_CREATE);
    int ret = H5Pset_chunk(dataset_pl,1,chunkdims); 


    dataset1 = H5Dcreate2(fid, DATASETNAME1,s2, sid, H5P_DEFAULT,dataset_pl, H5P_DEFAULT);

    H5Sclose(sid);

    file_dataspace = H5Dget_space(dataset1);

    hsize_t offset = 0;
    for(int i=0;i<myrank;i++)
	 offset += (hsize_t)num_events_recorded[i];
    hsize_t block_count = (hsize_t)(num_events_recorded[myrank]);

    ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offset,NULL,&block_count,NULL);

    
    mem_dataspace = H5Screate_simple(1,&block_count, NULL);

    std::vector<struct event_hdf> *data_array1 = new std::vector<struct event_hdf> ();

    uint64_t min_v = UINT64_MAX; uint64_t max_v = 0;

    if(myrank==0) min_v = (*(myevents[index]->buffer))[0].ts;
    if(myrank==numprocs-1)
    {
	max_v = (*(myevents[index]->buffer))[myevents[index]->buffer->size()-1].ts;
    }

    MPI_Bcast(&min_v,1,MPI_UINT64_T,0,MPI_COMM_WORLD);
    MPI_Bcast(&max_v,1,MPI_UINT64_T,numprocs-1,MPI_COMM_WORLD);

    attr_data.push_back(min_v); attr_data.push_back(max_v);

    for(int i=0;i<myevents[index]->buffer->size();i++)
    {
	event e = (*(myevents[index]->buffer))[i];
	struct event_hdf ef;
	ef.ts = e.ts;
	memcpy(ef.data,e.data.data(),e.data.size());
	data_array1->push_back(ef);
    }
   
   xfer_plist = H5Pcreate(H5P_DATASET_XFER);

   ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);

   ret = H5Dwrite(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist, data_array1->data());

    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);
   
    attr_space[0] = H5Screate_simple(1, attr_size, NULL);

    attr_name[0] = "DataSizes";
    attr_id[0] = H5Acreate2(dataset1, attr_name[0], H5T_NATIVE_UINT64, attr_space[0], H5P_DEFAULT, H5P_DEFAULT);
    ret = H5Awrite(attr_id[0], H5T_NATIVE_UINT64, attr_data.data());

    H5Sclose(attr_space[0]);
    H5Aclose(attr_id[0]);

    ret = H5Dclose(dataset1);
    H5Fclose(fid); 

    data_array1->clear();
    delete data_array1;
}

void read_write_process::pwrite_extend(const char *filename,std::string &s)
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

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fopen(filename, H5F_ACC_RDWR, fapl);

    hid_t ret = H5Pclose(fapl);

    attr_name[0] = "DataSizes";

    dataset1 = H5Dopen2(fid, DATASETNAME1, H5P_DEFAULT);

    hid_t attr_id = H5Aopen(dataset1,attr_name[0],H5P_DEFAULT);
    std::vector<uint64_t> attrs;
    attrs.resize(5);

    ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

    m1.lock();
    auto r1 = write_names.find(s);
    int index = (r1->second).first;
    event_metadata em = (r1->second).second;
    m1.unlock();
    int datasize = em.get_datasize();

    boost::shared_lock<boost::shared_mutex> lk(myevents[index]->m);

    std::vector<int> num_events_recorded_l,num_events_recorded;
    num_events_recorded_l.resize(numprocs);
    num_events_recorded.resize(numprocs);
    std::fill(num_events_recorded_l.begin(),num_events_recorded_l.end(),0);
    std::fill(num_events_recorded.begin(),num_events_recorded.end(),0);

    num_events_recorded_l[myrank] = myevents[index]->buffer->size();

    MPI_Allreduce(num_events_recorded_l.data(),num_events_recorded.data(),numprocs,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
    
    int total_records = 0;
    for(int i=0;i<numprocs;i++) total_records += num_events_recorded[i];

    if(myrank==0) std::cout <<" total_records = "<<total_records<<std::endl;

    hsize_t offset = 0;
    for(int i=0;i<myrank;i++)
	   offset += (hsize_t)num_events_recorded[i];

    hsize_t block_size = (hsize_t)num_events_recorded[myrank];
    
    adims[0] = datasize;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event_hdf));
    H5Tinsert(s2,"key",HOFFSET(struct event_hdf,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event_hdf,data),s1);

    std::vector<struct event_hdf> *data_array1 = new std::vector<struct event_hdf> ();

    for(int i=0;i<myevents[index]->buffer->size();i++)
    {
	struct event e = (*(myevents[index]->buffer))[i];
	struct event_hdf ep;
        ep.ts = e.ts;	
	memcpy(ep.data,e.data.data(),e.data.size());
	data_array1->push_back(ep);
    }

    dims[0] = (hsize_t)(total_records+attrs[0]);

    maxsize = H5S_UNLIMITED;
    H5Dset_extent(dataset1, dims);
    file_dataspace = H5Dget_space(dataset1);
    mem_dataspace = H5Screate_simple(1,&block_size,&maxsize);
 
    hsize_t one = 1;
    offset += attrs[0]; 
    ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offset,NULL,&one,&block_size);
    xfer_plist = H5Pcreate(H5P_DATASET_XFER); 
    ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);
     
    ret = H5Dwrite(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist, data_array1->data());

    uint64_t min_v,max_v;
    if(myrank==0)
    {
	min_v = (*(myevents[index]->buffer))[0].ts;
    }
    if(myrank==numprocs-1)
    {
	max_v = (*(myevents[index]->buffer))[myevents[index]->buffer->size()-1].ts;
    }
    
    MPI_Bcast(&min_v,1,MPI_UINT64_T,0,MPI_COMM_WORLD);
    MPI_Bcast(&max_v,1,MPI_UINT64_T,numprocs-1,MPI_COMM_WORLD);

    attrs[0] += total_records;
    attrs[3] = min_v; attrs[4] = max_v;

    ret = H5Awrite(attr_id,H5T_NATIVE_UINT64,attrs.data());

    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);
    H5Aclose(attr_id);
    H5Dclose(dataset1);
    H5Fclose(fid);
    delete data_array1;

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

    std::vector<struct event_hdf> *data_array1 = nullptr;

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

    data_array1 = new std::vector<struct event_hdf> ();

    data_array1->resize(num_events);

        
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

    hsize_t adims[1];
    adims[0] = datasize;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event_hdf));
    H5Tinsert(s2,"key",HOFFSET(struct event_hdf,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event_hdf,data),s1);


    file_dataspace = H5Dget_space(dataset1);
    ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,&offset,NULL,&block_size,NULL);
    mem_dataspace = H5Screate_simple(1,&block_size, NULL);
    xfer_plist = H5Pcreate(H5P_DATASET_XFER);
    ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);
    ret = H5Dread(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist, data_array1->data());
     
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);

    ret = H5Aclose(attr_id);
    ret = H5Dclose(dataset1);

    for(int i=0;i<data_array1->size();i++)
    {
	struct event e;
	e.ts = (*data_array1)[i].ts;
	e.data.resize(datasize);
	memcpy(e.data.data(),(*data_array1)[i].data,datasize);
	readevents[index]->buffer->push_back(e);
    }

    delete data_array1;

   H5Fclose(fid);  

}


void read_write_process::pwrite(const char *filename,std::string &s)
{
   std::string fname(filename);

   auto r = std::find(file_names.begin(),file_names.end(),fname);
   if(r == file_names.end())
   {
	pwrite_new(filename,s);
   }
   else pwrite_extend(filename,s);

}
