#include <hdf5.h>
#include <vector>
#include <string>
#include <thread>
#include <iostream>
#include "h5_async_vol.h"

struct thread_arg
{
int tid;
std::vector<hid_t> event_ids;
std::vector<hid_t> fspaces;
std::vector<hid_t> memspaces;
std::vector<hid_t> fapls;
std::vector<hid_t> gapls;
std::vector<std::vector<uint32_t>*> data;

};

void pcreate(struct thread_arg *t)
{

    hid_t       acc_tpl;                                
    hid_t       xfer_plist;                            
    hid_t       sid;                                   
    hid_t       file_dataspace;                         
    hid_t       mem_dataspace;                          

    const char *attr_name[1];
    hsize_t     dims[1];                             
    hid_t attr_id[1];
    hsize_t attr_size[1];
    hid_t attr_space[1];

    hid_t es_id = H5EScreate();
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    hid_t dataset_pl = H5Pcreate(H5P_DATASET_CREATE);
    std::string dsetname = "dataset1";

    std::string filename = "file"+std::to_string(t->tid)+".h5";

    hid_t fid = H5Fcreate_async(filename.c_str(), H5F_ACC_TRUNC|H5F_ACC_SWMR_WRITE, H5P_DEFAULT, fapl,es_id);

    hsize_t total_ints = 500000000;
    hid_t filespace = H5Screate_simple(1,&total_ints,NULL);

    std::string grp_name = "async"+std::to_string(t->tid);

    hid_t grp_id = H5Gcreate_async(fid,grp_name.c_str(),H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT,es_id);

    hid_t dset = H5Dcreate_async(fid,dsetname.c_str(),H5T_NATIVE_UINT,filespace,H5P_DEFAULT,dataset_pl,H5P_DEFAULT,es_id);

    H5Dclose_async(dset,es_id);
    H5Gclose_async(grp_id,es_id);
    H5Fclose_async(fid,es_id);

    size_t num;
    hbool_t op_failed = false;
    H5ESwait(es_id,H5ES_WAIT_FOREVER,&num,&op_failed);
    H5Sclose(filespace);
    H5Pclose(fapl);
    H5ESclose(es_id);
}

void pwrite(struct thread_arg *t)
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

    hid_t es_id = H5EScreate();

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    hid_t gapl = H5Pcreate(H5P_GROUP_ACCESS);
    std::string dsetname = "dataset1";

    std::string filename = "file"+std::to_string(t->tid)+".h5";

    hid_t file = H5Fopen_async(filename.c_str(),H5F_ACC_RDWR|H5F_ACC_SWMR_WRITE,fapl,es_id);

    std::string grp_name = "async"+std::to_string(t->tid);

    hid_t grp_id = H5Gopen_async(file,grp_name.c_str(),gapl,es_id);

    hid_t dset = H5Dopen_async(file,"dataset1",H5P_DEFAULT,es_id);

    hsize_t block_size = 500000000;

    hid_t filespace =  H5Screate_simple(1,&block_size,NULL);

    hid_t memspace = H5Screate_simple(1,&block_size,NULL);

    std::vector<uint32_t> *data_array = new std::vector<uint32_t> ();

    data_array->resize(block_size);

    hsize_t offset = 0;

    int ret = H5Dwrite_async(dset,H5T_NATIVE_UINT,H5S_ALL,H5S_ALL,H5P_DEFAULT,data_array->data(),es_id);
    

    H5Dclose_async(dset,es_id);
    H5Gclose_async(grp_id,es_id);
    size_t num;
    hbool_t op_failed = false;
    H5Fclose_async(file,es_id);

    t->event_ids.push_back(es_id);
    t->fspaces.push_back(filespace);
    t->memspaces.push_back(memspace);
    t->fapls.push_back(fapl);
    t->gapls.push_back(gapl);
    t->data.push_back(data_array); 
    /*H5ESwait(es_id,H5ES_WAIT_FOREVER,&num,&op_failed);
	  
    H5ESclose(es_id);
    H5Pclose(gapl);
    H5Pclose(fapl);
    delete data_array;
    H5Sclose(filespace);
    H5Sclose(memspace);*/
}

void completion(struct thread_arg *t)
{

   for(int i=0;i<t->event_ids.size();i++)
   {
	size_t num;
	hbool_t op_failed = false;
	H5ESwait(t->event_ids[i],H5ES_WAIT_FOREVER,&num,&op_failed);
	H5ESclose(t->event_ids[i]);
	H5Pclose(t->gapls[i]);
	H5Pclose(t->fapls[i]);
	delete t->data[i];
	H5Sclose(t->fspaces[i]);
	H5Sclose(t->memspaces[i]);
   }


}


void pread(struct thread_arg *t)
{
    hid_t       fid;
    hid_t       acc_tpl;
    hid_t       xfer_plist;
    hid_t       sid;
    hid_t       dataset1, dataset2, dataset3, dataset4;
    hid_t       dataset5, dataset6, dataset7;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);

    int id = t->tid-4;
    std::string filename = "file"+std::to_string(id)+".h5";

    fid = H5Fopen(filename.c_str(),H5F_ACC_RDONLY|H5F_ACC_SWMR_READ,fapl);

    if(fid < 0) return;


    hid_t dataset = H5Dopen2(fid,"dataset1",H5P_DEFAULT);

    hsize_t total_ints = 500000000;

    hid_t filespace = H5Screate_simple(1,&total_ints,NULL);
    hsize_t block_size = total_ints;
    hid_t memspace = H5Screate_simple(1,&block_size,NULL);
	    //H5Dget_space(dataset);

    hsize_t offset = 0;
    int ret = H5Sselect_hyperslab(filespace, H5S_SELECT_SET,&offset, NULL,&block_size, NULL);

    std::vector<uint32_t> *rdata = new std::vector<uint32_t> ();

    rdata->resize(total_ints);

    ret = H5Dread(dataset,H5T_NATIVE_UINT,memspace,filespace,H5P_DEFAULT,rdata->data());


    H5Dclose(dataset);
    H5Sclose(filespace);
    H5Sclose(memspace);
    H5Pclose(fapl);

    H5Fclose(fid);

}

void thread_create(struct thread_arg *t)
{
   if(t->tid/4==0) pcreate(t);

}

void thread_work(struct thread_arg *t)
{


  if(t->tid/4==0) pwrite(t);
  if(t->tid/4==1)
  pread(t);


}

int main(int argc,char **argv)
{

H5open();

hbool_t is_ts = false;

H5is_library_threadsafe(&is_ts);
if(is_ts)
std::cout <<" is_ts = "<<is_ts<<std::endl;

H5VLis_connector_registered_by_name("async");

int num_threads = 4;

std::vector<struct thread_arg> t_args(num_threads);
std::vector<std::thread> workers(num_threads);

auto t1 = std::chrono::high_resolution_clock::now();

  for(int i=0;i<num_threads;i++)
  {
	t_args[i].tid = i;
	std::thread t{thread_create,&t_args[i]};
	workers[i] = std::move(t);
  }

  for(int i=0;i<num_threads;i++)
	  workers[i].join();
auto t2 = std::chrono::high_resolution_clock::now();

auto ctime = std::chrono::duration<double>(t2-t1).count();

std::cout <<" ctime = "<<ctime<<std::endl;

t1 = std::chrono::high_resolution_clock::now();

  for(int i=0;i<num_threads;i++)
  {
	std::thread t{thread_work,&t_args[i]};
	workers[i] = std::move(t);
  }

  for(int i=0;i<num_threads;i++)
	  workers[i].join();
  
t2 = std::chrono::high_resolution_clock::now();

double t = std::chrono::duration<double> (t2-t1).count();

std::cout <<" num_threads = "<<num_threads<<" write time = "<<t<<std::endl;

for(int i=0;i<num_threads;i++)
{
   std::thread t{completion,&t_args[i]};
   workers[i] = std::move(t);
}

for(int i=0;i<num_threads;i++)
  workers[i].join();

H5close();

}
