#include "nvme_buffer.h"
#include <mpi.h>

void nvme_buffers::create_nvme_buffer(std::string &s,event_metadata &em)
{
     std::string fname = prefix+s;
     auto r = nvme_fnames.find(fname);

     if(r == nvme_fnames.end())
     {
          file_mapping::remove(fname.c_str());
          int maxsize = 65536*VALUESIZE;
          managed_mapped_file *mf = new managed_mapped_file(create_only,fname.c_str(),maxsize);
          const allocator_event_t allocator_e(mf->get_segment_manager());
          std::string vecname = fname+"MyEventVector";
          MyEventVect *ev = mf->construct<MyEventVect> (vecname.c_str()) (allocator_e);
          boost::shared_mutex *m = new boost::shared_mutex();
          file_locks.push_back(m);
          nvme_ebufs.push_back(ev);
          nvme_files.push_back(mf);
          file_names.push_back(fname);
          buffer_names.push_back(vecname);
          std::pair<std::string,std::pair<int,event_metadata>> p2;
	  p2.first.assign(fname);
	  boost::mutex *mock = new boost::mutex();
	  p2.second.first = file_names.size()-1;
          nvme_fnames.insert(p2);
	  blocks.push_back(mock);
	  std::atomic<int> *bs = (std::atomic<int>*)std::malloc(sizeof(std::atomic<int>));
	  bs->store(0);
	  buffer_state.push_back(bs);
      }
}

void nvme_buffers::copy_to_nvme(std::string &s,std::vector<struct event> *inp,int numevents)
{
    std::string fname = prefix+s;
    auto r = nvme_fnames.find(fname);

    if(r == nvme_fnames.end()) return;

    int index = r->second.first;
   
    //int tag = index;

    //get_buffer(index,tag,1);

    //boost::upgrade_lock<boost::shared_mutex> lk(*file_locks[index]);

    MyEventVect *ev = nvme_ebufs[index];

    for(int i=0;i<numevents;i++)
      ev->push_back((*inp)[i]);

    nvme_files[index]->flush();
    //buffer_state[index]->store(0);

}

void nvme_buffers::erase_from_nvme(std::string &s, int numevents)
{
      std::string fname = prefix+s;
      auto r = nvme_fnames.find(fname);

      if(r==nvme_fnames.end()) return;

      int index = r->second.first;

      //int tag = 100+index;

      //get_buffer(index,tag,2);

      //boost::upgrade_lock<boost::shared_mutex> lk(*file_locks[index]);

      MyEventVect *ev = nvme_ebufs[index];

      ev->erase(ev->begin(),ev->begin()+numevents);

      nvme_files[index]->flush();
      //buffer_state[index]->store(0);

}

void nvme_buffers::get_buffer(int index,int tag,int type)
{
   MPI_Request *reqs = (MPI_Request *)std::malloc(2*numprocs*sizeof(MPI_Request));
   int nreq = 0;

   int s_req = type;
   int op;

   int m_tag = tag;
   if(myrank==0)
   {
	//blocks[index]->lock();

	//std::cout <<" index = "<<index<<" type = "<<type<<" tag = "<<tag<<std::endl;	
	int prev_value = 0;
	int next_value = type;

	do
	{
	   prev_value = 0;
	   next_value = type;

	}while(!buffer_state[index]->compare_exchange_strong(prev_value,next_value));

	for(int i=1;i<numprocs;i++)
	{
	   MPI_Isend(&s_req,1,MPI_INT,i,m_tag,MPI_COMM_WORLD,&reqs[nreq]);
	   nreq++;
	}

   }
   else
   {
	MPI_Irecv(&op,1,MPI_INT,0,m_tag,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   if(myrank != 0) buffer_state[index]->store(type);

   std::free(reqs);

}

int nvme_buffers::buffer_index(std::string &s)
{
    std::string fname = prefix+s;
    auto r = nvme_fnames.find(fname);

    int index;

    if(r == nvme_fnames.end()) index = -1;
    else index = r->second.first;

    return index;
}

void nvme_buffers::release_buffer(int index)
{
     /*if(myrank==0) 
     {
	  blocks[index]->unlock();
     }*/
     buffer_state[index]->store(0);

}

void nvme_buffers::fetch_buffer(std::vector<struct event> *data_array,std::string &s,int &index, int &tag)
{

     std::string fname = prefix+s;
     auto r = nvme_fnames.find(fname);

     if(r==nvme_fnames.end()) return;

     index = r->second.first;

     //tag += index;

     //get_buffer(index,tag,3);

     //boost::shared_lock<boost::shared_mutex> lk(*file_locks[index]);

     MyEventVect *ev = nvme_ebufs[index];

     for(int i=0;i<ev->size();i++)
     {
         data_array->push_back((*ev)[i]);
     }

          //nvme_ebufs[index]->clear();
          //nvme_files[index]->flush();

     //buffer_state[index]->store(0);

}

