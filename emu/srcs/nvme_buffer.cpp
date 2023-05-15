#include "nvme_buffer.h"


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
          std::pair<int,event_metadata> p1(file_names.size()-1,em);
          std::pair<std::string,std::pair<int,event_metadata>> p2(fname,p1);
          nvme_fnames.insert(p2);
      }
}

void nvme_buffers::copy_to_nvme(std::string &s,std::vector<struct event> *inp,int numevents)
{
    std::string fname = prefix+s;
    auto r = nvme_fnames.find(fname);

    if(r == nvme_fnames.end()) return;

    int index = r->second.first;

    boost::upgrade_lock<boost::shared_mutex> lk(*file_locks[index]);

    MyEventVect *ev = nvme_ebufs[index];

    for(int i=0;i<numevents;i++)
      ev->push_back((*inp)[i]);
    nvme_files[index]->flush();
}

void nvme_buffers::erase_from_nvme(std::string &s, int numevents)
{
      std::string fname = prefix+s;
      auto r = nvme_fnames.find(fname);

      if(r==nvme_fnames.end()) return;

      int index = r->second.first;

      boost::upgrade_lock<boost::shared_mutex> lk(*file_locks[index]);

      MyEventVect *ev = nvme_ebufs[index];

      ev->erase(ev->begin(),ev->begin()+numevents);

      nvme_files[index]->flush();
}

std::vector<struct event> *nvme_buffers::fetch_buffer(std::string &s,int &index)
{

     std::string fname = prefix+s;
     auto r = nvme_fnames.find(fname);

     if(r==nvme_fnames.end()) return nullptr;

     index = r->second.first;

     boost::shared_lock<boost::shared_mutex> lk(*file_locks[index]);

     std::vector<struct event> *data_array = new std::vector<struct event> ();

     MyEventVect *ev = nvme_ebufs[index];

     for(int i=0;i<ev->size();i++)
     {
         data_array->push_back((*ev)[i]);
     }

          //nvme_ebufs[index]->clear();
          //nvme_files[index]->flush();

     return data_array;
}

