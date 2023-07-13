#include "KeyValueStoreIO.h"
#include "KeyValueStoreAccessor.h"
#include "invertedlist.h"


void KeyValueStoreIO::io_function(struct thread_arg *t)
{

    std::vector<int> op_type;
    op_type.resize(3);

    std::vector<int> consensus;

    consensus.resize(nservers);

    int count = 100000;
    int i = 0;

    while(true)
    {

       std::fill(op_type.begin(),op_type.end(),0);
       std::fill(consensus.begin(),consensus.end(),0);

       op_type[0] = (req_queue->empty()==true) ? 0 : 1;

       op_type[1] = (sync_queue->empty()==true) ? 0 : 1;

       if(op_type[0])
       {

         while(!req_queue->empty())
        { 	
	   struct request *r = nullptr;

	   if(!req_queue->pop(r)) break;

	   delete r;

        }
       }


       MPI_Allgather(&op_type[1],1,MPI_INT,consensus.data(),1,MPI_INT,MPI_COMM_WORLD);

       int nprocs_sync = 0;
       for(int i=0;i<consensus.size();i++)
	       nprocs_sync += consensus[i];
       if(nprocs_sync==nservers)
       {
	   uint32_t prev, next;
	   bool b = false;
	   uint32_t mask = 1;
	   mask = mask << 31;

	   do
	   {
		prev = synchronization_word.load();
		b = false;
		//if(prev != 0) break;
		next = prev | mask;
	   }while(!(b=synchronization_word.compare_exchange_strong(prev,next)));

	   while(synchronization_word.load()!=mask);

	   std::vector<struct sync_request *> sync_reqs;
	   while(!sync_queue->empty())
	   {
		struct sync_request *r = nullptr;
		if(sync_queue->pop(r))
		{
		   sync_reqs.push_back(r);
		}
		break;
	   }

	   for(int i=0;i<sync_reqs.size();i++)
	   {
	      integer_invlist* invlist = (integer_invlist*)(sync_reqs[i]->funcptr);

	      invlist->flush_table_file(0);
	   }

	   do
	   {
	      prev = synchronization_word.load();
	      next = prev & ~mask;
	   }while(!(b=synchronization_word.compare_exchange_strong(prev,next)));
	   break;
       }
	
       i++;
       //if(i==count) break;
    }
}

void KeyValueStoreIO::get_common_requests(std::vector<struct request*> &sync_reqs)
{

	   int numreqs = sync_reqs.size();
	   std::vector<int> req_count(nservers);
	   std::fill(req_count.begin(),req_count.end(),0);

	   MPI_Alltoall(&numreqs,1,MPI_INT,req_count.data(),1,MPI_INT,MPI_COMM_WORLD);

	   std::vector<int> send_lens(sync_reqs.size());
	   for(int i=0;i<sync_reqs.size();i++)
	   {
		send_lens[i] = sync_reqs[i]->name.length()+sync_reqs[i]->attr_name.length();
	   }

	   int total_req = 0;
	   for(int i=0;i<nservers;i++) total_req += req_count[i];
	   
	   std::vector<int> recv_lens;
	   recv_lens.resize(total_req);

	   int send_count = sync_reqs.size();
	   std::vector<int> displ(nservers);
	    
	   displ[0] = 0;
	   for(int i=1;i<nservers;i++) displ[i] = displ[i-1]+req_count[i-1];

	   MPI_Allgatherv(send_lens.data(),send_count,MPI_INT,recv_lens.data(),req_count.data(),displ.data(),MPI_INT,MPI_COMM_WORLD);
           std::string send_name = sync_reqs[0]->name+sync_reqs[0]->attr_name;

	   for(int i=1;i<sync_reqs.size();i++)
	   {	
		send_name += sync_reqs[i]->name+sync_reqs[i]->attr_name; 
	   } 

	   int total_length = 0;
	   for(int i=0;i<recv_lens.size();i++)
		total_length += recv_lens[i];

	   std::vector<char> recv_names(total_length);

	   int l = send_name.length();

	   std::vector<int> recv_counts(nservers);
	   std::fill(recv_counts.begin(),recv_counts.end(),0);
		
	   for(int i=0;i<nservers;i++)
	   {
		for(int j=0;j<req_count[i];j++) recv_counts[i] += recv_lens[displ[i]+j];
	   }

	   std::vector<int> recv_displ(nservers);
	   recv_displ[0] = 0;
	   for(int i=1;i<nservers;i++) recv_displ[i] = recv_displ[i-1]+recv_counts[i-1];


	   MPI_Allgatherv(send_name.data(),l,MPI_CHAR,recv_names.data(),recv_counts.data(),recv_displ.data(),MPI_CHAR,MPI_COMM_WORLD);

	   std::vector<std::vector<std::string>> recv_reqs(nservers);

	   for(int i=0;i<nservers;i++)
	   {
		for(int j=0;j<req_count[i];j++)
		{
		   int len = recv_lens[displ[i]+j];
		   std::string s;
		   s.assign(recv_names.data()+recv_displ[i],recv_names.data()+recv_displ[i]+len);
		   recv_displ[i]+=len;
		   recv_reqs[i].push_back(s);
		}
	   }
	   
	   std::vector<std::string> common_reqs;

	   std::unordered_map<std::string,int> stringcount;

	   for(int i=0;i<nservers;i++)
	   {
		for(int j=0;j<recv_reqs[i].size();j++)
		{
		   std::string s = recv_reqs[i][j];
		   auto r = stringcount.find(s);
		   if(r==stringcount.end())
		   {
			std::pair<std::string,int> p;
			p.first.assign(s);
			p.second = 0;
			stringcount.insert(p);
		   }	
		   else r->second = r->second+1;
		}
	   }
	

	   for(auto r = stringcount.begin(); r != stringcount.end(); ++r)
	   {
		if(r->second==nservers) common_reqs.push_back(r->first);
	   } 

	   std::sort(common_reqs.begin(),common_reqs.end());

}
