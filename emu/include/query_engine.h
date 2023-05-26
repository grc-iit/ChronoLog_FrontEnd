#ifndef __QUERY_ENGINE_H_
#define __QUERY_ENGINE_H_

#include "query_request.h"
#include "query_response.h"
#include "distributed_queue.h"
#include "query_parser.h"
#include "rw.h"

struct thread_arg_q
{
 int tid;
};

class query_engine
{
   private:
	int numprocs;
	int myrank;
	distributed_queue *Q;
 	query_parser *S;  
        dsort *ds;	
	read_write_process *rwp;
	data_server_client *dsc;
 	std::vector<struct thread_arg_q> t_args;
	std::vector<std::thread> workers;
	int numthreads;
	std::atomic<int> end_session;
	std::atomic<int> query_number;
	boost::lockfree::queue<struct query_resp*> *O; 
	std::unordered_map<std::string,std::pair<int,int>> buffer_names;
	std::vector<struct atomic_buffer*> sbuffers;
	std::mutex m1;

   public:
	query_engine(int n,int r,data_server_client *c,read_write_process *w) : numprocs(n), myrank(r), dsc(c), rwp(w)
	{

    	   Q = new distributed_queue(numprocs,myrank);
	   O = new boost::lockfree::queue<struct query_resp*> (128);
	   tl::engine *t_server = dsc->get_thallium_server();
           tl::engine *t_server_shm = dsc->get_thallium_shm_server();
           tl::engine *t_client = dsc->get_thallium_client();
           tl::engine *t_client_shm = dsc->get_thallium_shm_client();
           std::vector<tl::endpoint> server_addrs = dsc->get_serveraddrs();
           std::vector<std::string> ipaddrs = dsc->get_ipaddrs();
           std::vector<std::string> shmaddrs = dsc->get_shm_addrs();
	   Q->server_client_addrs(t_server,t_client,t_server_shm,t_client_shm,ipaddrs,shmaddrs,server_addrs);
   	   Q->bind_functions();
	   query_number.store(0);
	   MPI_Barrier(MPI_COMM_WORLD);	  
	   S = new query_parser(numprocs,myrank);
	   ds = rwp->get_sorter();
	   end_session.store(0);
	   numthreads = 1;
	   t_args.resize(numthreads);
	   workers.resize(numthreads);
	   std::function<void(struct thread_arg_q *)> QSFunc(
           std::bind(&query_engine::service_query,this, std::placeholders::_1));

	   for(int i=0;i<numthreads;i++)
	   {
             t_args[i].tid = i;
	     std::thread qe{QSFunc,&t_args[i]};
	     workers[i] = std::move(qe);
	   }

	}
	~query_engine()
	{
	    delete Q;
	    delete S;
	    while(!O->empty())
	    {
		struct query_resp *r = nullptr;
		if(O->pop(r))
		{
		   delete r->response_vector;
		   delete r;
		}
	    }
	    delete O;
	}

	void end_sessions()
	{
	   if(myrank==0)
	   {
	      std::string s = "endsession";
	      send_query(s);
	   }

	   for(int i=0;i<workers.size();i++) workers[i].join();
	}

	void end_service()
	{
	   for(int i=0;i<workers.size();i++)
		   workers[i].join();
	}

	void send_query(std::string &s);
	void sort_response(std::string&,int,std::vector<struct event>*,uint64_t&);
	void get_range(std::vector<struct event>*,std::vector<struct event>*,std::vector<struct event>*,uint64_t minkeys[3],uint64_t maxkeys[3],int);
	std::vector<struct event> *sort_response_full(std::vector<struct event>*,std::vector<struct event>*,std::vector<struct event>*,int,uint64_t maxkeys[3]);
	void service_query(struct thread_arg_q*);
	bool end_file_read(bool,int);

	int create_buffer(std::string &s, int &sort_id)
	{
	   m1.lock();
	   auto r1 = buffer_names.find(s);
	   int index1 = -1;
	   int index2 = -1;
	   if(r1 == buffer_names.end())
	   {
		struct atomic_buffer *n = new struct atomic_buffer();
		n->buffer_size.store(0);
		n->buffer = new std::vector<struct event> ();
		sbuffers.push_back(n);
		index2 = ds->create_sort_buffer();
		std::pair<std::string,std::pair<int,int>> p;
		p.first.assign(s);
		p.second.first = sbuffers.size()-1;
		p.second.second = index2;
		buffer_names.insert(p);	
		index1 = sbuffers.size()-1;
	   }
	   else 
	   {
		index1 = r1->second.first;
		index2 = r1->second.second;
	   }
	   m1.unlock();

	   sort_id = index2;
	   return index1;

	}
};

#endif




