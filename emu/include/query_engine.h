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
	read_write_process *rwp;
	data_server_client *dsc;
	std::atomic<int> end_of_session;
 	std::vector<struct thread_arg_q> t_args;
	std::vector<std::thread> workers;
	int numthreads;
	std::atomic<int> query_number;

   public:
	query_engine(int n,int r,data_server_client *c,read_write_process *w) : numprocs(n), myrank(r), dsc(c), rwp(w)
	{

    	   Q = new distributed_queue(numprocs,myrank);
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
	   end_of_session.store(0);
	   numthreads = 2;
	   t_args.resize(numthreads);
	   workers.resize(numthreads);
	   std::function<void(struct thread_arg_q *)> QSFunc(
           std::bind(&query_engine::service_query,this, std::placeholders::_1));

	   for(int i=0;i<numthreads;i++)
	   {
	     std::thread qe{QSFunc,&t_args[i]};
	     workers[i] = std::move(qe);
	   }

	}
	~query_engine()
	{
	    delete Q;
	    delete S;
	}

	void end_sessions()
	{
	   end_of_session.store(1);

	   for(int i=0;i<numthreads;i++)
	   workers[i].join();
	}
	void send_query(std::string &s);
	void service_query(struct thread_arg_q*);

};

#endif




