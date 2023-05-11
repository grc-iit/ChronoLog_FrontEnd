#ifndef __PROCESS_H_
#define __PROCESS_H_

#include "mds.h"
#include "rw.h"
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "client.h"
#include "query_engine.h"

class emu_process
{

private:
      int numprocs;
      int myrank;
      int numcores;
      data_server_client *dsc;
      read_write_process *rwp;
      ClockSynchronization<ClocksourceCPPStyle> *CM;
      std::string server_addr;
      //metadata_server *MS; 	
      //metadata_client *MC;
      query_engine *QE;
public:

      emu_process(int np,int r,int n) : numprocs(np), myrank(r), numcores(n)
      {
	std::string unit = "microsecond";
	CM = new ClockSynchronization<ClocksourceCPPStyle> (myrank,numprocs,unit);

	int num_cores_rw = std::ceil(0.5*(double)numcores);
	int num_cores_ms = std::ceil(0.5*(double)numcores);

	if(myrank!=0) num_cores_rw = numcores;

	dsc = new data_server_client(numprocs,myrank); 

	rwp = new read_write_process(r,np,CM,num_cores_rw,dsc);
	QE = new query_engine(numprocs,myrank,dsc,rwp);
	int nchars;
	std::vector<char> addr_string;

	if(myrank==0)
	{
	  char processor_name[1024];
          int len = 0;
          MPI_Get_processor_name(processor_name, &len);
	  std::string myhostname;
          myhostname.assign(processor_name);
          char ip[16];
          struct hostent *he = gethostbyname(myhostname.c_str());
          auto **addr_list = (struct in_addr **) he->h_addr_list;
          strcpy(ip, inet_ntoa(*addr_list[0]));
	  std::string myipaddr;
          myipaddr.assign(ip);
	  nchars = myipaddr.length();
	  for(int i=0;i<myipaddr.length();i++)
		  addr_string.push_back(myipaddr[i]);
	}
	
	MPI_Bcast(&nchars,1,MPI_INT,0,MPI_COMM_WORLD);

	if(myrank != 0) addr_string.resize(nchars);

	MPI_Bcast(addr_string.data(),nchars,MPI_CHAR,0,MPI_COMM_WORLD);
	std::string addr_ip(addr_string.data());

	int port_no = 1234;
	server_addr = "ofi+sockets://"+addr_ip+":"+std::to_string(port_no);

	/*MS = nullptr;
	if(myrank==0)
	{
	   MS = new metadata_server(numprocs,myrank,server_addr,num_cores_ms);
	   MS->bind_functions();
	}	
	MPI_Barrier(MPI_COMM_WORLD);
        MC = new metadata_client(server_addr);*/
      }

      metadata_client *getclientobj()
      {
	return nullptr;//MC;
      }
      void synchronize()
      {
	CM->SynchronizeClocks();
	CM->ComputeErrorInterval();

      }
      std::string & get_serveraddr()
      {
	      return server_addr;
      }
      void prepare_service(std::string &name, event_metadata &em,int maxsize)
      {
	   int max_size_per_proc = maxsize/numprocs;
	   int rem = maxsize% numprocs;
	   if(myrank < rem) max_size_per_proc++;
	   rwp->create_write_buffer(name,em,max_size_per_proc);

      }
      read_write_process* get_rw_object()
      {
	      return rwp;
      }
      
      void read_events(const char *filename)
      {
	//rwp->preaddata(filename,s);
	rwp->preadfileattr(filename);

      }

      void data_streams(std::vector<std::string> &snames,std::vector<int> &total_events,int &nbatches)
      {

	rwp->spawn_write_streams(snames,total_events,nbatches);

	rwp->end_sessions();

	QE->send_query();

	MPI_Barrier(MPI_COMM_WORLD);

	QE->end_sessions();
      }

      ~emu_process()
      {
	//rwp->end_sessions();
	/*if(MS != nullptr) delete MS;
	delete MC;*/
	delete rwp;
	delete QE;
	delete CM;
	delete dsc;
      }

};

#endif
