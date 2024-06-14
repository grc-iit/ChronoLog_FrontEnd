cmake_minimum_required(VERSION 3.10)

project(Emulator VERSION 1.0.0 DESCRIPTION "emulation engine")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DVALUESIZE=1000 -DMAXBUFFERSIZE=16384 -DCHUNKSIZE=4194304 -DMAXSTREAMS=50 -DBLOCKSIZE=4194304 -DNDISKS=4 -DMAXBLOCKS=1000 -O3 -lm -lc -ldl -pthread")
set(RPC_LIB_FLAGS "")
include_directories(${CMAKE_PREFIX_PATH}/include)
add_definitions(-w)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g")

include(GNUInstallDirs)

set(MERCURY_PATH "/projects/bbrr/aparna1/IO/mercury")
set(MARGO_PATH "/projects/bbrr/aparna1/IO/margo")
set(ABT_PATH "/projects/bbrr/aparna1/IO/argobots")
set(THALLIUM_PATH "/projects/bbrr/aparna1/IO/thallium")
set(HDF5_PATH "/projects/bbrr/aparna1/IO/hdf5_ompi")
set(MPI_PATH "/projects/bbrr/aparna1/IO/mpich-4.2")
set(JSONC_PATH "/projects/bbrr/aparna1/IO/spack/opt/spack/linux-rhel8-zen3/gcc-11.2.0/json-c-0.16-ioo7c34koa6xzom2ojuhq55l4avzerjo")

include_directories(${MERCURY_PATH}/include)
include_directories(${MARGO_PATH}/include)
include_directories(${ABT_PATH}/include)
include_directories(${THALLIUM_PATH}/include)
include_directories(${HDF5_PATH}/include)
include_directories(${JSONC_PATH}/include)
#include_directories(${MPI_PATH}/include)

#find_package(mercury REQUIRED)
#find_package(thallium REQUIRED)
#include_directories(${THALLIUM_INCLUDE_DIRS})
#set(RPC_LIB_FLAGS thallium ${RPC_LIB_FLAGS})

#find_package(Boost REQUIRED COMPONENTS ALL)

set(Boost_INC_DIRS "/sw/external/boost/boost_1_81_0")

#find_package(MPI REQUIRED COMPONENTS CXX C)
find_package(TBB REQUIRED)
#find_package(HDF5 REQUIRED COMPONENTS C)

#if (MPI_FOUND)
#    message(STATUS "found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
#endif ()
#include_directories(${MPI_CXX_INCLUDE_DIRS})
include_directories(${Boost_INC_DIRS})
include_directories(${MPI_PATH}/include)
include_directories("/sw/spack/deltas11-2023-03/apps/linux-rhel8-zen/gcc-8.5.0/intel-tbb-2021.9.0-m5eh674/include")
#set(LIB_FLAGS ${RPC_LIB_FLAGS} ${MPI_CXX_LIBRARIES} -lpthread -lrt)
#set(HDF5_VOL_ASYNC "/home/asasidharan/spack/opt/spack/linux-ubuntu22.04-skylake_avx512/gcc-11.3.0/hdf5-vol-async-1.5-fr5n4kr6agu52ynzxp7nnm4xhyugulxw") 

#include_directories(${HDF5_C_INCLUDE_DIRS})
#include_directories(${HDF5_VOL_ASYNC}/include)

# libs for NATS tests
#set(LIB_FLAGS ${LIB_FLAGS} -lnats)

# libs for Redis tests
#set(LIB_FLAGS ${LIB_FLAGS} -lev -L/home/kbateman/install/lib/ -L/home/kbateman/install/lib64/ -lhiredis -lredox)

add_subdirectory(emu)
add_subdirectory(PluginKVS)
#add_subdirectory(PubSubClient)
