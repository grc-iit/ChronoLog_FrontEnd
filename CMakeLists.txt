cmake_minimum_required(VERSION 3.10)

project(Emulator VERSION 1.0.0 DESCRIPTION "emulation engine")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DVALUESIZE=5000 -O3 -lm -lc -ldl -pthread")
set(RPC_LIB_FLAGS "")
include_directories(${CMAKE_PREFIX_PATH}/include)
add_definitions(-w)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

include(GNUInstallDirs)

find_package(mercury REQUIRED)
find_package(thallium REQUIRED)
include_directories(${THALLIUM_INCLUDE_DIRS})
set(RPC_LIB_FLAGS thallium ${RPC_LIB_FLAGS})

find_package(Boost REQUIRED COMPONENTS ALL)

find_package(MPI REQUIRED COMPONENTS CXX C)
find_package(HDF5 REQUIRED COMPONENTS C)

if (MPI_FOUND)
    message(STATUS "found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
endif ()
include_directories(${MPI_CXX_INCLUDE_DIRS})
include_directories(${Boost_INC_DIRS})
set(LIB_FLAGS ${RPC_LIB_FLAGS} ${MPI_CXX_LIBRARIES} -lpthread -lrt)

include_directories(${HDF5_C_INCLUDE_DIRS})

# libs for NATS tests
#set(LIB_FLAGS ${LIB_FLAGS} -lnats)

# libs for Redis tests
#set(LIB_FLAGS ${LIB_FLAGS} -lev -L/home/kbateman/install/lib/ -L/home/kbateman/install/lib64/ -lhiredis -lredox)

add_subdirectory(emu)
