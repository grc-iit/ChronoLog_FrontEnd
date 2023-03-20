cmake_minimum_required(VERSION 3.10)

project(Emulator VERSION 1.0.0 DESCRIPTION "emulation engine")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -gdwarf-2 -g3 -lm -lc -ldl -pthread")
set(RPC_LIB_FLAGS "")
include_directories(${CMAKE_PREFIX_PATH}/include)
add_definitions(-w)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

include(GNUInstallDirs)

find_package(mercury REQUIRED)
find_package(thallium REQUIRED)
include_directories(${THALLIUM_INCLUDE_DIRS})
set(RPC_LIB_FLAGS thallium ${RPC_LIB_FLAGS})

find_package(MPI REQUIRED COMPONENTS CXX C)
if (MPI_FOUND)
    message(STATUS "found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
endif ()
include_directories(${MPI_CXX_INCLUDE_DIRS})
set(LIB_FLAGS ${RPC_LIB_FLAGS} ${MPI_CXX_LIBRARIES} -lpthread -lrt)
set(HDF5_INCLUDE_DIRS "/home/asasidharan/spack/opt/spack/linux-ubuntu22.04-skylake_avx512/gcc-11.3.0/hdf5-1.14.0-pa3hja77wn44jewrnsfojlfn46y6iukr/include")
set(HDF5_LIBS "/home/asasidharan/spack/opt/spack/linux-ubuntu22.04-skylake_avx512/gcc-11.3.0/hdf5-1.14.0-pa3hja77wn44jewrnsfojlfn46y6iukr/lib")

include_directories(${HDF5_INCLUDE_DIRS})

# libs for NATS tests
#set(LIB_FLAGS ${LIB_FLAGS} -lnats)

# libs for Redis tests
#set(LIB_FLAGS ${LIB_FLAGS} -lev -L/home/kbateman/install/lib/ -L/home/kbateman/install/lib64/ -lhiredis -lredox)

add_subdirectory(emu)
