cmake_minimum_required(VERSION 3.10)

project(HDF5 VERSION 1.0.0 DESCRIPTION "hdf5 tests")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -gdwarf-2 -g3 -lm -lc -ldl -pthread")
set(RPC_LIB_FLAGS "")
include_directories(${CMAKE_PREFIX_PATH}/include)
add_definitions(-w)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

include(GNUInstallDirs)
find_package(HDF5)

set(HDF5_VOL_ASYNC "/home/asasidharan/spack/opt/spack/linux-ubuntu22.04-skylake_avx512/gcc-11.3.0/hdf5-vol-async-1.5-fr5n4kr6agu52ynzxp7nnm4xhyugulxw") 

include_directories(${HDF5_VOL_ASYNC}/include)
include_directories(${HDF5_INCLUDE_DIRS})

set(LIB_FLAGS ${RPC_LIB_FLAGS} ${MPI_CXX_LIBRARIES} ${HDF5_C_LIBRARIES} -lpthread -lrt)
# libs for NATS tests
#set(LIB_FLAGS ${LIB_FLAGS} -lnats)

# libs for Redis tests
#set(LIB_FLAGS ${LIB_FLAGS} -lev -L/home/kbateman/install/lib/ -L/home/kbateman/install/lib64/ -lhiredis -lredox)

add_executable(writer writer.cpp)
target_link_libraries(writer ${LIB_FLAGS}) 

add_executable(writer_async_vol writer_async_vol.cpp)
target_link_libraries(writer_async_vol ${LIB_FLAGS})

add_executable(reader reader.cpp)
target_link_libraries(reader ${LIB_FLAGS} -L${HDF5_VOL_ASYNC}/lib -lasynchdf5 -lh5async)
