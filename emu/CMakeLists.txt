
#add_executable(rw_process process.cpp main.cpp)
#include_directories(${HDF5_INCLUDE_DIRS})
#add_dependencies(rw_process copy_server_list_em)
#set_target_properties(${example} PROPERTIES ENVIRONMENT LD_PRELOAD=${CMAKE_BINARY_DIR}/libhcl.so:${THALLIUM_LIBRARY_LD_PRELOAD})
#target_link_libraries(rw_process -L${HDF5_LIBS} -lhdf5 ${MPI_CXX_LIBRARIES} -lrt)
#target_link_libraries(rw_process hcl)

add_executable(engine main.cpp city.cpp rw.cpp distributed_sort.cpp rw_request.cpp query_parser.cpp util.h)
target_link_libraries(engine ${MPI_CXX_LIBRARIES} thallium -L${HDF5_LIBS} -lhdf5 -lpthread -lrt)
set_target_properties(engine PROPERTIES FOLDER emu)
