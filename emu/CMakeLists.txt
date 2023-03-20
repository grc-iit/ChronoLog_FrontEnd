
add_custom_target(copy_hostfile)
add_custom_command(TARGET copy_hostfile
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/hostfile
        ${CMAKE_CURRENT_BINARY_DIR})


add_custom_target(copy_server_list_em)
add_custom_command(
        TARGET copy_server_list_em
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/server_list
        ${CMAKE_CURRENT_BINARY_DIR})

#add_executable(rw_process process.cpp main.cpp)
#include_directories(${HDF5_INCLUDE_DIRS})
#add_dependencies(rw_process copy_server_list_em)
#set_target_properties(${example} PROPERTIES ENVIRONMENT LD_PRELOAD=${CMAKE_BINARY_DIR}/libhcl.so:${THALLIUM_LIBRARY_LD_PRELOAD})
#target_link_libraries(rw_process -L${HDF5_LIBS} -lhdf5 ${MPI_CXX_LIBRARIES} -lrt)
#target_link_libraries(rw_process hcl)

add_executable(engine main.cpp process.cpp distributed_sort.cpp city.cpp util.h)
add_dependencies(engine copy_hostfile)
add_dependencies(engine copy_server_list_em)
target_link_libraries(engine ${MPI_CXX_LIBRARIES} thallium -L${HDF5_LIBS} -lhdf5 -lpthread -lrt)
set_target_properties(engine PROPERTIES FOLDER emu)
