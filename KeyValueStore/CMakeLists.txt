
include_directories("../emu/include")
include_directories("include")
add_executable(KeyValueStore srcs/clientservice.cpp)
target_link_libraries(KeyValueStore ${MPI_CXX_LIBRARIES} thallium ${HDF5_C_LIBRARIES} -L${HDF5_VOL_ASYNC}/lib -lasynchdf5 -lh5async ${Boost_LIBRARIES} -lpthread -lrt ${PROJECT_NAME})
set_target_properties(KeyValueStore PROPERTIES FOLDER KeyValueStore)
