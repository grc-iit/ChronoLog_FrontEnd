
include_directories("../include")

add_executable(test1_integerkeys test1_integerkeys.cpp)
if(PARALLEL)
target_link_libraries(test1_integerkeys ${MPI_CXX_LIBRARIES})
endif()
target_link_libraries(test1_integerkeys thallium ${HDF5_C_LIBRARIES} ${Boost_LIBRARIES} -lpthread -lrt ${KVSLIBNAME} -lm)
set_target_properties(test1_integerkeys PROPERTIES FOLDER test1_integerkeys)
