
include_directories("../include")
set(clients test1_integerkeys test2_floatkeys)

foreach(client ${clients}) 
    add_executable(${client} ${client}.cpp)
    target_link_libraries(${client} thallium ${MPI_CXX_LIBRARIES} ${HDF5_C_LIBRARIES} ${Boost_LIBRARIES} -lpthread -lrt ${KVSLIBNAME} -lm)
    set_target_properties(${client} PROPERTIES FOLDER ${client})
endforeach()



