
include_directories("../include")
set(clients test1_integerkeys test2_floatkeys test3_doublekeys test4_unsignedlongkeys test_ycsb_timeseries test_ycsb test1_integerkeys_mt)
foreach(client ${clients}) 
    add_executable(${client} ${client}.cpp)
    target_link_libraries(${client} margo mercury mercury_util na abt mpi hdf5 boost_system boost_atomic boost_thread pthread rt ${KVSLIBNAME} m)
    set_target_properties(${client} PROPERTIES FOLDER ${client})
endforeach()



