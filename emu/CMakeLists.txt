
add_executable(engine main.cpp city.cpp rw.cpp distributed_sort.cpp rw_request.cpp query_parser.cpp util.h)
target_link_libraries(engine ${MPI_CXX_LIBRARIES} thallium ${HDF5_C_LIBRARIES} -L${HDF5_VOL_ASYNC}/lib -lasynchdf5 -lh5async ${Boost_LIBRARIES} -lpthread -lrt)
set_target_properties(engine PROPERTIES FOLDER emu)
