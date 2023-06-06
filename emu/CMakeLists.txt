
include_directories("include")
add_executable(engine srcs/main.cpp srcs/city.cpp srcs/rw.cpp srcs/distributed_sort.cpp srcs/rw_request.cpp srcs/query_parser.cpp srcs/nvme_buffer.cpp srcs/query_engine.cpp srcs/write_buffer.cpp)
target_link_libraries(engine ${MPI_CXX_LIBRARIES} thallium ${HDF5_C_LIBRARIES} -L${HDF5_VOL_ASYNC}/lib -lasynchdf5 -lh5async ${Boost_LIBRARIES} -lpthread -lrt)
set_target_properties(engine PROPERTIES FOLDER emu)
