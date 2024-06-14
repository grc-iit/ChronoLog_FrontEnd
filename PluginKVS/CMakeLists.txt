
include_directories("../emu/include")
include_directories("include")

set(KVSLIBNAME  "KVSLib")
add_library(${KVSLIBNAME} SHARED srcs/KeyValueStore.cpp srcs/KeyValueStoreIO.cpp srcs/util.cpp ../emu/srcs/city.cpp)
include_directories(${KVSLIBNAME} include)
target_link_directories(${KVSLIBNAME} PUBLIC ${MERCURY_PATH}/lib)
target_link_directories(${KVSLIBNAME} PUBLIC ${ABT_PATH}/lib)
target_link_directories(${KVSLIBNAME} PUBLIC ${MARGO_PATH}/lib)
target_link_directories(${KVSLIBNAME} PUBLIC ${HDF5_PATH}/lib)
target_link_directories(${KVSLIBNAME} PUBLIC ${MPI_PATH}/lib)
target_link_directories(${KVSLIBNAME} PUBLIC ${JSONC_PATH}/lib64)
target_link_directories(${KVSLIBNAME} PUBLIC "/sw/external/boost/boost_1_81_0/stage/lib")
target_link_libraries(${KVSLIBNAME} margo mercury mercury_util na abt mpicxx mpi hdf5 boost_system boost_atomic boost_thread pthread rt)
set_target_properties(${KVSLIBNAME} PROPERTIES LINKER_LANGUAGE CXX)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(TARGETS ${KVSLIBNAME}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
add_subdirectory(KVSClient)
