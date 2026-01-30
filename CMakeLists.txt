cmake_minimum_required(VERSION 3.28)
project(ArrowComputeEngine)

############################## Custom build modes ###############################

set(CMAKE_CXX_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C++ compiler during Sanitize builds."
  FORCE)
set(CMAKE_C_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C compiler during Sanitize builds."
  FORCE)
set(CMAKE_EXE_LINKER_FLAGS_SANITIZE
  ${CMAKE_EXE_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used for linking binaries during Sanitize builds."
  FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  ${CMAKE_SHARED_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used by the shared libraries linker during Sanitize builds."
  FORCE)
mark_as_advanced(
  CMAKE_CXX_FLAGS_SANITIZE		  CMAKE_EXE_LINKER_FLAGS_SANITIZE
  CMAKE_C_FLAGS_SANITIZE		  CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  )

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
  "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
  FORCE)

############################### External Projects ###############################

include(ExternalProject)
set(BOSS_SOURCE_BRANCH main CACHE STRING "if boss core is built from source, which thread should be fetched")
set(BOSS_SOURCE_REPOSITORY https://github.com/boss-dms/BOSS.git CACHE STRING "url for the boss repository")
ExternalProject_Add(BOSS
  GIT_REPOSITORY ${BOSS_SOURCE_REPOSITORY}
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  GIT_TAG ${BOSS_SOURCE_BRANCH}
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ArrowComputeEngine_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}> -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
  )


ExternalProject_Add(Arrow
  URL https://github.com/apache/arrow/archive/refs/tags/apache-arrow-23.0.0.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP NEW
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  SOURCE_SUBDIR cpp
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ArrowComputeEngine_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}> -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -DARROW_COMPUTE=ON -DARROW_CSV=ON -DARROW_FILESYSTEM=ON -DARROW_ACERO=ON ARROW_JEMALLOC_USE_SHARED=OFF ARROW_SIMD_LEVEL=NONE ARROW_RUNTIME_SIMD_LEVEL=NONE ARROW_ENABLE_THREADING=OFF ARROW_JEMALLOC=ON ARROW_IPC=OFF
  )
  

#################################### Targets ####################################
if(MSVC)
  # not making any difference on Windows
  # and helps Visual Studio to parse it correctly as a target
  set(LibraryType SHARED)
else()
  set(LibraryType MODULE)
endif(MSVC)
add_library(ArrowComputeEngine ${LibraryType} Source/ArrowComputeEngine.cpp)

set_property(TARGET ArrowComputeEngine PROPERTY CXX_STANDARD 23)
if(MSVC)
  target_compile_options(ArrowComputeEngine PUBLIC "/Zc:__cplusplus")
  target_compile_options(ArrowComputeEngine PUBLIC "/EHsc")
endif(MSVC)
target_include_directories(ArrowComputeEngine SYSTEM PUBLIC ${ArrowComputeEngine_BINARY_DIR}/deps/include)
add_dependencies(ArrowComputeEngine BOSS)
add_dependencies(ArrowComputeEngine Arrow)
target_link_libraries(ArrowComputeEngine PUBLIC "arrow")
target_link_libraries(ArrowComputeEngine PUBLIC "arrow_compute")
target_link_libraries(ArrowComputeEngine PUBLIC "arrow_acero")
target_link_directories(ArrowComputeEngine PUBLIC ${ArrowComputeEngine_BINARY_DIR}/deps/lib)



set_target_properties(ArrowComputeEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS ArrowComputeEngine LIBRARY DESTINATION lib)
