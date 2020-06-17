cmake_minimum_required(VERSION 3.1 FATAL_ERROR)
project(roll_frame_torch)

#set libtorch path
#set(TORCH_HOME "/path/to/libtorch")
#set(TORCH_HOME $ENV{TORCH_HOME})
#set(CMAKE_PREFIX_PATH ${TORCH_HOME})
message("-- CMAKE_PREFIX_PATH : " ${CMAKE_PREFIX_PATH})

find_package(Torch REQUIRED)
find_package(JNI REQUIRED)
message("-- CMAKE_CXX_FLAGS : " ${CMAKE_CXX_FLAGS})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TORCH_CXX_FLAGS}")
# set(CMAKE_BUILD_TYPE Release)
# set(CMAKE_CXX_STANDARD 14)



include_directories(${JAVA_INCLUDE_PATH})
include_directories(include)
#include_directories(${TORCH_HOME}/include/torch/csrc/api/include)
#include_directories(${TORCH_HOME}/include)

# list all src cpp files
set(SOURCES src/eggroll/roll_frame/pytorch/roll_frame_torch.cc)

# specifying header is not necessary
# set(HEADER include/util.h)

add_library(roll_frame_torch SHARED ${SOURCES})
# add_executable(roll_frame_torch ${SOURCES})
target_link_libraries(roll_frame_torch "${TORCH_LIBRARIES}")

set_property(TARGET roll_frame_torch PROPERTY CXX_STANDARD 14)

# The following code block is suggested to be used on Windows.
# According to https://github.com/pytorch/pytorch/issues/25457,
# the DLLs need to be copied to avoid memory errors.

if (MSVC)
  message("-- Running on Windows ...")
  include_directories(${JAVA_INCLUDE_PATH}/win32)
  file(GLOB TORCH_DLLS "${TORCH_INSTALL_PREFIX}/lib/*.dll")
  add_custom_command(TARGET roll_frame_torch
                     POST_BUILD
                     COMMAND ${CMAKE_COMMAND} -E copy_if_different
                     ${TORCH_DLLS}
                     $<TARGET_FILE_DIR:roll_frame_torch>)
else()
  message("-- Running on Linux ...")
  include_directories(${JAVA_INCLUDE_PATH}/linux)
endif ()