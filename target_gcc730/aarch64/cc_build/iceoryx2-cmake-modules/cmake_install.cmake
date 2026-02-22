# Install script for directory: /mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-cmake-modules

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "TRUE")
endif()

# Set path to fallback-tool for dependency-resolution.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/opt/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu/bin/aarch64-linux-gnu-objdump")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-cmake-modules/modules" TYPE DIRECTORY FILES "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-cmake-modules/modules/")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-cmake-modules/modules" TYPE DIRECTORY FILES "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-cmake-modules/platform/generic/modules/")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-cmake-modules/modules" TYPE DIRECTORY FILES "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/generated/iceoryx2-cmake-modules/modules/")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/share/doc/iceoryx2-cmake-modules" TYPE FILE FILES
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-cmake-modules/LICENSE-APACHE"
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-cmake-modules/LICENSE-MIT"
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-cmake-modules/NOTICE.md"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-cmake-modules" TYPE FILE FILES
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/iceoryx2-cmake-modules/iceoryx2-cmake-modulesConfigVersion.cmake"
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/iceoryx2-cmake-modules/iceoryx2-cmake-modulesConfig.cmake"
    )
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/iceoryx2-cmake-modules/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
