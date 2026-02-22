# Install script for directory: /mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-c

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
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/iceoryx2/v0.8.999" TYPE DIRECTORY FILES "/tmp/target_gcc730/aarch64-unknown-linux-gnu/release/iceoryx2-ffi-c-cbindgen/include/")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "lib" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE FILE FILES
    "/tmp/target_gcc730/aarch64-unknown-linux-gnu/release/libiceoryx2_ffi_c.a"
    "/tmp/target_gcc730/aarch64-unknown-linux-gnu/release/libiceoryx2_ffi_c.so"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/share/doc/iceoryx2-c" TYPE FILE FILES
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-c/LICENSE-APACHE"
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-c/LICENSE-MIT"
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/iceoryx2-c/NOTICE.md"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "dev" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-c" TYPE FILE FILES
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/iceoryx2-c/iceoryx2-cConfigVersion.cmake"
    "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/iceoryx2-c/iceoryx2-cConfig.cmake"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-c/iceoryx2-cTargets.cmake")
    file(DIFFERENT _cmake_export_file_changed FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-c/iceoryx2-cTargets.cmake"
         "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/iceoryx2-c/CMakeFiles/Export/6dfa232419070da6f61194c0c01fe7d4/iceoryx2-cTargets.cmake")
    if(_cmake_export_file_changed)
      file(GLOB _cmake_old_config_files "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-c/iceoryx2-cTargets-*.cmake")
      if(_cmake_old_config_files)
        string(REPLACE ";" ", " _cmake_old_config_files_text "${_cmake_old_config_files}")
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-c/iceoryx2-cTargets.cmake\" will be replaced.  Removing files [${_cmake_old_config_files_text}].")
        unset(_cmake_old_config_files_text)
        file(REMOVE ${_cmake_old_config_files})
      endif()
      unset(_cmake_old_config_files)
    endif()
    unset(_cmake_export_file_changed)
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/iceoryx2-c" TYPE FILE FILES "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/iceoryx2-c/CMakeFiles/Export/6dfa232419070da6f61194c0c01fe7d4/iceoryx2-cTargets.cmake")
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/mnt/c/Users/taijsh/Desktop/shm_communicator2/iceoryx2/target_gcc730/aarch64/cc_build/iceoryx2-c/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
