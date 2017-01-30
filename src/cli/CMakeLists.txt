# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


if (WIN32)
set(MESOS_EXECUTE_TARGET mesos-execute
  CACHE STRING "Utility used to schedule and run a command in a mesos cluster.")

# THE MESOS-EXECUTE SOURCE.
###########################
set(MESOS_EXECUTE_SRC
  ${MESOS_EXECUTE_SRC}
  execute.cpp
  )

# THE MESOS EXECUTE.
#######################
add_executable(${MESOS_EXECUTE_TARGET} ${MESOS_EXECUTE_SRC})

# ADD LINKER FLAGS (generates, e.g., -lglog on Linux).
######################################################
target_link_libraries(${MESOS_EXECUTE_TARGET} ${MESOS_LIBS_TARGET})

# ADD BINARY DEPENDENCIES (tells CMake what to compile/build first).
####################################################################
add_dependencies(${MESOS_TARGET} ${MESOS_EXECUTE_TARGET})
add_dependencies(${MESOS_EXECUTE_TARGET} ${MESOS_LIBS_TARGET})

#MESOS-BATCH
set(MESOS_BATCH_TARGET mesos-batch
  CACHE STRING "Batch processing framework.")

# THE MESOS-BATCH SOURCE.
###########################
set(MESOS_BATCH_SRC
  ${MESOS_BATCH_SRC}
  batch.cpp
  )

# THE MESOS BATCH.
#######################
add_executable(${MESOS_BATCH_TARGET} ${MESOS_BATCH_SRC})

# ADD LINKER FLAGS (generates, e.g., -lglog on Linux).
######################################################
target_link_libraries(${MESOS_BATCH_TARGET} ${MESOS_LIBS_TARGET})

# ADD BINARY DEPENDENCIES (tells CMake what to compile/build first).
####################################################################
add_dependencies(${MESOS_TARGET} ${MESOS_BATCH_TARGET})
add_dependencies(${MESOS_BATCH_TARGET} ${MESOS_LIBS_TARGET})
endif(WIN32)
