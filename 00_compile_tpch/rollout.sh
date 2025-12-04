#!/bin/bash
set -e

PWD=$(get_pwd ${BASH_SOURCE[0]})

step="compile_tpch"

log_time "Step ${step} started"

init_log ${step}
start_log
schema_name=${DB_VERSION}
export schema_name
table_name="compile"
export table_name

compile_flag="true"

function make_tpc()
{
  #compile the tools
  unzip -o -d ${TPC_H_DIR}/00_compile_tpch/ ${TPC_H_DIR}/00_compile_tpch/TPCH-software-code-3.0.1.zip
  cp ${TPC_H_DIR}/00_compile_tpch/dbgen/makefile ${TPC_H_DIR}/00_compile_tpch/TPCH-software-code-3.0.1/dbgen/
  cp ${TPC_H_DIR}/00_compile_tpch/dbgen/tpcd.h ${TPC_H_DIR}/00_compile_tpch/TPCH-software-code-3.0.1/dbgen/
  cd ${TPC_H_DIR}/00_compile_tpch/TPCH-software-code-3.0.1/dbgen
  rm -f ./*.o
  make clean
  ADDITIONAL_CFLAGS_OPTION="-g -Wno-unused-function -Wno-unused-but-set-variable -Wno-format -fcommon" make
  cp ${TPC_H_DIR}/00_compile_tpch/TPCH-software-code-3.0.1/dbgen/dbgen ${TPC_H_DIR}/00_compile_tpch/dbgen/
  cp ${TPC_H_DIR}/00_compile_tpch/TPCH-software-code-3.0.1/dbgen/qgen ${TPC_H_DIR}/00_compile_tpch/dbgen/
  cp ${TPC_H_DIR}/00_compile_tpch/TPCH-software-code-3.0.1/dbgen/dists.dss ${TPC_H_DIR}/00_compile_tpch/dbgen/
  cd ../../
}

function copy_tpc()
{
  cp ${PWD}/dbgen/qgen ../*_sql/queries
  cp ${PWD}/dbgen/qgen ../*_multi_user/queries
  cp ${PWD}/dbgen/dbgen ../*_gen_data/
  cp ${PWD}/dbgen/dists.dss ../*_sql/queries
  cp ${PWD}/dbgen/dists.dss ../*_multi_user/queries
  cp ${PWD}/dbgen/dists.dss ../*_gen_data/
}

function copy_queries()
{
  rm -rf ${TPC_H_DIR}/*_sql/queries
  rm -rf ${TPC_H_DIR}/*_multi_user/queries
  cp -R ${PWD}/dbgen/queries ${TPC_H_DIR}/*_sql/
  cp -R ${PWD}/dbgen/queries ${TPC_H_DIR}/*_multi_user/
}

function check_binary() {
  set +e
  
  cd ${PWD}/dbgen/
  cp -f dbgen.${CHIP_TYPE} dbgen
  cp -f qgen.${CHIP_TYPE} qgen
  chmod +x dbgen
  chmod +x qgen
  if [ "${LOG_DEBUG}" == "true" ]; then
    ./dbgen -h
  else
    ./dbgen -h > /dev/null 2>&1
  fi
  if [ $? == 1 ]; then 
    if [ "${LOG_DEBUG}" == "true" ]; then
      ./qgen -h
    else
      ./qgen -h > /dev/null 2>&1
    fi
    if [ $? == 0 ]; then
      compile_flag="false" 
    fi
  fi
  cd ..
  set -e
}

function check_chip_type() {
  # Get system architecture information
  ARCH=$(uname -m)

  # Determine the architecture type and assign to variable
  if [[ $ARCH == *"x86"* || $ARCH == *"i386"* || $ARCH == *"i686"* ]]; then
    export CHIP_TYPE="x86"
  elif [[ $ARCH == *"arm"* || $ARCH == *"aarch64"* ]]; then
    export CHIP_TYPE="arm"
  else
    export CHIP_TYPE="unknown"
  fi

  # Print the result for verification
  log_time "Chip type: $CHIP_TYPE"
}

check_chip_type
check_binary

if [ "${compile_flag}" == "true" ]; then
  make_tpc
else
  log_time "Binary works, no compiling needed."   
fi

print_log

log_time "Step ${step} finished"
printf "\n"