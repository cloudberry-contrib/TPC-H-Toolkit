#!/bin/bash
set -e

PWD=$(get_pwd ${BASH_SOURCE[0]})

if [ "${GEN_DATA_SCALE}" == "" ]; then
  echo "You must provide the scale as a parameter in terms of Gigabytes."
  echo "Example: ./rollout.sh 100"
  echo "This will create 100 GB of data for this test."
  exit 1
fi

function get_count_generate_data() {
  count="0"
  while read -r i; do
    next_count=$(ssh -o ConnectTimeout=0 -o LogLevel=quiet -n -f ${i} "bash -c 'ps -ef | grep generate_data.sh | grep -v grep | wc -l'" 2>&1 || true)
    check="^[0-9]+$"
    if ! [[ ${next_count} =~ ${check} ]] ; then
      next_count="1"
    fi
    count=$((count + next_count))
  done < ${TPC_H_DIR}/segment_hosts.txt
}

function kill_orphaned_data_gen() {
  echo "kill any orphaned dbgen processes on segment hosts"
  # always return true even if no processes were killed
  for i in $(cat ${TPC_H_DIR}/segment_hosts.txt); do
    ssh ${i} "pkill dbgen" || true &
  done
  wait
}

function copy_generate_data() {
  echo "copy generate_data.sh to segment hosts"
  for i in $(cat ${TPC_H_DIR}/segment_hosts.txt); do
    scp ${PWD}/generate_data.sh ${i}: &
  done
  wait
}

function gen_data() {
  get_version
  PARALLEL=$(gpstate | grep "Total primary segments" | awk -F '=' '{print $2}')
  if [ "${PARALLEL}" == "" ]; then
    echo "ERROR: Unable to determine how many primary segments are in the cluster using gpstate."
    exit 1
  fi
  echo "parallel: $PARALLEL"
  
  if [ "${VERSION}" == "gpdb_4_3" ] || [ "${VERSION}" == "gpdb_5" ]; then
    SQL_QUERY="select row_number() over(), g.hostname, p.fselocation as path from gp_segment_configuration g join pg_filespace_entry p on g.dbid = p.fsedbid join pg_tablespace t on t.spcfsoid = p.fsefsoid where g.content >= 0 and g.role = '${GPFDIST_LOCATION}' and t.spcname = 'pg_default' order by 1, 2, 3"
  else
    SQL_QUERY="select row_number() over(), g.hostname, g.datadir from gp_segment_configuration g where g.content >= 0 and g.role = '${GPFDIST_LOCATION}' order by 1, 2, 3"
  fi
  for i in $(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -A -t -c "${SQL_QUERY}"); do
    CHILD=$(echo ${i} | awk -F '|' '{print $1}')
    EXT_HOST=$(echo ${i} | awk -F '|' '{print $2}')
    GEN_DATA_PATH=$(echo ${i} | awk -F '|' '{print $3}' | sed 's#//#/#g')
    GEN_DATA_PATH="${GEN_DATA_PATH}/hbenchmark"
    echo "ssh -n ${EXT_HOST} \"bash -c 'cd ~/; ./generate_data.sh ${GEN_DATA_SCALE} ${CHILD} ${PARALLEL} ${GEN_DATA_PATH} > /tmp/tpch.generate_data.${CHILD}.log 2>&1 &'\""

    ssh -n ${EXT_HOST} "bash -c 'cd ~/; ./generate_data.sh ${GEN_DATA_SCALE} ${CHILD} ${PARALLEL} ${GEN_DATA_PATH} > /tmp/tpch.generate_data.${CHILD}.log 2>&1 &'" &
  done
  wait
}

step="gen_data"

log_time "Step ${step} started"
printf "\n"

init_log ${step}
start_log
schema_name=${DB_VERSION}
table_name="gen_data"

if [ "${GEN_NEW_DATA}" == "true" ]; then
  if [ "${RUN_MODEL}" != "local" ]; then
    PARALLEL=${CLIENT_GEN_PARALLEL}

    IFS=' ' read -ra GEN_PATHS <<< "${CLIENT_GEN_PATH}"
    TOTAL_PATHS=${#GEN_PATHS[@]}
    if [ ${TOTAL_PATHS} -eq 0 ]; then
      log_time "ERROR: CLIENT_GEN_PATH is empty or not set"
      exit 1
    fi
    log_time "Number of data generation paths: ${TOTAL_PATHS}"
    log_time "Parallel processes per path: ${PARALLEL}"
    log_time "Total parallel processes: $((TOTAL_PATHS * PARALLEL))"      

    # Prepare each data generation path
    for GEN_DATA_PATH in "${GEN_PATHS[@]}"; do
      if [[ ! -d "${GEN_DATA_PATH}" && ! -L "${GEN_DATA_PATH}" ]]; then
        log_time "mkdir ${GEN_DATA_PATH}"
        mkdir -p ${GEN_DATA_PATH}
      fi
      log_time "rm -rf ${GEN_DATA_PATH}/*"
      rm -rf ${GEN_DATA_PATH}/*
      log_time "mkdir -p ${GEN_DATA_PATH}/logs"
      mkdir -p ${GEN_DATA_PATH}/logs
    done

    # Start data generation processes for each path
    TOTAL_PARALLEL=$((TOTAL_PATHS * PARALLEL))
    
    if [ "$TOTAL_PARALLEL" -eq "1" ]; then
	    PARALLEL="2"
	    TOTAL_PARALLEL=$((TOTAL_PATHS * PARALLEL))
    fi

    CHILD=1    
    for GEN_DATA_PATH in "${GEN_PATHS[@]}"; do
      # Save the starting CHILD number for current path
      CURRENT_START_CHILD=${CHILD}
      PATH_CHILD=1
      while [ ${PATH_CHILD} -le ${PARALLEL} ]; do
        mkdir -p ${GEN_DATA_PATH}/${CHILD}
        cp ${PWD}/dbgen ${PWD}/dists.dss ${GEN_DATA_PATH}/${CHILD}/
        cd ${GEN_DATA_PATH}/${CHILD}/
        log_time "${GEN_DATA_PATH}/${CHILD}/dbgen -f -s ${GEN_DATA_SCALE} -C ${TOTAL_PARALLEL} -S ${CHILD} > ${GEN_DATA_PATH}/logs/tpch.generate_data.${CHILD}.log 2>&1 &"
        ${GEN_DATA_PATH}/${CHILD}/dbgen -f -s ${GEN_DATA_SCALE} -C ${TOTAL_PARALLEL} -S ${CHILD} > ${GEN_DATA_PATH}/logs/tpch.generate_data.${CHILD}.log 2>&1 &
        PATH_CHILD=$((PATH_CHILD + 1))
        CHILD=$((CHILD + 1))
      done
    done

    log_time "Waiting for data generation processes to complete..."
    wait
    
    #Adjust data files to remove duplicate data for region and nation
    CHILD=1
    for GEN_DATA_PATH in "${GEN_PATHS[@]}"; do
      PATH_CHILD=1
      while [ ${PATH_CHILD} -le ${PARALLEL} ]; do  
        if [ "$CHILD" -eq "1" ]; then
          mv ${GEN_DATA_PATH}/${CHILD}/nation.tbl ${GEN_DATA_PATH}/${CHILD}/nation.tbl.${CHILD}
          mv ${GEN_DATA_PATH}/${CHILD}/region.tbl ${GEN_DATA_PATH}/${CHILD}/region.tbl.${CHILD}
        fi
        if [ "$CHILD" -gt "1" ]; then
          rm -f ${GEN_DATA_PATH}/${CHILD}/nation.tbl
          rm -f ${GEN_DATA_PATH}/${CHILD}/region.tbl
          touch ${GEN_DATA_PATH}/${CHILD}/nation.tbl.${CHILD}
          touch ${GEN_DATA_PATH}/${CHILD}/region.tbl.${CHILD}
        fi
        PATH_CHILD=$((PATH_CHILD + 1))
        CHILD=$((CHILD + 1))
      done
    done
  else
    kill_orphaned_data_gen
    copy_generate_data
    gen_data
    echo "Current database running this test is ${VERSION}"
    echo ""
    get_count_generate_data
    echo "Now generating data.  This may take a while."
    seconds=0
    echo -ne "Generating data duration: "
    tput sc
    while [ "$count" -gt "0" ]; do
      tput rc
      echo -ne "${seconds} second(s)"
      sleep 5
      seconds=$(( seconds + 5 ))
      get_count_generate_data
    done
  fi
    
  echo ""
  log_time "Done generating data"
  echo ""
fi

print_log

echo "Finished ${step}"
log_time "Step ${step} finished"
printf "\n"
