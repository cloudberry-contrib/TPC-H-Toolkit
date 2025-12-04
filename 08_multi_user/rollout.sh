#!/bin/bash

set -e

PWD=$(get_pwd ${BASH_SOURCE[0]})
CurrentPath=$(get_pwd ${BASH_SOURCE[0]})

step="multi_user"

log_time "Step ${step} started"

rm -rf ${TPC_H_DIR}/*_multi_user/queries
cp -R ${TPC_H_DIR}/00_compile_tpch/dbgen/queries ${TPC_H_DIR}/*_multi_user/
cp ${TPC_H_DIR}/00_compile_tpch/dbgen/qgen ${TPC_H_DIR}/*_multi_user/queries
cp ${TPC_H_DIR}/00_compile_tpch/dbgen/dists.dss ${TPC_H_DIR}/*_multi_user/queries

if [ "${DB_CURRENT_USER}" != "${BENCH_ROLE}" ]; then
  GrantSchemaPrivileges="GRANT ALL PRIVILEGES ON SCHEMA ${DB_SCHEMA_NAME} TO ${BENCH_ROLE}"
  GrantTablePrivileges="GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${DB_SCHEMA_NAME} TO ${BENCH_ROLE}"
  if [ "${LOG_DEBUG}" == "true" ]; then
    log_time "Grant schema privileges to role ${BENCH_ROLE}"
  fi
  psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=0 -q -P pager=off -c "${GrantSchemaPrivileges}"
  if [ "${LOG_DEBUG}" == "true" ]; then
    log_time "Grant table privileges to role ${BENCH_ROLE}"
  fi
  psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=0 -q -P pager=off -c "${GrantTablePrivileges}"
fi

# define data loding log file
LOG_FILE="${TPC_H_DIR}/log/rollout_load.log"

# Handle RNGSEED configuration
if [ "${UNIFY_QGEN_SEED}" == "true" ]; then
  # Use a fixed RNGSEED when unified seed is enabled
  RNGSEED=2016032410
else 
  # Get RNGSEED from log file or use default
  if [[ -f "$LOG_FILE" ]]; then
    RNGSEED=$(tail -n 1 "$LOG_FILE" | cut -d '|' -f 6)
  else
    RNGSEED=2016032410
  fi
fi

if [ "${MULTI_USER_COUNT}" -eq "0" ]; then
  log_time "MULTI_USER_COUNT set at 0 so exiting..."
  exit 0
fi

function get_psql_count()
{
	psql_count=$(ps -ef | grep psql | grep multi_user | grep -v grep | wc -l)
}

function get_running_jobs_count() {
  job_count=$(ps -fu "${ADMIN_USER}" |grep -v grep |grep -i "${TPC_H_DIR}/08_multi_user/test.sh"|wc -l || true)
  echo "${job_count}"
}

function get_file_count()
{
  file_count=$(find ${TPC_H_DIR}/log -maxdepth 1 -name 'end_testing*' | grep -c . || true)
  echo "${file_count}"
}


rm -f ${TPC_H_DIR}/log/end_testing_*.log
rm -f ${TPC_H_DIR}/log/testing*.log
rm -f ${TPC_H_DIR}/log/rollout_testing_*.log
rm -f ${TPC_H_DIR}/log/*multi.explain_analyze.log

function generate_templates()
{
  if [ "${LOG_DEBUG}" == "true" ]; then
    log_time "rm -f ${PWD}/query_*.sql"
  fi
  rm -f ${PWD}/query_*.sql
  #create each user's directory
  sql_dir=${PWD}
  if [ "${LOG_DEBUG}" == "true" ]; then
    log_time "sql_dir: ${sql_dir}"
  fi
  for i in $(seq 1 ${MULTI_USER_COUNT}); do
    sql_dir="${PWD}/${i}"
    if [ "${LOG_DEBUG}" == "true" ]; then
      log_time "checking for directory ${sql_dir}"
    fi
    if [ ! -d "${sql_dir}" ]; then
      if [ "${LOG_DEBUG}" == "true" ]; then
        log_time "mkdir ${sql_dir}"
      fi
      mkdir ${sql_dir}
    fi
    if [ "${LOG_DEBUG}" == "true" ]; then
      log_time "rm -f ${sql_dir}/*.sql"
    fi
    rm -f ${sql_dir}/*.sql
  done
  #Create queries
  if [ "${LOG_DEBUG}" == "true" ]; then
    log_time "cd ${PWD}/queries"
  fi
  cd ${PWD}/queries
  
  for i in $(seq 1 $MULTI_USER_COUNT); do
    if [ "${LOG_DEBUG}" == "true" ]; then
      log_time "./qgen -d -r ${RNGSEED} -s ${GEN_DATA_SCALE} -p $i -c -v > $CurrentPath/query_$i.sql"
    fi
    ${PWD}/qgen -d -r ${RNGSEED} -s ${GEN_DATA_SCALE} -p $i -c -v > $CurrentPath/query_$i.sql &
  done
  wait

  cd ..
  #move the query_x.sql file to the correct session directory
  for i in ${PWD}/query_*.sql; do
    stream_number=$(basename ${i} | awk -F '.' '{print $1}' | awk -F '_' '{print $2}')
	#going from base 0 to base 1
	if [ "${LOG_DEBUG}" == "true" ]; then
	  log_time "stream_number: ${stream_number}"
	fi
	sql_dir=${PWD}/${stream_number}
	if [ "${LOG_DEBUG}" == "true" ]; then
	  log_time "mv ${i} ${sql_dir}/"
	fi
	mv ${i} ${sql_dir}/
  done
}

if [ "${RUN_MULTI_USER_QGEN}" = "true" ]; then
  if [ "${LOG_DEBUG}" == "true" ]; then
    log_time "Generating query templates for ${MULTI_USER_COUNT} users."
  fi
  generate_templates
  if [ "${LOG_DEBUG}" == "true" ]; then
    log_time "Completed query templates generation for ${MULTI_USER_COUNT} users."
  fi
fi
log_time "Starting ${MULTI_USER_COUNT} Troughput test."
SECONDS=0
for session_id in $(seq 1 ${MULTI_USER_COUNT}); do
  session_log=${TPC_H_DIR}/log/testing_session_${session_id}.log
  if [ "${LOG_DEBUG}" == "true" ]; then
    log_time "${PWD}/test.sh ${session_id}"
  fi
  ${PWD}/test.sh ${session_id} &> ${session_log} &
done

#sleep 60
log_time "Now executing ${MULTI_USER_COUNT} multi-users queries. This may take a while."
ELAPSED=0
echo -n "Multi-user query duration: "
running_jobs_count=${MULTI_USER_COUNT}
while [ ${running_jobs_count} -gt 0 ]; do
  printf "\rMulti-user query duration: ${ELAPSED} second(s)"
  # Record start time before calling the function
  start_time=$(date +%s)
  sleep 15
  running_jobs_count=$(get_running_jobs_count)
  # Calculate function execution time and add to total elapsed time
  end_time=$(date +%s)
  function_duration=$((end_time - start_time))
  ELAPSED=$((ELAPSED + function_duration))
done

echo ""
log_time "Multi-user queries completed."

file_count=$(get_file_count)

if [ "${file_count}" -ne "${MULTI_USER_COUNT}" ]; then
	log_time "The number of successfully completed sessions is less than expected!"
	log_time "Please review the log files to determine which queries failed."
	exit 1
fi

rm -f ${TPC_H_DIR}/log/end_testing_*.log # remove the counter log file if successful.

log_time "Step ${step} finished"
printf "\n"