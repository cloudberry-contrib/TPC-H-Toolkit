#!/bin/bash

set -e

PWD=$(get_pwd ${BASH_SOURCE[0]})
CurrentPath=$(get_pwd ${BASH_SOURCE[0]})

step="multi_user"

log_time "Step ${step} started"
printf "\n"

if [ "${DB_CURRENT_USER}" != "${BENCH_ROLE}" ]; then
  GrantSchemaPrivileges="GRANT ALL PRIVILEGES ON SCHEMA ${DB_SCHEMA_NAME} TO ${BENCH_ROLE}"
  GrantTablePrivileges="GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${DB_SCHEMA_NAME} TO ${BENCH_ROLE}"
  log_time "Grant schema privileges to role ${BENCH_ROLE}"
  psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=0 -q -P pager=off -c "${GrantSchemaPrivileges}"
  log_time "Grant table privileges to role ${BENCH_ROLE}"
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
	echo "MULTI_USER_COUNT set at 0 so exiting..."
	exit 0
fi

function get_psql_count()
{
	psql_count=$(ps -ef | grep psql | grep multi_user | grep -v grep | wc -l)
}

function get_running_jobs_count() {
  job_count=$(ps -fu "${ADMIN_USER}" |grep -v grep |grep "07_multi_user/test.sh"|wc -l || true)
  echo "${job_count}"
}

function get_file_count()
{
	file_count=$(ls ${TPC_H_DIR}/log/end_testing* 2> /dev/null | wc -l)
}


rm -f ${TPC_H_DIR}/log/end_testing_*.log
rm -f ${TPC_H_DIR}/log/testing*.log
rm -f ${TPC_H_DIR}/log/rollout_testing_*.log
rm -f ${TPC_H_DIR}/log/*multi.explain_analyze.log

function generate_templates()
{
	rm -f ${PWD}/query_*.sql

	#create each user's directory
	sql_dir=${PWD}
	echo "sql_dir: ${sql_dir}"
	for i in $(seq 1 ${MULTI_USER_COUNT}); do
		sql_dir="${PWD}/${i}"
		echo "checking for directory ${sql_dir}"
		if [ ! -d "${sql_dir}" ]; then
			echo "mkdir ${sql_dir}"
			mkdir ${sql_dir}
		fi
		echo "rm -f ${sql_dir}/*.sql"
		rm -f ${sql_dir}/*.sql
	done

	#Create queries
	echo "cd ${PWD}/queries"
	cd ${PWD}/queries
	
	for i in $(seq 1 $MULTI_USER_COUNT); do
		log_time "rm -f $CurrentPath/*.sql"
		log_time "./qgen -d -r ${RNGSEED} -s ${GEN_DATA_SCALE} -p $i -c -v > $CurrentPath/query_$i.sql"
		${PWD}/qgen -d -r ${RNGSEED} -s ${GEN_DATA_SCALE} -p $i -c -v > $CurrentPath/query_$i.sql
	done
	
	cd ..

	#move the query_x.sql file to the correct session directory
	for i in ${PWD}/query_*.sql; do
		stream_number=$(basename ${i} | awk -F '.' '{print $1}' | awk -F '_' '{print $2}')
		#going from base 0 to base 1
		echo "stream_number: ${stream_number}"
		sql_dir=${PWD}/${stream_number}
		echo "mv ${i} ${sql_dir}/"
		mv ${i} ${sql_dir}/
	done
}

if [ "${RUN_MULTI_USER_QGEN}" = "true" ]; then
  generate_templates
fi

for session_id in $(seq 1 ${MULTI_USER_COUNT}); do
	session_log=${TPC_H_DIR}/log/testing_session_${session_id}.log
	log_time "${PWD}/test.sh ${session_id}"
	${PWD}/test.sh ${session_id} &> ${session_log} &
done

#sleep 60

echo "Now executing queries. This may take a while."
seconds=0
echo -n "Multi-user query duration: "
tput sc
running_jobs_count=$(get_running_jobs_count)
while [ ${running_jobs_count} -gt 0 ]; do
  tput rc
  echo -n "${seconds} second(s)"
  sleep 15
  running_jobs_count=$(get_running_jobs_count)
  seconds=$((seconds + 15))
done

echo ""
echo "done."
echo ""·

get_file_count

if [ "${file_count}" -ne "${MULTI_USER_COUNT}" ]; then
	echo "The number of successfully completed sessions is less than expected!"
	echo "Please review the log files to determine which queries failed."
	exit 1
fi

rm -f ${TPC_H_DIR}/log/end_testing_*.log # remove the counter log file if successful.

echo "Finished ${step}"
log_time "Step ${step} finished"
printf "\n"