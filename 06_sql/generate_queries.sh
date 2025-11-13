#!/bin/bash

PWD=$(get_pwd ${BASH_SOURCE[0]})
set -e

query_id=1

if [ "${GEN_DATA_SCALE}" == "" ] || [ "${BENCH_ROLE}" == "" ]; then
	echo "Usage: generate_queries.sh scale rolename"
	echo "Example: ./generate_queries.sh 100 hbench"
	echo "This creates queries for 100GB of data."
	exit 1
fi

# Define data loading log file
LOG_FILE="${TPC_DS_DIR}/log/rollout_load.log"

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

# Clean up previous SQL files
rm -f ${TPC_H_DIR}/06_sql/*.${BENCH_ROLE}.*.sql*

for i in $(ls $PWD/queries/*.sql |  xargs -n 1 basename); do
	q=$(echo $i | awk -F '.' '{print $1}')
	id=$(printf %02d $q)
	file_id="1""$id"
	filename=${file_id}.${BENCH_ROLE}.${id}.sql

	echo "echo \":EXPLAIN_ANALYZE\" > ${TPC_H_DIR}/06_sql/$filename"

	printf "set role ${BENCH_ROLE};\nset search_path=${DB_SCHEMA_NAME},public;\n" > ${TPC_H_DIR}/06_sql/${filename}

	for o in $(cat ${TPC_H_DIR}/01_gen_data/optimizer.txt); do
        q2=$(echo ${o} | awk -F '|' '{print $1}')
        if [ "${id}" == "${q2}" ]; then
          optimizer=$(echo ${o} | awk -F '|' '{print $2}')
        fi
    done
	printf "set optimizer=${optimizer};\n" >> ${TPC_H_DIR}/06_sql/${filename}
	printf "set statement_mem=\"${STATEMENT_MEM}\";\n" >> ${TPC_H_DIR}/06_sql/${filename}

	if [ "${ENABLE_VECTORIZATION}" = "on" ]; then
	  printf "set vector.enable_vectorization=${ENABLE_VECTORIZATION};\n" >> ${TPC_H_DIR}/06_sql/${filename}
    fi
	
	printf ":EXPLAIN_ANALYZE\n" >> ${TPC_H_DIR}/06_sql/${filename}

	# Check database if postgresql then comment out optimizer settings
	if [ "${DB_VERSION}" == "postgresql" ]; then
      sed -i 's/^set optimizer=.*/-- &/' "${TPC_H_DIR}/06_sql/${filename}"
      sed -i 's/^set statement_mem=.*/-- &/' "${TPC_H_DIR}/06_sql/${filename}"
    fi
	
	cd ${TPC_H_DIR}/06_sql/queries
	log_time "./qgen -d -r ${RNGSEED} -s ${GEN_DATA_SCALE} $q >> ${TPC_H_DIR}/06_sql/$filename"
	./qgen -d -r ${RNGSEED} -s ${GEN_DATA_SCALE} $q >> ${TPC_H_DIR}/06_sql/$filename
	cd ..
done

log_time "COMPLETE: qgen scale ${GEN_DATA_SCALE}"
