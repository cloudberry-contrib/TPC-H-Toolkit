#!/bin/bash
set -e

PWD=$(get_pwd ${BASH_SOURCE[0]})

step="score"

log_time "Step ${step} started"
printf "\n"

init_log ${step}

report_schema="${DB_SCHEMA_NAME}_reports"
multi_user_report_schema="${DB_SCHEMA_NAME}_multi_user_reports"

# Add error handling to prevent empty variables that could cause SQL syntax errors
LOAD_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "SELECT ROUND(MAX(end_epoch_seconds) - MIN(start_epoch_seconds)) FROM ${report_schema}.load WHERE tuples > 0" 2>/dev/null || echo "0")
ANALYZE_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select round(sum(extract('epoch' from duration))) from ${report_schema}.analyze where tuples = -1" 2>/dev/null || echo "0")
QUERIES_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select round(sum(extract('epoch' from duration))) from (SELECT split_part(description, '.', 2) AS id, min(duration) AS duration FROM ${report_schema}.sql where tuples >= 0 GROUP BY split_part(description, '.', 2)) as sub" 2>/dev/null || echo "0")
CONCURRENT_QUERY_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select round(sum(extract('epoch' from duration))) from ${multi_user_report_schema}.sql" 2>/dev/null || echo "0")
THROUGHPUT_ELAPSED_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select max(end_epoch_seconds) - min(start_epoch_seconds) from ${multi_user_report_schema}.sql" 2>/dev/null || echo "0")
SUCCESS_QUERY=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select count(*) from ${report_schema}.sql where tuples >= 0" 2>/dev/null || echo "0")
FAILD_QUERY=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select count(*) from ${report_schema}.sql where tuples < 0 and id > 1" 2>/dev/null || echo "0")

S_Q=${MULTI_USER_COUNT}
SF=${GEN_DATA_SCALE}
TOTAL_PRICE=1

M_SUCCESS_QUERY=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select count(*) from ${multi_user_report_schema}.sql where tuples >= 0" 2>/dev/null || echo "0")
M_FAILD_QUERY=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select count(*) from ${multi_user_report_schema}.sql where tuples < 0 and id > 1" 2>/dev/null || echo "0")

# Validate that time variables are numeric and not empty
for var in LOAD_TIME ANALYZE_TIME QUERIES_TIME CONCURRENT_QUERY_TIME THROUGHPUT_ELAPSED_TIME; do
  if ! [[ "${!var}" =~ ^[0-9]+$ ]]; then
    echo "Warning: ${var} is not a valid number: '${!var}'. Setting to 0."
    eval "${var}=0"
  fi
done

# Validate that count variables are numeric and not empty
for var in SUCCESS_QUERY FAILD_QUERY M_SUCCESS_QUERY M_FAILD_QUERY; do
  if ! [[ "${!var}" =~ ^[0-9]+$ ]]; then
    echo "Warning: ${var} is not a valid number: '${!var}'. Setting to 0."
    eval "${var}=0"
  fi
done

# Remove legacy score calculation sections (v1.3.1 and v2.2.0)

# Add v3.0.1 score calculations per TPC-H specification
# Validate critical variables to prevent division by zero and SQL errors
if [ "${QUERIES_TIME}" -eq 0 ]; then
  echo "Warning: QUERIES_TIME is 0, setting Power metric to 0"
  POWER=0
else
  # 1. Calculate Power metric (single stream performance)
  # Formula: Power@Size = (SF * 3600) / (product of all query and refresh function timing intervals)
  # Using approximation since we don't have exact refresh function timing data
  POWER=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select cast(${SF} as decimal) * 3600.0 / cast(${QUERIES_TIME} as decimal)" 2>/dev/null || echo "0")
fi

if [ "${THROUGHPUT_ELAPSED_TIME}" -eq 0 ]; then
  echo "Warning: THROUGHPUT_ELAPSED_TIME is 0, setting Throughput metric to 0"
  THROUGHPUT=0
else
  # 2. Calculate Throughput metric (multi-stream performance)
  # Formula: Throughput@Size = (S * 22 * 3600) / Ts * SF
  # Where: S = number of query streams, Ts = throughput test elapsed time in seconds
  THROUGHPUT=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select cast(${S_Q} as decimal) * 22 * 3600.0 / cast(${THROUGHPUT_ELAPSED_TIME} as decimal) * cast(${SF} as decimal)" 2>/dev/null || echo "0")
fi

# 3. Calculate composite QphH@Size metric
# Formula: QphH@Size = sqrt(Power@Size * Throughput@Size)
QPHH=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select sqrt(cast(${POWER} as decimal) * cast(${THROUGHPUT} as decimal))" 2>/dev/null || echo "0")

# 4. Calculate Price/Performance metric
# Formula: $/kQphH@Size = (1000 * Total System Price) / QphH@Size
# Note: TOTAL_PRICE should be set as an environment variable with system cost
if (( $(echo "${QPHH} == 0" | bc -l) )); then
  echo "Warning: QPHH is 0, setting Price/Performance metric to 0"
  PRICE_PER_KQPHH=0
else
  PRICE_PER_KQPHH=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select 1000 * cast(${TOTAL_PRICE} as decimal) / cast(${QPHH} as decimal)" 2>/dev/null || echo "0")
fi

# Validate final calculated values are valid numbers
for var in POWER THROUGHPUT QPHH PRICE_PER_KQPHH; do
  if ! [[ "${!var}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "Warning: ${var} is not a valid number: '${!var}'. Setting to 0."
    eval "${var}=0"
  fi
done

printf "Number of Streams (Sq)\t\t%d\n" "${S_Q}"
printf "Scale Factor (SF)\t\t%d\n" "${SF}"
printf "Load (seconds)\t\t\t%d\n" "${LOAD_TIME}"
printf "Analyze (seconds)\t\t%d\n" "${ANALYZE_TIME}"
printf "1 User Queries (seconds)\t%d\tFor %d success queries and %d failed queries\n" "${QUERIES_TIME}" "${SUCCESS_QUERY}" "${FAILD_QUERY}"
printf "Sum of Elapse Time for all Concurrent Queries (seconds)\t%d\n" "${CONCURRENT_QUERY_TIME}"
printf "Throughput Test Elapsed Time (seconds)\t%d\tFor %d success queries and %d failed queries\n" "${THROUGHPUT_ELAPSED_TIME}" "${M_SUCCESS_QUERY}" "${M_FAILD_QUERY}"

printf "\n"

printf "TPC-H v3.0.1 Performance Metrics\n"
printf "====================================\n"
printf "Data refresh tests are not supported with this toolkit.\n"
printf "This score is a simulated number, don't use for any purpose.\n"
printf "%-20s %10.1f QphH\n" "Power@${SF}GB" ${POWER}
printf "%-20s %10.1f QphH\n" "Throughput@${SF}GB" ${THROUGHPUT}
printf "%-20s %10.1f QphH\n" "QphH@${SF}GB" ${QPHH}
printf "%-20s %10.2f\n" "Price/kQphH@${SF}GB" ${PRICE_PER_KQPHH}
printf "\n"

echo "Finished ${step}"

log_time "Step ${step} finished"
printf "\n"
