#!/bin/bash
set -e

PWD=$(get_pwd ${BASH_SOURCE[0]})

step="score"

log_time "Step ${step} started"
printf "\n"

init_log ${step}

report_schema="${DB_SCHEMA_NAME}_reports"
multi_user_report_schema="${DB_SCHEMA_NAME}_multi_user_reports"

LOAD_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "SELECT ROUND(MAX(end_epoch_seconds) - MIN(start_epoch_seconds)) FROM ${report_schema}.load WHERE tuples > 0")
ANALYZE_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select round(sum(extract('epoch' from duration))) from ${report_schema}.analyze where tuples = -1")
QUERIES_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select round(sum(extract('epoch' from duration))) from (SELECT split_part(description, '.', 2) AS id, min(duration) AS duration FROM ${report_schema}.sql where tuples >= 0 GROUP BY split_part(description, '.', 2)) as sub")
CONCURRENT_QUERY_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select round(sum(extract('epoch' from duration))) from ${multi_user_report_schema}.sql")
THROUGHPUT_ELAPSED_TIME=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select max(end_epoch_seconds) - min(start_epoch_seconds) from ${multi_user_report_schema}.sql")
SUCCESS_QUERY=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select count(*) from ${report_schema}.sql where tuples >= 0")
FAILD_QUERY=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select count(*) from ${report_schema}.sql where tuples < 0 and id > 1")

S_Q=${MULTI_USER_COUNT}
SF=${GEN_DATA_SCALE}
TOTAL_PRICE=1

M_SUCCESS_QUERY=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select count(*) from ${multi_user_report_schema}.sql where tuples >= 0")
M_FAILD_QUERY=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select count(*) from ${multi_user_report_schema}.sql where tuples < 0 and id > 1")



# Remove legacy score calculation sections (v1.3.1 and v2.2.0)

# Add v3.0.1 score calculations per TPC-H specification
# 1. Calculate Power metric (single stream performance)
# Formula: Power@Size = (SF * 3600) / (product of all query and refresh function timing intervals)
# Using approximation since we don't have exact refresh function timing data
POWER=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select cast(${SF} as decimal) * 3600.0 / cast(${QUERIES_TIME} as decimal)")

# 2. Calculate Throughput metric (multi-stream performance)
# Formula: Throughput@Size = (S * 22 * 3600) / Ts * SF
# Where: S = number of query streams, Ts = throughput test elapsed time in seconds
THROUGHPUT=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select cast(${S_Q} as decimal) * 22 * 3600.0 / cast(${THROUGHPUT_ELAPSED_TIME} as decimal) * cast(${SF} as decimal)")

# 3. Calculate composite QphH@Size metric
# Formula: QphH@Size = sqrt(Power@Size * Throughput@Size)
QPHH=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select sqrt(cast(${POWER} as decimal) * cast(${THROUGHPUT} as decimal))")

# 4. Calculate Price/Performance metric
# Formula: $/kQphH@Size = (1000 * Total System Price) / QphH@Size
# Note: TOTAL_PRICE should be set as an environment variable with system cost
PRICE_PER_KQPHH=$(psql ${PSQL_OPTIONS} -v ON_ERROR_STOP=1 -q -t -A -c "select 1000 * cast(${TOTAL_PRICE} as decimal) / cast(${QPHH} as decimal)")

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
