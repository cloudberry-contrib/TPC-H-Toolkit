#!/bin/bash
set -e

GEN_PATH_NAME=${1}

for i in $(ps -ef | grep gpfdist | grep -i "${GEN_PATH_NAME}" | grep -v grep | grep -v stop_gpfdist | awk -F ' ' '{print $2}'); do
  #echo "killing ${i}"
  kill ${i}
done
