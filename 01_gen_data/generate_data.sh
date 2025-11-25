#!/bin/bash
set -e
# runs on segment host; we don't inherit the functions
PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

GEN_DATA_SCALE=${1}
CHILD=${2}
PARALLEL=${3}
GEN_DATA_PATH=${4}
SINGLE_SEGMENT="0"
DATA_DIRECTORY="${GEN_DATA_PATH}"

echo "GEN_DATA_SCALE: ${GEN_DATA_SCALE}"
echo "CHILD: ${CHILD}"
echo "PARALLEL: ${PARALLEL}"
echo "GEN_DATA_PATH: ${GEN_DATA_PATH}"

if [[ ! -d "${DATA_DIRECTORY}" && ! -L "${DATA_DIRECTORY}" ]]; then
	echo "mkdir ${DATA_DIRECTORY}"
	mkdir ${DATA_DIRECTORY}
fi

rm -f ${DATA_DIRECTORY}/*

#for single nodes, you might only have a single segment but dbgen requires at least 2
if [ "$PARALLEL" -eq "1" ]; then
	PARALLEL="2"
	SINGLE_SEGMENT="1"
fi

cp ${PWD}/dbgen ${PWD}/dists.dss ${DATA_DIRECTORY}

cd ${PWD}
cd $DATA_DIRECTORY
# ${PWD}/dbgen -scale ${GEN_DATA_SCALE} -dir ${DATA_DIRECTORY} -parallel ${PARALLEL} -child ${CHILD} -terminate n
${DATA_DIRECTORY}/dbgen -s $GEN_DATA_SCALE -C $PARALLEL -S $CHILD -v

if [ "$CHILD" -eq "1" ]; then
	mv $DATA_DIRECTORY/nation.tbl $DATA_DIRECTORY/nation.tbl.${CHILD}
	mv $DATA_DIRECTORY/region.tbl $DATA_DIRECTORY/region.tbl.${CHILD}
fi

if [ "$CHILD" -gt "1" ]; then
	rm -f $DATA_DIRECTORY/nation.tbl
	rm -f $DATA_DIRECTORY/region.tbl
	touch $DATA_DIRECTORY/nation.tbl.${CHILD}
	touch $DATA_DIRECTORY/region.tbl.${CHILD}
fi

# make sure there is a file in each directory so that gpfdist doesn't throw an error
cd ${PWD}
declare -a tables=("supplier" "region" "part" "partsupp" "customer" "orders" "nation" "lineitem")

for i in "${tables[@]}"; do
	filename="${DATA_DIRECTORY}/${i}.tbl.${CHILD}"
	echo ${filename}
	if [ ! -f ${filename} ]; then
		touch ${filename}
	fi
done

#for single nodes, you might only have a single segment but dbgen requires at least 2
if [ "$SINGLE_SEGMENT" -eq "1" ]; then
	CHILD="2"
	#build the second list of files
	# ${PWD}/dbgen -scale ${GEN_DATA_SCALE} -dir ${DATA_DIRECTORY} -parallel ${PARALLEL} -child ${CHILD} -terminate n
    cd $DATA_DIRECTORY
	$PWD/dbgen -s $GEN_DATA_SCALE -C $PARALLEL -S $CHILD -f
	cd ${PWD}
	rm -f $DATA_DIRECTORY/nation.tbl
	rm -f $DATA_DIRECTORY/region.tbl
	# make sure there is a file in each directory so that gpfdist doesn't throw an error
	declare -a tables=("supplier" "region" "part" "partsupp" "customer" "orders" "nation" "lineitem")

	for i in "${tables[@]}"; do
		filename="${DATA_DIRECTORY}/${i}.tbl.${CHILD}"
		echo ${filename}
		if [ ! -f ${filename} ]; then
			touch ${filename}
		fi
	done
fi
