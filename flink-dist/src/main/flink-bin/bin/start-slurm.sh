#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

### This script is basically start-cluster.sh with srun for ssh and some configuration ###

# Start a Flink cluster on Slurm in batch or streaming mode
USAGE="Usage: srun --nodes=1-1 --nodelist=<MASTER> start-slurm.sh [batch|streaming]"

if [[ -z $SLURM_JOB_ID ]]; then
    echo "No Slurm environment detected. $USAGE"
    exit 1
fi

STREAMING_MODE=$1

if [[ -z $STREAMING_MODE ]]; then
    STREAMING_MODE="batch"
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

echo "Starting cluster (${STREAMING_MODE} mode)."

"${FLINK_BIN_DIR}"/jobmanager.sh start cluster ${STREAMING_MODE}

readSlaves

for slave in ${SLAVES[@]}; do
    srun --nodes=1-1 --nodelist=$slave "${FLINK_BIN_DIR}"/taskmanager.sh start ${STREAMING_MODE}
done
