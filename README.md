# TPC-H Benchmark Toolkit for Cloudberry Database

[![TPC-H](https://img.shields.io/badge/TPC--H-v3.0.1-blue)](https://www.tpc.org/tpch/default5.asp)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

A comprehensive tool for running TPC-H benchmarks on Cloudberry Database. This implementation automates the execution of the TPC-H benchmark suite, including data generation, schema creation, data loading, and query execution.

## Overview

This tool provides:
- Automated TPC-H benchmark execution
- Support for both local and cloud deployments
- Configurable data generation (1GB to 100TB+)
- Customizable query execution parameters
- Detailed performance reporting

## Table of Contents
- [Decision Support Benchmark for Cloudberry Database](#decision-support-benchmark-for-cloudberry-database)
  - [Overview](#overview)
  - [Table of Contents](#table-of-contents)
  - [Quick Start](#quick-start)
  - [Supported TPC-H Versions](#supported-tpc-h-versions)
  - [Prerequisites](#prerequisites)
    - [Tested Products](#tested-products)
    - [Local Cluster Setup](#local-cluster-setup)
    - [Remote Client Setup](#remote-client-setup)
    - [TPC-H Tools Dependencies](#tpc-h-tools-dependencies)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Configuration](#configuration)
    - [Environment Options](#environment-options)
    - [Benchmark Options](#benchmark-options)
    - [Storage Options](#storage-options)
    - [Step Control Options](#step-control-options)
    - [Miscellaneous Options](#miscellaneous-options)
  - [Performance Tuning](#performance-tuning)
  - [Benchmark Modifications](#benchmark-modifications)
  - [Troubleshooting](#troubleshooting)
    - [Common Issues and Solutions](#common-issues-and-solutions)
    - [Logs and Diagnostics](#logs-and-diagnostics)
  - [Contributing](#contributing)
  - [License](#license)

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/cloudberry-contrib/TPC-H-Toolkit.git
cd TPC-H-Toolkit

# 2. Configure your environment
vim tpch_variables.sh

# 3. Run the benchmark
./run.sh
```

### Local Mode Guide
For running tests on MPP Architecture (Cloudberry / Greenplum / HashData Lightning):

1. Set `RUN_MODEL="local"` in `tpch_variables.sh`
2. Configure database connection parameters
3. Run with `./tpch.sh`

### Cloud Mode Guide
For running tests on PostgreSQL compatible databases (Hashdata Enterprise, SynxDB Elastic):

1. Set `RUN_MODEL="cloud"` in `tpch_variables.sh`
2. Configure `PSQL_OPTIONS`, `CLIENT_GEN_PATH`, and `CLIENT_GEN_PARALLEL`
3. Run with `./tpch.sh`

## Supported TPC-H Versions

| TPC-H Benchmark Version | Published Date | Standard Specification |
|-|-|-|
| 3.0.1 (latest) | 2021/02/18 | [PDF](https://tpc.org/TPC_Documents_Current_Versions/pdf/tpc-h_v3.0.0.pdf)|
| 2.18.0 | 2017/06/12 | [PDF](https://tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.18.0.pdf)|

This tool uses TPC-H 3.0.1 as the default benchmark specification.

## Prerequisites

### Tested Products
- HashData Enterprise / HashData Lightning
- Greenplum 4.x / Greenplum 5.x / Greenplum 6.x / Greenplum 7.x
- Cloudberry Database 1.x / 2.x
- PostgreSQL 12+ (limited support)

### Local Cluster Setup
For running tests directly on the coordinator host:

1. Set `RUN_MODEL="local"` in `tpch_variables.sh`
2. Ensure you have a running Cloudberry Database with `gpadmin` access
3. Create `gpadmin` database
4. Configure password-less `ssh` between `mdw` (coordinator) and segment nodes (`sdw1..n`)

### Remote Client Setup
For running tests from a remote client:

1. Set `RUN_MODEL="cloud"` in `tpch_variables.sh`
2. Install `psql` client with passwordless access (`.pgpass`)
3. Configure required variables:
   ```bash
   export RANDOM_DISTRIBUTION="true"
   export TABLE_STORAGE_OPTIONS="compresstype=zstd, compresslevel=5"
   export CLIENT_GEN_PATH="/tmp/dsbenchmark"
   export CLIENT_GEN_PARALLEL="2"
   ```

### TPC-H Tools Dependencies
Install the dependencies on `mdw` for compiling the `dbgen` (data generation) and `qgen` (query generation) tools:

```bash
ssh root@mdw
yum -y install gcc make
```

The TPC-H 3.0.1 source code is included in the repository as `TPCH-software-code-3.0.1.zip` in the 00_compile_tpch directory. Original source can also be downloaded from the [TPC website](http://tpc.org/tpc_documents_current_versions/current_specifications5.asp).

## Installation

Clone the repository with Git:

```bash
ssh gpadmin@mdw
git clone https://github.com/cloudberry-contrib/TPC-H-Toolkit.git
```

Place the folder under `/home/gpadmin/` and set ownership:

```bash
chown -R gpadmin:gpadmin TPC-H-Toolkit
```

## Usage

To run the benchmark interactively:

```bash
ssh gpadmin@mdw
cd ~/TPC-H-Toolkit
./tpch.sh
```

To run in the background with logging:

```bash
nohup ./run.sh > tpch_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

By default, this will run a scale 1 (1GB) benchmark with 2 concurrent users, executing all steps from data generation through reporting.

## Configuration

The benchmark is controlled through the `tpch_variables.sh` file with the following key sections:

### Environment Options
```bash
# Core settings
export ADMIN_USER="gpadmin"
export BENCH_ROLE="hbench"
export DB_SCHEMA_NAME="tpch"

export RUN_MODEL="local"  # "local" or "cloud"

export PSQL_OPTIONS="-h hostname -p 5432"  # Database connection options

# Cloud mode settings
export CLIENT_GEN_PATH="/tmp/dsbenchmark"  # Location for data generation
export CLIENT_GEN_PARALLEL="2"             # Parallel data generation processes

# Local mode settings
export LOCAL_GEN_PARALLEL="1"  # Parallel processes per segment
```

### Benchmark Options
```bash
# Scale and concurrency
export GEN_DATA_SCALE="1"      # Data scale factor (1 = 1GB)
export MULTI_USER_COUNT="2"    # Number of concurrent users
```

For large scale tests, recommended configurations:
- 3TB: `GEN_DATA_SCALE="3000"` with `MULTI_USER_COUNT="5"`
- 10TB: `GEN_DATA_SCALE="10000"` with `MULTI_USER_COUNT="7"`
- 30TB: `GEN_DATA_SCALE="30000"` with `MULTI_USER_COUNT="10"`

### Storage Options
```bash
# Table storage format
export TABLE_ACCESS_METHOD="ao_column"  # Options: heap/ao_row/ao_column/pax

export TABLE_STORAGE_OPTIONS="WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=5, blocksize=1048576)"

export TABLE_USE_PARTITION="false"  # Enable partitioning for large tables
```

Table distribution keys are defined in `03_ddl/distribution.txt`. You can modify tables' distribution keys by changing this file, setting distribution method to hash with column names or "REPLICATED".

### Step Control Options
```bash
# Setup and compilation
export RUN_COMPILE_TPCH="true"  # Compile data/query generators
export RUN_INIT="true"         # Initialize cluster settings

# Data generation and loading
export RUN_GEN_DATA="true"      # Generate test data
export GEN_NEW_DATA="true"      # Generate new data (when RUN_GEN_DATA=true)
export RUN_DDL="true"           # Create database schemas/tables
export DROP_EXISTING_TABLES="true"  # Drop existing tables (when RUN_DDL=true)
export RUN_LOAD="true"          # Load generated data
export RUN_ANALYZE="true"       # Analyze tables after loading

# Query execution
export RUN_SQL="true"                 # Run power test queries
export RUN_QGEN="true"                # Generate query streams
export RUN_SINGLE_USER_REPORTS="true" # Generate single-user reports

export RUN_MULTI_USER="false"         # Run throughput test queries
export RUN_MULTI_USER_QGEN="true"     # Generate multi-user queries
export RUN_MULTI_USER_REPORTS="false" # Generate multi-user reports
export RUN_SCORE="false"              # Compute final benchmark score
```

### Miscellaneous Options
```bash
export SINGLE_USER_ITERATIONS="1"      # Number of power test iterations
export EXPLAIN_ANALYZE="false"         # Enable query plan analysis
export ENABLE_VECTORIZATION="off"      # Enable vectorized execution
export RANDOM_DISTRIBUTION="false"     # Use random distribution for fact tables
export STATEMENT_MEM="1.9GB"           # Memory per statement (single-user)
export STATEMENT_MEM_MULTI_USER="1GB"  # Memory per statement (multi-user)
export GPFDIST_LOCATION="p"            # gpfdist location (p=primary, m=mirror)
```

## Performance Tuning

For optimal benchmark performance, consider these recommendations:

### Memory Settings
```bash
# For systems with 100GB+ RAM
export STATEMENT_MEM="8GB"
export STATEMENT_MEM_MULTI_USER="4GB"
```

### Storage Optimization
```bash
# Enable columnar storage with compression
export TABLE_ACCESS_METHOD="ao_column"
export TABLE_STORAGE_OPTIONS="WITH (compresstype=zstd, compresslevel=9)"
export TABLE_USE_PARTITION="true"
```

### Concurrency Tuning
```bash
# Adjust based on available CPU cores
export CLIENT_GEN_PARALLEL="$(nproc)"
export MULTI_USER_COUNT="$(( $(nproc) / 2 ))"
```

### Enable Vectorization
For supported systems (Lightning 1.5.3+):
```bash
export ENABLE_VECTORIZATION="on"
```

### Optimizer Settings (for supported systems)

```bash
# Adjust optimizer settings in 01_gen_data/optimizer.txt
# Turn ORCA on/off for each queries by setting in this file
# After changing the settings, make sure to run the QGEN to generate the queries with the new settings.
```

## Benchmark Modifications

The following modifications were made to the standard TPC-H queries for compatibility:

1. **Query 15**: Using alternative version for easier parsing while maintaining performance characteristics
2. **Date Interval Syntax**: Changed date arithmetic to use PostgreSQL-compatible interval syntax
3. **Query 1**: Hard-coded 90-day range due to substitution issues with qgen

## Troubleshooting

### Common Issues and Solutions

1. **Missing Dependencies**
   - Ensure `gcc` and `make` are installed on all nodes
   - Verify password-less SSH between coordinator and segments

2. **Permission Errors**
   - Verify ownership: `chown -R gpadmin:gpadmin /home/gpadmin/TPC-H-Toolkit`
   - Ensure sufficient disk space in data directories

3. **Configuration Validation**
   The script will abort and display any missing or invalid variables in `tpch_variables.sh`

### Logs and Diagnostics
- Main log file: Generated when using `run.sh` with timestamp
- Database server logs: Check PostgreSQL/Greenplum log directory
- Step-specific logs: Located in respective step directories (01_gen_data, 03_ddl, etc.)

## Contributing

We welcome contributions to the TPC-H Toolkit project. Please follow these steps to contribute:

1. Fork the repository
2. Create a new branch for your feature or bug fix
3. Make your changes and ensure they follow the project's coding standards
4. Add tests if applicable
5. Submit a pull request with a clear description of your changes

Before submitting a pull request, please ensure:
- Your code follows the existing style and conventions
- All tests pass
- Documentation is updated if necessary
- You have added yourself to the contributors list if desired

For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

The TPC-H benchmark specification is owned by the Transaction Processing Performance Council (TPC). This toolkit is designed to run the TPC-H benchmark but is not officially endorsed by the TPC.

The TPC-H 3.0.1 database generation tools (dbgen and qgen) are used under the terms of the TPC End User License Agreement. See [00_compile_tpch/EULA.txt](00_compile_tpch/EULA.txt) for details.
