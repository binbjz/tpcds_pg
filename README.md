# TPC-DS is a Decision Support Benchmark

TPC-DS is a decision support benchmark that models several generally applicable aspects of a decision
support system, including queries and data maintenance.

## How to run it

```sh
$ screen -S tpcds_session
$ export PGPASSWORD='pg-password'
$ ( /usr/bin/time psql -U <user_name> -d <db_name> -h <server_ip> -a -f <tpcds_query.sql> ) >tpcds.log 2>&1
```

### or

```sh
$ nohup bash -c "export PGPASSWORD='pg-password'; /usr/bin/time psql -U <user_name> -d <db_name> -h <server_ip> -a -f <tpcds_query.sql>" >tpcds.log 2>&1 &
```

## Install dependency libraries in the virtual environment for the purpose of collecting DB performance metrics

```sh
$ python -m venv metrics_venv
$ cd metrics_venv && source bin/activate
$ pip install asyncpg pandas pyarrow numpy matplotlib psutil loguru
```
