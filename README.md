# About
This benchmark is intended to provide an easy and repeatable way to measure the C# C* driver performance.
The benchmark is inspired by a production use case where we persist time series (metrics) all the time and read them all once per day for statistics purposes.

The code is meant to be as simple as possible but we still tried to make it as fast as possible, so any comment / improvement is welcome.


# Benchmark settings
## Cluster
 - 12 nodes with SSD / 32Gb RAM / Xeon (4 cores + HT / 8M @ 3.4GHz)
 - Cassandra 2.0.15

## Write settings
 - Prepared statements
 - 5,000 insert statements in parallel
 - 5,000 time series with 18,000 data points each
 - CL: Local_One

## Read settings
 - Prepared statements
 - 300 read statements in parallel
 - Read all time series
 - No paging
 - CL: Local_One

# Benchmark results

|        | Reads                        | Writes                    |
|--------|------------------------------|---------------------------|
| v2.7.1 | 00:01:21 (1,103,682 point/s) | 01:38:51 (15,172 point/s) |
| v2.7.2 | 00:00:48 (1,868,348 point/s) | TODO                      |
 
# How to run it at home
 - Replace `<INSERT_YOUR_DC_HERE>` with your own DC name in [create_schema.cql](https://github.com/Abc-Arbitrage/cassandra-csharp-benchmark/blob/master/create_schema.cql) and run it
 - Change `<INSERT_YOUR_CONTACT_HOST_HERE>` and `<INSERT_YOUR_DC_HERE>` in Program.cs
 - Compile & Run
 - Choose read/write/cleanup & wait
