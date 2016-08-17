# About
This benchmark is intended to provide an easy and repeatable way to measure the C* drivers performance.
The benchmark is inspired by a production use case where we persist time series (metrics) all the time and read them all once per day for statistics purposes.

The code is meant to be simple but we still tried to make it as fast as possible, so any comment / improvement is welcome.


# Benchmark settings
## Cluster
 - 12 nodes with SSD / 32Gb RAM / Xeon (4 cores + HT / 8M @ 3.4GHz)
 - Cassandra 2.1.12

## Write settings
 - Prepared statements
 - 5,000 time series with 18,000 data points each
 - CL: Local_One

## Read settings
 - Prepared statements
 - 30 read statements in parallel
 - Read all time series
 - No paging
 - CL: Local_One

# Benchmark results 
## C# Driver
### Specific write settings
 - 1,000 insert statements in parallel

### Results

| Version | Reads                        | Writes                     |
|:-------:|------------------------------|----------------------------|
| v2.7.0  | 00:00:40 (2,207,774 point/s) | 00:42:22 (35,392 point/s)  |
| v2.7.3  | 00:00:37 (2,394,778 point/s) | 00:45:03 (33,295 point/s)  |
| v3.0.0  | 00:00:41 (2,151,642 point/s) | 00:09:05 (165,136 point/s) |
| v3.0.8  | 00:00:37 (2,428,638 point/s) | 00:09:54 (151,308 point/s) |

## Go Driver
This is just a simple writer that mimics the C# benchmark and can probably be improved a lot.

### Specific write settings
 - 500 insert statements in parallel

### Results

|             Version                                   | Reads                        | Writes                     |
|:-----------------------------------------------------:|------------------------------|----------------------------|
| b2caded3d0f457e42515c06d4092c02055cebaa0 (2016-07-29) |            TODO              | 00:10:26 (143,766 point/s) |


# How to run it at home
 - Replace `<INSERT_YOUR_DC_HERE>` with your own DC name in [create_schema.cql](https://github.com/Abc-Arbitrage/cassandra-csharp-benchmark/blob/master/create_schema.cql) and run it
 - Change `<INSERT_YOUR_CONTACT_HOST_HERE>` and `<INSERT_YOUR_DC_HERE>` in Program.cs
 - Compile & Run
 - Choose read/write/cleanup & wait
