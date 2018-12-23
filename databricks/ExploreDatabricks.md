# Explore Databricks

##Databricks Cloud

[Sign up](https://databricks.com/try-databricks) for a Free Trial or Community Edition. The trial provides full featured data platform for a limited amount of time (14 days) while the Community Edition is free for use, but has certain limitations:

*  Single cluster limited to 6GB, no worker nodes
* Basic notebook without collaboration
* Max 3 users
* Public environment - work is shared

## Explore the Community Edition Environment

I will use Python notebook for this exploration.

The Databricks portal provides links to common tasks. 

### Import and Export Data

From the Databricks portal this is available as two links: `Upload Data`  and `Import & Explore Data`. 

* Upload a file
* Create table in S3
* Explore DBFS (under `/FileStore`) and create table
* Create table using connector (Amazon Redshift, Amazon Kinesis, Snowflake, JDBC, Cassandra, Kafka, Redis, Elasticsearch)

### Explore Pre-loaded Datasets

On the `dbfs` there are already pre-loaded datasets under `/databricks-datasets`.

#### Using `dbutils` Package

```python
# List local filesystem
display(dbutils.fs.ls('file:/'))

# List predefined datasets on DBFS
display(dbutils.fs.ls('/databricks-datasets'))

# List predefined datasets on DBFS using local file API
display(dbutils.fs.ls('file:/dbfs/databricks-datasets'))
```

For more information on `dbutils`, use `fs.help()` command:

```python
dbutils.fs.help()
```

#### Using `%fs` Magic

`Dbutils` is also available through `%fs` shorthand (magic):

```
# Get dbutils help
%fs help

# List contents of pre-loaded datasets
%fs ls /databricks-datasets

# Inspect content of README.md file
%fs head --maxBytes=1000000 "/databricks-datasets/README.md"
```

#### Using Local File API in Python

```python
# Print the file contents
with open('/dbfs/databricks-datasets/README.md', 'r') as readme:
  print(readme.read())
```

#### Airlines Dataset

Content of the folder:

```
%fs ls /databricks-datasets/airlines/
```

```
dbfs:/databricks-datasets/airlines/README.md	README.md	1089
dbfs:/databricks-datasets/airlines/_SUCCESS	_SUCCESS	0
dbfs:/databricks-datasets/airlines/part-00000	part-00000	67108879
dbfs:/databricks-datasets/airlines/part-00001	part-00001	67108862
dbfs:/databricks-datasets/airlines/part-00002	part-00002	67108930
.....
```



Readme file:

```
%fs head /databricks-datasets/airlines/README.md
```



```
================================================
Airline On-Time Statistics and Delay Causes
================================================

## Background
The U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics (BTS) tracks the on-time performance of domestic flights operated by large air carriers. Summary information on the number of on-time, delayed, canceled and diverted flights appears in DOT's monthly Air Travel Consumer Report, published about 30 days after the month's end, as well as in summary tables posted on this website. BTS began collecting details on the causes of flight delays in June 2003. Summary statistics and raw data are made available to the public at the time the Air Travel Consumer Report is released.


FAQ Information is available at http://www.rita.dot.gov/bts/help_with_data/aviation/index.html


## Data Source
http://www.transtats.bts.gov/OT_Delay/OT_DelayCause1.asp


## Usage Restrictions
The data is released under the Freedom of Information act. More information can be found at http://www.fas.org/sgp/foia/citizen.html
```



Since there is no information on the file format, inspecting one file:

```
%fs head /databricks-datasets/airlines/part-00000
```

```
Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed,IsDepDelayed
1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,YES
1987,10,15,4,729,730,903,849,PS,1451,NA,94,79,NA,14,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,NO
1987,10,17,6,741,730,918,849,PS,1451,NA,97,79,NA,29,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,YES

```

Quick data exploration using SparkSQL

```sql
spark.sql("SELECT * FROM csv.`/databricks-datasets/airlines/part*`").show()
```



Same can be achieved with the DataFrame API:

```python
df_airlines = spark.read.format('csv').load('/databricks-datasets/airlines/part*')
df_airlines.show()
```

Or using schema inference:

```python
df_airlines = spark.read  \
    .format('csv')  \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load('/databricks-datasets/airlines/part-*')
df_airlines.printSchema()
```

It takes a few seconds to to read all the data files.

```
root
 |-- Year: integer (nullable = true)
 |-- Month: integer (nullable = true)
 |-- DayofMonth: integer (nullable = true)
 |-- DayOfWeek: integer (nullable = true)
 |-- DepTime: string (nullable = true)
 |-- CRSDepTime: integer (nullable = true)
 |-- ArrTime: string (nullable = true)
 |-- CRSArrTime: integer (nullable = true)
 |-- UniqueCarrier: string (nullable = true)
 |-- FlightNum: integer (nullable = true)
 |-- TailNum: string (nullable = true)
 |-- ActualElapsedTime: string (nullable = true)
 |-- CRSElapsedTime: integer (nullable = true)
 |-- AirTime: string (nullable = true)
 |-- ArrDelay: string (nullable = true)
 |-- DepDelay: string (nullable = true)
 |-- Origin: string (nullable = true)
 |-- Dest: string (nullable = true)
 |-- Distance: string (nullable = true)
 |-- TaxiIn: string (nullable = true)
 |-- TaxiOut: string (nullable = true)
 |-- Cancelled: integer (nullable = true)
 |-- CancellationCode: string (nullable = true)
 |-- Diverted: integer (nullable = true)
 |-- CarrierDelay: string (nullable = true)
 |-- WeatherDelay: string (nullable = true)
 |-- NASDelay: string (nullable = true)
 |-- SecurityDelay: string (nullable = true)
 |-- LateAircraftDelay: string (nullable = true)
 |-- IsArrDelayed: string (nullable = true)
 |-- IsDepDelayed: string (nullable = true)
```





## Reference

1. Spark The Definitive Guide
2. [Databricks: Spark The Definitive Guide on GitHub](https://github.com/databricks/Spark-The-Definitive-Guide)
3. [Databricks File System DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html)
4. [Databricks User Guide: Notebooks](https://docs.databricks.com/user-guide/notebooks/index.html)

