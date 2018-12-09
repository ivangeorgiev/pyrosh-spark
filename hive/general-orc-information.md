# General ORC Information



### Case Study: Optimize Access to Existing Data





Source characteristics:

| Characteristic             | Description                              |
| -------------------------- | ---------------------------------------- |
| Storage Format             | CSV                                      |
| Partitioned by             | country string - 7, <br />date_str - 443 |
| Number of files            | 31,000                                   |
| Total file size            | 842,842,445                              |
| Average file size          | 27,188                                   |
| Total number of partitions | 2,624                                    |
| Number of records          | 9,203,824                                |



```sql
SELECT date_str, country, COUNT(*) 
  FROM mart.purchases 
 GROUP BY date_str, country;
```

The query takes 1300s.



#### Create ORC Table As Select

```sql
CREATE TABLE ig_sandbox.purchases
STORED AS ORC
AS SELECT * FROM mart.purchases;
```

Limitation: Hive doesn't support partitions at the target table with `CREATE TABLE AS`. Version used is: 1.2

Here is the Hive execution log:

```

================================
Logs for Query 'use dev_bdahive'
================================


===============================================================================================================
Logs for Query 'CREATE TABLE ig_sandbox.vod_purchases
STORED AS ORC
AS SELECT * FROM dev_bdahive.vod_purchases'
===============================================================================================================

INFO  : Tez session hasn't been created yet. Opening session
DEBUG : Adding local resource: scheme: "hdfs" host: "mycluster" port: -1 file: "/tmp/hive/hive/_tez_session_dir/4562edaa-9989-437b-8841-59771d6397c8/hive-hcatalog-core.jar"
INFO  : Dag name: CREATE TABLE ig_sandb...dahive.vod_purchases(Stage-1)
DEBUG : DagInfo: {"context":"Hive","description":"CREATE TABLE ig_sandbox.vod_purchases\nSTORED AS ORC\nAS SELECT * FROM dev_bdahive.vod_purchases"}
DEBUG : Setting Tez DAG access for queryId=hive_20181209211252_0eb15712-fa7e-42a7-b998-ada98d6101f3 with viewAclString=*, modifyStr=spark-admin,hive
INFO  : Status: Running (Executing on YARN cluster with App id application_1544182532382_0030)

INFO  : Status: DAG finished successfully in 1708.30 seconds
INFO  : 
INFO  : Query Execution Summary
INFO  : --------------------------------------------------------------------------------
INFO  : OPERATION                            DURATION
INFO  : --------------------------------------------------------------------------------
INFO  : Compile Query                          31.33s
INFO  : Prepare Plan                          164.06s
INFO  : Submit Plan                             0.77s
INFO  : Start DAG                               0.54s
INFO  : Run DAG                              1708.30s
INFO  : --------------------------------------------------------------------------------
INFO  : 
INFO  : Task Execution Summary
INFO  : ------------------------------------------------------------------------------------------------------------------
INFO  :   VERTICES  TOTAL_TASKS  FAILED_ATTEMPTS  KILLED_TASKS   DURATION(ms)  CPU_TIME(ms)  GC_TIME(ms)  INPUT_RECORDS  OUTPUT_RECORDS
INFO  : ------------------------------------------------------------------------------------------------------------------
INFO  :      Map 1          353                0             0     1455571.00       974,650       76,166      9,203,824               0
INFO  : ------------------------------------------------------------------------------------------------------------------
INFO  : 
INFO  : org.apache.tez.common.counters.DAGCounter:
INFO  :    NUM_SUCCEEDED_TASKS: 353
INFO  :    TOTAL_LAUNCHED_TASKS: 353
INFO  :    OTHER_LOCAL_TASKS: 352
INFO  :    RACK_LOCAL_TASKS: 1
INFO  :    AM_CPU_MILLISECONDS: 109260
INFO  :    AM_GC_TIME_MILLIS: 483
INFO  : File System Counters:
INFO  :    WASB_BYTES_READ: 842842445
INFO  :    WASB_BYTES_WRITTEN: 92595897
INFO  : org.apache.tez.common.counters.TaskCounter:
INFO  :    GC_TIME_MILLIS: 76166
INFO  :    CPU_MILLISECONDS: 974650
INFO  :    PHYSICAL_MEMORY_BYTES: 370894438400
INFO  :    VIRTUAL_MEMORY_BYTES: 995240816640
INFO  :    COMMITTED_HEAP_BYTES: 370894438400
INFO  :    INPUT_RECORDS_PROCESSED: 9203824
INFO  :    INPUT_SPLIT_LENGTH_BYTES: 842842445
INFO  :    OUTPUT_RECORDS: 0
INFO  : HIVE:
INFO  :    CREATED_FILES: 1
INFO  :    DESERIALIZE_ERRORS: 0
INFO  :    RECORDS_IN_Map_1: 9203824
INFO  :    RECORDS_OUT_1_ig_sandbox.vod_purchases: 9203824
INFO  : TaskCounter_Map_1_INPUT_vod_purchases:
INFO  :    INPUT_RECORDS_PROCESSED: 9203824
INFO  :    INPUT_SPLIT_LENGTH_BYTES: 842842445
INFO  : TaskCounter_Map_1_OUTPUT_out_Map_1:
INFO  :    OUTPUT_RECORDS: 0
DEBUG : Task Stage-4:DDL cannot run because parent Stage-0:MOVE isn't done.
INFO  : Moving data to directory wasb://sdp-spark@sdphadoop.blob.core.windows.net/hive/warehouse/ig_sandbox.db/vod_purchases from wasb://sdp-spark@sdphadoop.blob.core.windows.net/hive/warehouse/ig_sandbox.db/.hive-staging_hive_2018-12-09_21-12-52_834_8882734109597169547-10/-ext-10001
INFO  : Table ig_sandbox.vod_purchases stats: [numFiles=1, numRows=9203824, totalSize=92578204, rawDataSize=9332677536]
```



The resulting table:

| Characteristic    | Description |
| ----------------- | ----------- |
| Number of files   | 1           |
| Total size        | 88.3 MB     |
| Number of records | 9,203,824   |

Running the same query, but on the new table, using ORC format:

```
SELECT date_str, country, COUNT(*) 
  FROM ig_sandbox.purchases 
 GROUP BY date_str, country;
```

Query finishes in 25 s.



#### Load Non-Partitioned ORC Table

```sql
DROP TABLE IF EXISTS ig_sandbox.purchases_nopart;
CREATE TABLE `ig_sandbox.purchases_nopart`(
`purchasedate` string,
`deviceid` string,
`vodproductid` string,
`total_list_price_in_vat` string,
`total_list_price_ex_vat` string,
`total_purch_price_in_vat` string,
`total_purch_price_ex_vat` string,
`currency` string,
`purchasechannel` string,
`country` string,
`date_str` string)
STORED AS
    ORC
```



```sql
INSERT OVERWRITE TABLE ig_sandbox.purchases_nopart
SELECT 
    `purchasedate`,
    `deviceid`,
    `vodproductid`,
    `total_list_price_in_vat`,
    `total_list_price_ex_vat`,
    `total_purch_price_in_vat`,
    `total_purch_price_ex_vat`,
    `currency`,
    `purchasechannel`,`country`,
    `date_str` 
 FROM ig_sandbox.vod_purchases;
```

Finished in 88 s, creating 1 file.

#### Load Non-Partitioned ORC Table with Limited Stripesize

```sql
DROP TABLE IF EXISTS ig_sandbox.purchases_10m;
CREATE TABLE `ig_sandbox.purchases_10m`(
`purchasedate` string,
`deviceid` string,
`vodproductid` string,
`total_list_price_in_vat` string,
`total_list_price_ex_vat` string,
`total_purch_price_in_vat` string,
`total_purch_price_ex_vat` string,
`currency` string,
`purchasechannel` string,
`country` string,
`date_str` string)
STORED AS
    ORC
TBLPROPERTIES (
    "orc.stripe.size"="10485760"
)
```



```sql
INSERT OVERWRITE TABLE ig_sandbox.purchases_10m
SELECT 
    `purchasedate`,
    `deviceid`,
    `vodproductid`,
    `total_list_price_in_vat`,
    `total_list_price_ex_vat`,
    `total_purch_price_in_vat`,
    `total_purch_price_ex_vat`,
    `currency`,
    `purchasechannel`,`country`,
    `date_str` 
 FROM ig_sandbox.vod_purchases;
```

Loading took 77 s.



The Hive Log:

```

================================
Logs for Query 'use dev_bdahive'
================================


==============================================================================================================================================================================================================================================================================================================================================================
Logs for Query 'INSERT OVERWRITE TABLE ig_sandbox.purchases_10m
SELECT 
    `purchasedate`,
    `deviceid`,
    `vodproductid`,
    `total_list_price_in_vat`,
    `total_list_price_ex_vat`,
    `total_purch_price_in_vat`,
    `total_purch_price_ex_vat`,
    `currency`,
    `purchasechannel`,`country`,
    `date_str` 
 FROM ig_sandbox.vod_purchases'
==============================================================================================================================================================================================================================================================================================================================================================

INFO  : Tez session hasn't been created yet. Opening session
DEBUG : Adding local resource: scheme: "hdfs" host: "mycluster" port: -1 file: "/tmp/hive/hive/_tez_session_dir/7d292aeb-d079-44b5-8b16-c0016d0ac1b5/hive-hcatalog-core.jar"
INFO  : Dag name: INSERT OVERWRITE TABL...andbox.vod_purchases(Stage-1)
DEBUG : DagInfo: {"context":"Hive","description":"INSERT OVERWRITE TABLE ig_sandbox.purchases_10m\nSELECT \n    `purchasedate`,\n    `deviceid`,\n    `vodproductid`,\n    `total_list_price_in_vat`,\n    `total_list_price_ex_vat`,\n    `total_purch_price_in_vat`,\n    `total_purch_price_ex_vat`,\n    `currency`,\n    `purchasechannel`,`country`,\n    `date_str` \n FROM ig_sandbox.vod_purchases"}
DEBUG : Setting Tez DAG access for queryId=hive_20181209223318_95defba9-f4a4-4799-8f08-a2be1167c1e9 with viewAclString=*, modifyStr=spark-admin,hive
INFO  : Status: Running (Executing on YARN cluster with App id application_1544182532382_0039)

INFO  : Status: DAG finished successfully in 76.94 seconds
INFO  : 
INFO  : Query Execution Summary
INFO  : --------------------------------------------------------------------------------
INFO  : OPERATION                            DURATION
INFO  : --------------------------------------------------------------------------------
INFO  : Compile Query                           0.74s
INFO  : Prepare Plan                            5.85s
INFO  : Submit Plan                             0.67s
INFO  : Start DAG                               0.40s
INFO  : Run DAG                                76.94s
INFO  : --------------------------------------------------------------------------------
INFO  : 
INFO  : Task Execution Summary
INFO  : ------------------------------------------------------------------------------------------------------------------
INFO  :   VERTICES  TOTAL_TASKS  FAILED_ATTEMPTS  KILLED_TASKS   DURATION(ms)  CPU_TIME(ms)  GC_TIME(ms)  INPUT_RECORDS  OUTPUT_RECORDS
INFO  : ------------------------------------------------------------------------------------------------------------------
INFO  :      Map 1            1                0             0       75068.00        79,630          740      9,203,824               0
INFO  : ------------------------------------------------------------------------------------------------------------------
INFO  : 
INFO  : org.apache.tez.common.counters.DAGCounter:
INFO  :    NUM_SUCCEEDED_TASKS: 1
INFO  :    TOTAL_LAUNCHED_TASKS: 1
INFO  :    RACK_LOCAL_TASKS: 1
INFO  :    AM_CPU_MILLISECONDS: 6460
INFO  :    AM_GC_TIME_MILLIS: 0
INFO  : File System Counters:
INFO  :    WASB_BYTES_READ: 92468809
INFO  :    WASB_BYTES_WRITTEN: 98168347
INFO  : org.apache.tez.common.counters.TaskCounter:
INFO  :    GC_TIME_MILLIS: 740
INFO  :    CPU_MILLISECONDS: 79630
INFO  :    PHYSICAL_MEMORY_BYTES: 1034420224
INFO  :    VIRTUAL_MEMORY_BYTES: 2820694016
INFO  :    COMMITTED_HEAP_BYTES: 1034420224
INFO  :    INPUT_RECORDS_PROCESSED: 8994
INFO  :    INPUT_SPLIT_LENGTH_BYTES: 92575867
INFO  :    OUTPUT_RECORDS: 0
INFO  : HIVE:
INFO  :    CREATED_FILES: 1
INFO  :    DESERIALIZE_ERRORS: 0
INFO  :    RECORDS_IN_Map_1: 9203824
INFO  :    RECORDS_OUT_1_ig_sandbox.purchases_10m: 9203824
INFO  : TaskCounter_Map_1_INPUT_vod_purchases:
INFO  :    INPUT_RECORDS_PROCESSED: 8994
INFO  :    INPUT_SPLIT_LENGTH_BYTES: 92575867
INFO  : TaskCounter_Map_1_OUTPUT_out_Map_1:
INFO  :    OUTPUT_RECORDS: 0
INFO  : Loading data to table ig_sandbox.purchases_10m from wasb://sdp-spark@sdphadoop.blob.core.windows.net/hive/warehouse/ig_sandbox.db/purchases_10m/.hive-staging_hive_2018-12-09_22-33-18_688_3853318438495225146-18/-ext-10000
INFO  : Table ig_sandbox.purchases_10m stats: [numFiles=1, numRows=9203824, totalSize=98168254, rawDataSize=9332677536]
```

Resulting table has one file, but the size is a little bit bigger - 94MB (vs 88 MB with default stripe seize of ).

Now, I am creating a new table `purchases_10m_2` with limited stripe size - same structure as `purchases_10m` 

```sql
CREATE TABLE ig_sandbox.purchases_10m_2 LIKE ig_sandbox.purchases_10m;

INSERT OVERWRITE TABLE ig_sandbox.purchases_10m_2
SELECT 
    `purchasedate`,
    `deviceid`,
    `vodproductid`,
    `total_list_price_in_vat`,
    `total_list_price_ex_vat`,
    `total_purch_price_in_vat`,
    `total_purch_price_ex_vat`,
    `currency`,
    `purchasechannel`,`country`,
    `date_str` 
 FROM ig_sandbox.purchases_10m;
```

Load finished for roughly the same time - 82s.



| Source Stripe Size | Stripe Size       | # Files | File Size | Load Time, s   |
| ------------------ | ----------------- | ------- | --------- | -------------- |
| 67,108,864         | 67,108,864 (part) |         |           |                |
| 10,485,760         | 67,108,864        | 1       | 88.3 MB   | 92.6           |
| 10,485,760         | 10,485,760        | 1       | 88.3 MB   | 82             |
| 1,048,576          | 10,485,760        | 1       | 88.3 MB   | 110.5<br />122 |
| 10,485,760         | 1,048,576         | 1       | 132.4 MB  | 63.9           |



#### Load Into Partitioned ORC Table

```sql
CREATE TABLE `ig_sandbox.purchases_part`(
`purchasedate` string,
`deviceid` string,
`vodproductid` string,
`total_list_price_in_vat` string,
`total_list_price_ex_vat` string,
`total_purch_price_in_vat` string,
`total_purch_price_ex_vat` string,
`currency` string,
`purchasechannel` string)
PARTITIONED BY (
`country` string,
`date_str` string)
STORED AS
    ORC
```



And let's load the new table. I will use the already created ORC table as a source to save some time.

```sql
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ig_sandbox.purchases_part
       PARTITION(country, date_str)
SELECT 
    `purchasedate`,
    `deviceid`,
    `vodproductid`,
    `total_list_price_in_vat`,
    `total_list_price_ex_vat`,
    `total_purch_price_in_vat`,
    `total_purch_price_ex_vat`,
    `currency`,
    `purchasechannel`,`country`,
    `date_str` 
 FROM ig_sandbox.purchases;
```



There is a small issue. The number of partitions is 2,624. The total data size in the first ORC table is 88MB. This means we will produce size of average 33 KB each. These are small files.



### Resources

* Hive Configuration at [orc.apache.org](https://orc.apache.org/docs/hive-config.html)



