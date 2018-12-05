# Getting Started with PySpark





### Create Spark Context

```python
import pyspark
sc = pyspark.SparkContext('local[*]')
```

### Create SparkSql Context

```python
import pyspark.sql
spark = pyspark.sql.SparkSession.builder \
     .master("local[*]") \
     .appName("My Application Name") \
     .config("spark.some.config.option", "some-value") \
     .getOrCreate()

sc = spark.sparkContext
sql = spark.sql
```

### To get Spark version, use:

```python
# Get Spark version
sc.version

# or
spark.version
```

### To Add Dependencies \(Avro, XML, etc. \)

```python
from os import environ
environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.4.1,com.databricks:spark-avro_2.11:4.0.0,org.mariadb.jdbc:mariadb-java-client:2.3.0,mysql:mysql-connector-java:8.0.13 pyspark-shell' 
```



