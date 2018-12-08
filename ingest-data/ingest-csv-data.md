---
description: Ingest CSV data in Spark using pySpark.
---

# Read and Manage CSV Data with Spark

I am about to show you two ways to load CSV data into Spark and make it available for processing with high-level APIs.

The first method uses low-level RDD API. It might be useful in many situations, especially if the data needs preliminary management before transforming it into a DataSet. The second and the third methods are somewhat similar. Thy just use different APIs. They both provide great flexibility and simplicity. 

I will actually show you at the end a fourth method which is the probably the quickest and the easiest one, but is very limited in flexibility.

Examples use following dataset: [https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption](https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption).

### Method 1: CSV Pre-processing With RDD

In this recipe we will:

1. Load CSV data into Spark RDD. We will do some exploration on the data:
   1. Count number of records in RDD.
   2. Inspect 5 records from the RDD.
   3. Search RDD for data.
2. Cleanse the Data by Removing Headers and Filter Out Missing Data
3. Create Spark DataFrame for further exploration using DataFrame API
4. Compute Summary Statistics on DataFrame

#### Read CSV data into Spark RDD

To create RDD from text file, use `SparkContext.textFile()`\` method. textFile\(\) method reads text file from any supported filesystem and returns an RDD of strings. Each element from the RDD contains a line, a record, from the source file.

```python
# Create RDD from the text file
rdd_raw_data = sc.textFile('dataset/household_power_consumption.txt')
```

#### Exploration: Count Number of Records in Spark RDD

Let's do some exploration on the loaded data. Spark RDD exposes a method `.count()` which returns the number of records in the RDD. The `.count()` method is an action.

```python
# Get the number of records in an RDD
rdd_raw_data.count()
#> 2075260
```

#### Exploration: Inspect 5 Records from the Spark RDD

```python
# Get the top 5 text lines from RDD
rdd_raw_data.take(5)
```

The output is as follows:

```text
['Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3',
 '16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000',
 '16/12/2006;17:25:00;5.360;0.436;233.630;23.000;0.000;1.000;16.000',
 '16/12/2006;17:26:00;5.374;0.498;233.290;23.000;0.000;2.000;17.000',
 '16/12/2006;17:27:00;5.388;0.502;233.740;23.000;0.000;1.000;17.000']
```

#### Exploration: Search RDD for Data

Our source data includes missing values. These values are marked with a question mark '?'. It is not very practical to browse all the 2 million records to search for missing values. One way to search for the data is to filter the RDD.

The RDD `.filter()` method takes as an argument a runnable, for example a lambda function. For simplicity I will refer to it as filter function. The filter function takes as an argument RDD element and returns boolean value, indicating if the element passes the filter. The filter function executed for each element in the RDD. A new RDD is returned, containing only elements from the original RDD for wich the filter function returned `True`.

Searching our data for missing data could look like following:

```python
# Filter RDD text records to see missing data
rdd_raw_data.filter(lambda row: row.find('?') >=0).take(5)
```

```text
['21/12/2006;11:23:00;?;?;?;?;?;?;',
 '21/12/2006;11:24:00;?;?;?;?;?;?;',
 '30/12/2006;10:08:00;?;?;?;?;?;?;',
 '30/12/2006;10:09:00;?;?;?;?;?;?;',
 '14/1/2007;18:36:00;?;?;?;?;?;?;']
```

In this case we peek only 5 elements.

#### Cleanse The RDD Records

Let's store the header line into a variable.

```python
header = rdd_data.first()
```

To cleanse the data we remove the header line and missing data, using chained .filter\(\) method call:

```python
# Remove Haders and Missing Data
rdd_data = rdd_raw_data  \
   .filter(lambda r: r != header)  \
   .filter(lambda r: r.find('?') < 0)
rdd_data.show(5)
```

```text
['16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000',
 '16/12/2006;17:25:00;5.360;0.436;233.630;23.000;0.000;1.000;16.000',
 '16/12/2006;17:26:00;5.374;0.498;233.290;23.000;0.000;2.000;17.000',
 '16/12/2006;17:27:00;5.388;0.502;233.740;23.000;0.000;1.000;17.000',
 '16/12/2006;17:28:00;3.666;0.528;235.680;15.800;0.000;1.000;17.000']
```

The first call of the .filter\(\) method removes the lines which match the header line and return an RDD with removed headers. The second .filter\(\) call is applied on the resulting RDD. It further filters out the records which contain a question mark. The result is a cleansed RDD. We store a reference to this RDD into the `rdd_data` variable.

To inspect the result we use the `.show()` method on the cleansed RDD.

#### Convert CSV RDD to DataFrame

To accomplish this task I will use the DataFrameReader API. It provides reach functionality and makes parsing really easy.

For best results, I prefer defining schema on the data by hand:

```python
from pyspark.sql.types import *
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Global_active_power", DecimalType(10,2)),
    StructField("Global_reactive_power", DecimalType(10,2)),
    StructField("Voltage", DecimalType(10,2)),
    StructField("Global_intensity", DecimalType(10,2)),
    StructField("Sub_metering_1", DecimalType(10,2)),
    StructField("Sub_metering_2", DecimalType(10,2)),
    StructField("Sub_metering_3", DecimalType(10,2))]
```

And finally the conversion to DataFrame with the first 5 records:

```python
df_data = spark.read.csv(rdd_data, schema=schema, sep=";")
df_data.show(5)
```

```text
+----------+--------+-------------------+---------------------+-------+----------------+--------------+--------------+--------------+
|      Date|    Time|Global_active_power|Global_reactive_power|Voltage|Global_intensity|Sub_metering_1|Sub_metering_2|Sub_metering_3|
+----------+--------+-------------------+---------------------+-------+----------------+--------------+--------------+--------------+
|16/12/2006|17:24:00|               4.22|                 0.42| 234.84|           18.40|          0.00|          1.00|         17.00|
|16/12/2006|17:25:00|               5.36|                 0.44| 233.63|           23.00|          0.00|          1.00|         16.00|
|16/12/2006|17:26:00|               5.37|                 0.50| 233.29|           23.00|          0.00|          2.00|         17.00|
|16/12/2006|17:27:00|               5.39|                 0.50| 233.74|           23.00|          0.00|          1.00|         17.00|
|16/12/2006|17:28:00|               3.67|                 0.53| 235.68|           15.80|          0.00|          1.00|         17.00|
+----------+--------+-------------------+---------------------+-------+----------------+--------------+--------------+--------------+
only showing top 5 rows
```

It is also possible to convert the RDD "manually":

```python
df_data = rdd_data.map(lambda r: r.split(';')).toDF(schema)
```

#### Exploration: Compute Summary \(Descriptive\) Statistics on Dataset 

Spark DataFrame API provides convenient way for computing basic summary statistics for a DataFrame.  The describe\(\) method computes basic statistics for numeric and string columns. The result is a new DataFrame, which contains the result:

* count
* mean - Applicable only for numeric columns.
* stddev - Applicable only for numeric columns
* min
* max

```python
df_data.describe().show()
```

```text
+-------+--------+--------+-------------------+---------------------+-----------------+------------------+------------------+------------------+-----------------+
|summary|    Date|    Time|Global_active_power|Global_reactive_power|          Voltage|  Global_intensity|    Sub_metering_1|    Sub_metering_2|   Sub_metering_3|
+-------+--------+--------+-------------------+---------------------+-----------------+------------------+------------------+------------------+-----------------+
|  count| 2049280| 2049280|            2049280|              2049280|          2049280|           2049280|           2049280|           2049280|          2049280|
|   mean|    null|    null| 1.0916150365005446|  0.12371447630388221|240.8398579745135| 4.627759310588324|1.1219233096502186|1.2985199679887571| 6.45844735712055|
| stddev|    null|    null| 1.0572941610939892|  0.11272197955071642|3.239986679009991|4.4443962597861795| 6.153031089701349| 5.822026473177558|8.437153908665477|
|    min|1/1/2007|00:00:00|              0.076|                0.000|          223.200|             0.200|             0.000|             0.000|            0.000|
|    max|9/9/2010|23:59:00|              9.994|                1.390|          254.150|             9.800|             9.000|             9.000|            9.000|
+-------+--------+--------+-------------------+---------------------+-----------------+------------------+------------------+------------------+-----------------+
```

#### Advanced: DataFrameReader API

This is somewhat advanced information. The purpose is to show you how you can find information about the API from Jupiter notebook. Interesting `DataFrameReader` API reference could be found [here](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-DataFrameReader.html). Let's explore  the `.csv()` method.

```python
# This works in Jupyter notebook
spark.read.csv?
```

```text
Signature: spark.read.csv(path, schema=None, sep=None, encoding=None, quote=None, escape=None, comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None, negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None, maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None, columnNameOfCorruptRecord=None, multiLine=None, charToEscapeQuoteEscaping=None)
Docstring:
Loads a CSV file and returns the result as a  :class:`DataFrame`.

This function will go through the input once to determine the input schema if
``inferSchema`` is enabled. To avoid going through the entire data once, disable
``inferSchema`` option or specify the schema explicitly using ``schema``.

:param path: string, or list of strings, for input path(s),
             or RDD of Strings storing CSV rows.
:param schema: an optional :class:`pyspark.sql.types.StructType` for the input schema
               or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
:param sep: sets a single character as a separator for each field and value.
            If None is set, it uses the default value, ``,``.
:param encoding: decodes the CSV files by the given encoding type. If None is set,
                 it uses the default value, ``UTF-8``.
:param quote: sets a single character used for escaping quoted values where the
              separator can be part of the value. If None is set, it uses the default
              value, ``"``. If you would like to turn off quotations, you need to set an
              empty string.
:param escape: sets a single character used for escaping quotes inside an already
               quoted value. If None is set, it uses the default value, ``\``.
:param comment: sets a single character used for skipping lines beginning with this
                character. By default (None), it is disabled.
:param header: uses the first line as names of columns. If None is set, it uses the
               default value, ``false``.
:param inferSchema: infers the input schema automatically from data. It requires one extra
               pass over the data. If None is set, it uses the default value, ``false``.
:param ignoreLeadingWhiteSpace: A flag indicating whether or not leading whitespaces from
                                values being read should be skipped. If None is set, it
                                uses the default value, ``false``.
:param ignoreTrailingWhiteSpace: A flag indicating whether or not trailing whitespaces from
                                 values being read should be skipped. If None is set, it
                                 uses the default value, ``false``.
:param nullValue: sets the string representation of a null value. If None is set, it uses
                  the default value, empty string. Since 2.0.1, this ``nullValue`` param
                  applies to all supported types including the string type.
:param nanValue: sets the string representation of a non-number value. If None is set, it
                 uses the default value, ``NaN``.
:param positiveInf: sets the string representation of a positive infinity value. If None
                    is set, it uses the default value, ``Inf``.
:param negativeInf: sets the string representation of a negative infinity value. If None
                    is set, it uses the default value, ``Inf``.
:param dateFormat: sets the string that indicates a date format. Custom date formats
                   follow the formats at ``java.text.SimpleDateFormat``. This
                   applies to date type. If None is set, it uses the
                   default value, ``yyyy-MM-dd``.
:param timestampFormat: sets the string that indicates a timestamp format. Custom date
                        formats follow the formats at ``java.text.SimpleDateFormat``.
                        This applies to timestamp type. If None is set, it uses the
                        default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
:param maxColumns: defines a hard limit of how many columns a record can have. If None is
                   set, it uses the default value, ``20480``.
:param maxCharsPerColumn: defines the maximum number of characters allowed for any given
                          value being read. If None is set, it uses the default value,
                          ``-1`` meaning unlimited length.
:param maxMalformedLogPerPartition: this parameter is no longer used since Spark 2.2.0.
                                    If specified, it is ignored.
:param mode: allows a mode for dealing with corrupt records during parsing. If None is
             set, it uses the default value, ``PERMISSIVE``.

        * ``PERMISSIVE`` : when it meets a corrupted record, puts the malformed string                   into a field configured by ``columnNameOfCorruptRecord``, and sets other                   fields to ``null``. To keep corrupt records, an user can set a string type                   field named ``columnNameOfCorruptRecord`` in an user-defined schema. If a                   schema does not have the field, it drops corrupt records during parsing.                   A record with less/more tokens than schema is not a corrupted record to CSV.                   When it meets a record having fewer tokens than the length of the schema,                   sets ``null`` to extra fields. When the record has more tokens than the                   length of the schema, it drops extra tokens.
        * ``DROPMALFORMED`` : ignores the whole corrupted records.
        * ``FAILFAST`` : throws an exception when it meets corrupted records.

:param columnNameOfCorruptRecord: allows renaming the new field having malformed string
                                  created by ``PERMISSIVE`` mode. This overrides
                                  ``spark.sql.columnNameOfCorruptRecord``. If None is set,
                                  it uses the value specified in
                                  ``spark.sql.columnNameOfCorruptRecord``.
:param multiLine: parse records, which may span multiple lines. If None is
                  set, it uses the default value, ``false``.
:param charToEscapeQuoteEscaping: sets a single character used for escaping the escape for
                                  the quote character. If None is set, the default value is
                                  escape character when escape and quote characters are
                                  different, ```` otherwise.

>>> df = spark.read.csv('python/test_support/sql/ages.csv')
>>> df.dtypes
[('_c0', 'string'), ('_c1', 'string')]
>>> rdd = sc.textFile('python/test_support/sql/ages.csv')
>>> df2 = spark.read.csv(rdd)
>>> df2.dtypes
[('_c0', 'string'), ('_c1', 'string')]

```



### Method 2. Load CSV Data Using DataFrameReader API

We already explored the DataFrameReader API to convert our RDD into a nice DataFrame. The DataFrameReader API provides much more rich functionalities, including schema inference and many more. Here's how I'm gonna use it.

```python
df_data = spark.read.format("csv")  \
    .option('delimiter', ';') \
    .option("header", True)  \
    .option('inferSchema', True) \
    .load('dataset/household_power_consumption.txt')
```

I believe the above example is pretty much self-explanatory. We read data in CSV format. Fields are delimited by semicolon \(`;`\). The column names are in a header line. We leave Spark to figure out the schema. 

We can define a temporary  view on the DataFrame and start querying it with SparkSql or DataFrame API.

```python
df_data.createOrReplaceTempView('power_consumption')
spark.sql('SELECT COUNT(*) FROM power_consumption').show()
```

```text
+--------+
|count(1)|
+--------+
| 2075259|
+--------+
```

And an example, using the DataFrame API:

```python
from pyspark.sql.functions import *

r = df_data.groupBy('Date').count().show(5)
```

```text
+----------+-----+
|      Date|count|
+----------+-----+
| 30/1/2007| 1440|
| 13/2/2007| 1440|
|19/11/2007| 1440|
| 12/1/2008| 1440|
| 26/2/2008| 1440|
+----------+-----+
only showing top 5 rows
```

For more info on the Spark DataFrame API, refer to the [documentation](http://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#module-pyspark.sql.functions).

#### Explore: What is the Schema of the DataFrame

Spark provides convenient way for inspecting the DataFrame's schema. It is very useful in situations where the DataFrame holds complex , deeply nested data structures.

```python
df_data.printSchema()
```

```text
root
 |-- Date: string (nullable = true)
 |-- Time: string (nullable = true)
 |-- Global_active_power: string (nullable = true)
 |-- Global_reactive_power: string (nullable = true)
 |-- Voltage: string (nullable = true)
 |-- Global_intensity: string (nullable = true)
 |-- Sub_metering_1: string (nullable = true)
 |-- Sub_metering_2: string (nullable = true)
 |-- Sub_metering_3: double (nullable = true)
```

You can also get the schema as JSON:

```python
df_data.schema.json()
```

```text
'{"fields":[{"metadata":{},"name":"Date","nullable":true,"type":"string"},{"metadata":{},"name":"Time","nullable":true,"type":"string"},{"metadata":{},"name":"Global_active_power","nullable":true,"type":"string"},{"metadata":{},"name":"Global_reactive_power","nullable":true,"type":"string"},{"metadata":{},"name":"Voltage","nullable":true,"type":"string"},{"metadata":{},"name":"Global_intensity","nullable":true,"type":"string"},{"metadata":{},"name":"Sub_metering_1","nullable":true,"type":"string"},{"metadata":{},"name":"Sub_metering_2","nullable":true,"type":"string"},{"metadata":{},"name":"Sub_metering_3","nullable":true,"type":"double"}],"type":"struct"}'
```

This method is very convenient when you want to serialize/deserialize the schema.

#### Get Pretty JSON Schema

To produce much more human-readable JSON schema, I use json utilities, provided by Python:

```python
import json
print(json.dumps(json.loads(df_data.schema.json()), indent=2))
```

```text
{
  "fields": [
    {
      "metadata": {},
      "name": "Date",
      "nullable": true,
      "type": "string"
    },
...................
```



#### Method 3: Create SparkSql Table

```python
spark.sql("""DROP TABLE IF EXISTS power_consumption""")
spark.sql("""
  CREATE TABLE power_consumption 
    USING CSV
    OPTIONS ( delimiter ";", header "true")
    LOCATION 'Datasets/data/household_power_consumption.txt'
""")

spark.sql("SELECT * FROM power_consumption").show(3)
```

```text
+----------+--------+-------------------+---------------------+-------+----------------+--------------+--------------+--------------+
|      Date|    Time|Global_active_power|Global_reactive_power|Voltage|Global_intensity|Sub_metering_1|Sub_metering_2|Sub_metering_3|
+----------+--------+-------------------+---------------------+-------+----------------+--------------+--------------+--------------+
|16/12/2006|17:24:00|              4.216|                0.418|234.840|          18.400|         0.000|         1.000|          17.0|
|16/12/2006|17:25:00|              5.360|                0.436|233.630|          23.000|         0.000|         1.000|          16.0|
|16/12/2006|17:26:00|              5.374|                0.498|233.290|          23.000|         0.000|         2.000|          17.0|
+----------+--------+-------------------+---------------------+-------+----------------+--------------+--------------+--------------+
only showing top 3 rows
```



#### Method 4: Direct SparkSql Query

Finally the promised direct query method. I generated a users file from http://randomuser.me. 

```python
spark.sql("SELECT * FROM csv.`../data/user/user*.csv`").describe().show()
```

```text
+-------+-----+-------+----------+--------------------+
|summary|  _c0|    _c1|       _c2|                 _c3|
+-------+-----+-------+----------+--------------------+
|  count|   11|     11|        11|                  11|
|   mean| null|   null|      null|                null|
| stddev| null|   null|      null|                null|
|    min| miss| amelia|de krijger|amelia.gibson@exa...|
|    max|title|wiepkje|      webb|wiepkje.vantienen...|
+-------+-----+-------+----------+--------------------+
```



