---
description: Ingest CSV data in Spark using pySpark.
---

# Ingest CSV Data

Examples use following dataset: [https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption](https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption).

### Use RDD API to Pre-process CSV Data

To create RDD from text file, use `SparkContext.textFile()`\` method. The resulting RDD contains one record for each line of the source file.

```python
dir_data = 'Datasets/data'
# Create RDD from the text file
rdd_raw_data = sc.textFile('{}/household_power_consumption.txt'.format(dir_data))
```

Let's do some exploration on the loaded data.

```python
# Get the number of records in an RDD
rdd_raw_data.count()
#> 2075260
```

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

Source data, includes missing data. It is marked with question mark: \`?\` 

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

To cleanse the data we remove the header line and missing data:

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

#### Explore DataFrameReader API

As a next step we want to convert the RDD to a DataFrame. There are many ways to achieve this. We are using `DataFrameReader` API. Interesting reference could be found [here](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-DataFrameReader.html). Let's explore  the `.csv()` method.

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

#### Convert RDD with CSV lines to DataFrame

We first define schema for the DataFrame:

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
    StructField("Sub_metering_3", DecimalType(10,2))])
```

To convert the RDD with CSV lines into a DataFrame:

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



