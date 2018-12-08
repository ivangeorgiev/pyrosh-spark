# Query CSV Data with SparkSQL

### Inline Query CSV Data

```python
spark.sql("SELECT * FROM csv.`data/household_power_consumption.csv`").show(5, False)
```

```text
+-------------------------------------------------------------------------------------------------------------------------+
|_c0                                                                                                                      |
+-------------------------------------------------------------------------------------------------------------------------+
|Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3|
|16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000                                                        |
|16/12/2006;17:25:00;5.360;0.436;233.630;23.000;0.000;1.000;16.000                                                        |
|16/12/2006;17:26:00;5.374;0.498;233.290;23.000;0.000;2.000;17.000                                                        |
|16/12/2006;17:27:00;5.388;0.502;233.740;23.000;0.000;1.000;17.000                                                        |
+-------------------------------------------------------------------------------------------------------------------------+
only showing top 5 rows
```

### Create Table from CSV Data

```python
spark.sql("""DROP TABLE IF EXISTS power_consumption""")
spark.sql("""
CREATE TABLE power_consumption 
USING CSV
OPTIONS ( delimiter ";", header "true")
LOCATION 'Datasets/data/household_power_consumption.txt'
""")
```

```python
spark.sql("DESCRIBE power_consumption").show()
```

```text
+--------------------+---------+-------+
|            col_name|data_type|comment|
+--------------------+---------+-------+
|                Date|   string|   null|
|                Time|   string|   null|
| Global_active_power|   string|   null|
|Global_reactive_p...|   string|   null|
|             Voltage|   string|   null|
|    Global_intensity|   string|   null|
|      Sub_metering_1|   string|   null|
|      Sub_metering_2|   string|   null|
|      Sub_metering_3|   string|   null|
+--------------------+---------+-------+
```



