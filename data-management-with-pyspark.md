# Data Management with pySpark



The dataset used in the examples can be downloaded from [https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption](https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption).

```python
# Create a RDD
rdd_hhEPC = sc.textFile('household_power_consumption.txt')

# Get the number of lines/records/rows in RDD
rdd_hhEPC.count()
# 2075260

# Get the header line
header = rdd_hhEPC.first()
print(header)
# Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3
```

To remove the header line\(s\) we use  the `.filter()`  method.

```python
# Remove Haders and Missing Data
rdd_data = rdd_hhEPC  \
   .filter(lambda r: r != header)  \
   .filter(lambda r: r.find('?') >= 0)

# See the first 10 lines
rdd_data.take(10)
```

```text
['16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000',
 '16/12/2006;17:25:00;5.360;0.436;233.630;23.000;0.000;1.000;16.000',
 '16/12/2006;17:26:00;5.374;0.498;233.290;23.000;0.000;2.000;17.000',
 '16/12/2006;17:27:00;5.388;0.502;233.740;23.000;0.000;1.000;17.000',
 '16/12/2006;17:28:00;3.666;0.528;235.680;15.800;0.000;1.000;17.000',
 '16/12/2006;17:29:00;3.520;0.522;235.020;15.000;0.000;2.000;17.000',
 '16/12/2006;17:30:00;3.702;0.520;235.090;15.800;0.000;1.000;17.000',
 '16/12/2006;17:31:00;3.700;0.520;235.220;15.800;0.000;1.000;17.000',
 '16/12/2006;17:32:00;3.668;0.510;233.990;15.800;0.000;1.000;17.000',
 '16/12/2006;17:33:00;3.662;0.510;233.860;15.800;0.000;2.000;16.000']
```



