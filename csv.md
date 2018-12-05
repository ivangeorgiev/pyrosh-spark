---
description: Work with CSV files in pySpark
---

# CSV

### Query CSV File with SparkSQL

```python
spark.sql('SELECT * FROM csv.`{}/household_power_consumption.txt`'.format(dir_data)).show()
```



