# Compute Basic Statistics for DataFrame



```python
spark.sql("""SELECT * FROM csv.`../data/user/user*.csv`""") \
   .describe()  \
   .show()
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



