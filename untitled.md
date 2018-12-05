# Unit Testing pySpark Jobs

### Create Base PySparkTestCase Class

The `PySparkTestCase` class implements `.setUpClass()` and `.tearDownClass()`  methods from the base `TestCase` class to setup class scope fixtures:

* sc - SparkContext
* spark - SparkSession
* sql - SparkSql

```python
import unittest
import pyspark


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (pyspark.sql.SparkSession
             .builder
             .master('local[2]')
             .config('spark.jars.packages', 'com.databricks:spark-avro_2.11:3.0.1')
             .appName('pytest-pyspark-local-testing')
             .enableHiveSupport()
             .getOrCreate())
        cls.sc = cls.spark.sparkContext
        cls.sql = cls.spark.sql

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
```

### Writing a Simple PySpark Unit Test Case

```python
class SimpleTestCase(PySparkTestCase):

    def test_basic(self):
        test_input = [
            ' hello spark ',
            ' hello again spark spark'
        ]

        input_rdd = self.sc.parallelize(test_input, 1)

        from operator import add

        results = input_rdd.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(add).collect()
        self.assertEqual(results, [('hello', 2), ('spark', 3), ('again', 1)])
```

### Execute Test Case

You can execute the test case, using `unittest.main()`\`:

```python
if __name__ == '__main__':
    unittest.main(verbosity=2)
```

For more control on the test case execution:

```python
def suite():
    suite = unittest.TestSuite()
    suite.addTest(SimpleTestCase('test_basic'))
    return suite

runner = unittest.TextTestRunner()
runner.run(suite())
```

