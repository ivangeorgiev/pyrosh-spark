# Using Spark with Jupyter

## Install Spark with Jupyter on Windows

Following procedure helps setting up Spark with Jupyter notebook on Windows.

1. Instal Java - JDK 1.8+
2. Install Python 3
3. Install Spark
4. Download winutils.exe
5. Set Environment Variables
6. Verify Components
7. Start Jupyter Notebook

### 1. Install JDK 1.8

Go to Oracle's Java site to download and install latest JDK.

Will assume Java is installed at `C:\Program Files\Java\jdk1.8.0_191 `



### 2. Install Python 3.7

Download and install Python from https://www.python.org/downloads/.

Will assume Python is installed at `C:\Python37`.

### 3. Install Spark

Download Spark from https://spark.apache.org/downloads.html:

- Spark release: 2.4.0 (Nov 02 2018)
- Package type: Pre-built for Apache Hadoop 2.7 and later
- Download: [spark-2.4.0-bin-hadoop2.7.tgz](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz) 

Extract Spark. Will assume Spark is extracted at: `C:\apps\spark-2.4.0-bin-hadoop2.7`

Modify log4j configuration to reduce log activity. Copy log4j.properties.template to log4j.properties. Modify:

```
log4j.rootCategory=WARN, console
```



### 4. Download winutils.exe

Download winutils.exe (http://media.sundog-soft.com/Udemy/winutils.exe or https://github.com/steveloughran/winutils) and place it under `%SPARK_HOME%\bin`.

### 5. Install Jupyter

```bash
pip install --upgrade pip
pip install jupyter
```



### 6. Set Environment Variables

```bash
set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_191
set SPARK_HOME=C:\apps\spark-2.4.0-bin-hadoop2.7
set HADOOP_HOME=%SPARK_HOME%
set PATH=%PATH%;%SPARK_HOME%\bin
```

Open `Control Panel` and in the search box type `environment variables`. Click the `Edit the system environment variables` link.

- The System Properties dialog opens.
- Click the `Environment Variables...` button
- Add above variables. Suggest that you not use variable references, but specify the values fully. This way you avoid problems caused by unpredictable order of variable evaluation and assignment.

### 6. Verify Components

```bash
# Verify Python
> python --version
Python 3.7.1

# Verify Java
> java -version
java version "1.8.0_191"
Java(TM) SE Runtime Environment (build 1.8.0_191-b12)
Java HotSpot(TM) 64-Bit Server VM (build 25.191-b12, mixed mode)

```



Start `pyspark` and execute simple program

```python
>>> spark.range(5).toDF("num").show()
+---+
|num|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```

Press `Ctrl-Z` to exit.

### 7. Start Jupyter Notebook

Navigate to your notebook directory (`C:\Sandbox\notebook`) and start Jupyter:

```bash
jupyter notebook
```

Create a new Python 3 notebook and execute following into a cell:

```python
import os
import sys
import glob

def get_spark(appName = 'HelloWorld'):
    spark_home = os.path.abspath(os.environ.get('SPARK_HOME', None))
    spark_python = os.path.abspath(spark_home + '/python')
    pyj4 = os.path.abspath(glob.glob(spark_python + '/lib/py4j*.zip')[0])
    if (not spark_python in sys.path):
        sys.path.append(spark_python)
    if (not pyj4 in sys.path):
        sys.path.append(pyj4)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

spark = get_spark()
```

In next cell you can run:

```python
spark.range(5).toDF('num').show()
```

```
+---+
|num|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```



```python
#This is an early version
import os
import sys

spark_home = os.environ.get('SPARK_HOME', None)
# print(spark_home)
# > C:\apps\spark-2.4.0-bin-hadoop2.7

sys.path.append(spark_home + '/python')
sys.path.append(spark_home + '/python/lib/py4j-0.10.7-src.zip')
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

# sc = spark.sparkContext
# nums = sc.parallelize([1,2,3,4])
# nums.collect()

spark.range(5).toDF("num").show()
```

