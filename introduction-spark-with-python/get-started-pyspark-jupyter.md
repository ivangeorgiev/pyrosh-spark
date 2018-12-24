# Getting Started  with pySpark and Jupyter Notebook in Minutes

In this section I will show you how to quickly setup Spark and Jupyter Notebook. You will also create your first Spark program. After finishing this tutorial, you will have your environment set up and will be able to use Jupyter notebooks to interact with Spark on your local machine.

## What You Need

You need to have a computer running decent operating system with browser. You need to have Python 3+ and Java Development Kit 1.8 installed.

I am using Windows, but basically the same approach works with pretty much any operating system - e.g. Linux or Mac OS.



## Install Spark

**Step 1:** Download Spark from https://spark.apache.org/downloads.html. Here is what I selected:

- Spark release: 2.4.0 (Nov 02 2018)
- Package type: Pre-built for Apache Hadoop 2.7 and later
- Download: [spark-2.4.0-bin-hadoop2.7.tgz](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz) 

**Step 2:** Extract Spark. Will assume Spark is extracted at: `C:\apps\spark-2.4.0-bin-hadoop2.7`

To extract Spark you can use [https://www.win-rar.com](https://www.win-rar.com) or [7-Zip](https://www.7-zip.org/). I personally prefer [7-Zip](https://www.7-zip.org/). On Linux/Mac OS you do not need separate archive manager.

**Step 3: (Optional)** Define system environment variable pointing where Spark is installed.

```
SPARK_HOME=C:\apps\spark-2.4.0-bin-hadoop2.7
```



## Install Jupyter

**Step 1:** Open a command/terminal window.

**Step 2:** Install Jupyter as Python package, using Python package manager:

```bash
pip install jupyter
```



## Install `findspark` Package

`findspark` package makes it easy to access Spark packages directly from Jupyter notebooks (or any Python program) without much pain. It also can automatically update system settings, e.g. setting SPARK_HOME.

```
pip install findspark
```



## Start Jupyter Notebook Server

**Step 1:** Create a directory where you will store your Jupyter notebooks.

```
C:\Sandbox\notebooks
```

**Step 2:** Open a command/terminal window and change the notebooks directory:

```
cd C:\Sandbox\notebooks
```

**Step 3:** Start Jupyter:

```
jupyter notebook
```

The Jupyter notebook server starts and opens web browser window, showing the Jupyter user interface. 

## Write Your First Spark Program

Create new Python 3 notebook. You can also [download the notebook](SetupSparkAndJupyter.ipynb) and open it in Jupyter.

In Jupyter notebook you use cells to enter code. You can execute a cell and see the result immediately after the cell. The process is interactive. You can also execute a cell multiple time.

If you set the optional `SPARK_HOME` environment variable, you just use `finspark.init()`. Enter following as cell 1 and press `Shift-Enter` to execute:

```python
import findspark
findspark.init()
```

If you haven't defined `SPARK_HOME` environment variable, you need to pass the location of Spark as a parameter to `findspark.init()`

```python
import findspark
findspark.init(r'C:\apps\spark-2.4.0-bin-hadoop2.7')
```

In the next step you initialize `SparkSession`. Enter following as cell 2 and press `Shift-Enter` to execute:

```python
# Import SparkSession module
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()
```

Now you can write your program. 

In the following example, we create a DataFrame with a single column `num` and 100 rows containing numbers from 1 to 100 each. Than we compute the sum of the numbers. Enter following as cell 3 and press `Shift-Enter` to execute:

```python
# 1. Create a DataFrame with numbers from 1 to 100
df_numbers = spark.range(1,101).toDF('num')

# 2. Aggregate numbers DataFrame by computing the sum() and produce a new DataFrame
df_sum = df_numbers.groupBy().sum('num')

# 3. Collect DataFrame rows and print
df_sum.show()
```

The result is show below the cell:

```
+--------+
|sum(num)|
+--------+
|    5050|
+--------+
```

