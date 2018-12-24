
# Parse Excel Files in Python

This article is available in following formats:
* [Parse Excel Files in Python Jupyter Notebook (.ipynb)](parse-excel-files-python.ipynb)
* [Parse Excel Files in Python HTML](parse-excel-files-python.html)
* [Parse Excel Files in Python Markdown](parse-excel-files-python.md)

Excel Workbook used as an example is available at [corruption_perception_index.xls](res/corruption_perception_index.xls).

To setup your environment for Jupyter notebook see [Using Spark with Jupyter](../using-spark-with-jupyter.html).

## Problem

Very often data coming from business people is encoded in a variety of formats. Excel is one of the most popular formats for exchanging documents. Documents stored in Excel could be well strucutred or unstructured. The former presents challenge in parsing the document and extracting information from it.

## Solution

There are many possible approaches to the problem. We are going to use the `xlrd` library to parse Excel files into Python data structures for further processing or data transformations.

API documentation can be found at: https://xlrd.readthedocs.io/en/latest/

There is a also `xlwr` library for writing Excel files.

## Discussion


### Install `xlrd`

To install `xlrd` library, execute following command in a console:

```bash
pip install xlrd
```

Once installation is complete, you can start using the `xlrd` library to parse Excel files.

### Explore `xlrd` Workbook

For this exploration we will define some configuration settings.


```python
# File name for Excel workbook
workbook_file = 'res/corruption_perception_index.xls'

# Worksheet we are processing
sheet_name = 'CPI 2013'

# Number of rows to skip from the top of the worksheet
skip_rows = 1

# Number of header rows after the skip rows
header_rows = 2

```

We will cover following topics:
* Open Excel workbook file
* Get number of worksheets in a workbook
* Get a list of sheet names from a workbook
* Get a list of Sheet objects from a workbook
* Find a Sheet by name
* Find a Sheet by index

Let's start with Excel workbook. To open a workbook we use [`xlrd.open_workbook()`](https://xlrd.readthedocs.io/en/latest/api.html#xlrd.open_workbook). The function returns an instance of a [`Book`](https://xlrd.readthedocs.io/en/latest/api.html#xlrd.book.Book) class.


```python
import xlrd

workbook = xlrd.open_workbook(workbook_file)

# Workbooks are represented by instances of Book class
workbook
```




    <xlrd.book.Book at 0x2aa7ee7f198>



`Book.nsheets` attribute returns the number of worksheets in the workbook.


```python
# Get the number of sheets in the workbook.
workbook.nsheets
```




    1



`Book.sheets()` method returns a list of sheets in the workbook. Each sheet is represented as a [Sheet](https://xlrd.readthedocs.io/en/latest/api.html#xlrd.sheet.Sheet) object. All sheets not already loaded will be loaded.


```python
# List sheets in workbook and print sheet names
for sheet in workbook.sheets():
    print(sheet.name)
```

    CPI 2013
    

`Book.sheet_names()` method returns a list of sheet names from the workbook. This information is available even when no sheets have yet been loaded.


```python
# Print names of all sheets in workbook.
for sheet_name in workbook.sheet_names():
    print(sheet_name)
```

    CPI 2013
    

`Book.sheet_by_index(sheet_index)` method returns a sheet by given index. `sheet_index` must be in `range(nsheets)`.


```python
# Get the name of the first sheet
workbook.sheet_by_index(0).name
```




    'CPI 2013'




```python
# If sheet index is outside the range, an error is raised.
workbook.sheet_by_index(10).name
```


    ---------------------------------------------------------------------------

    IndexError                                Traceback (most recent call last)

    <ipython-input-18-387a3bcae45d> in <module>
          1 # If sheet index is outside the range, an error is raised.
    ----> 2 workbook.sheet_by_index(10).name
    

    c:\python37\lib\site-packages\xlrd\book.py in sheet_by_index(self, sheetx)
        464         :returns: A :class:`~xlrd.sheet.Sheet`.
        465         """
    --> 466         return self._sheet_list[sheetx] or self.get_sheet(sheetx)
        467 
        468     def sheet_by_name(self, sheet_name):
    

    IndexError: list index out of range


`Book.sheet_by_name(sheet_name)` method returns a sheet by a given name.


```python
# Get the index of a sheet by name
workbook.sheet_by_name(sheet_name).number
```




    0




```python
# If the sheet doesn't exist, an error is raised.
workbook.sheet_by_name('Sheet must exist')
```


    ---------------------------------------------------------------------------

    ValueError                                Traceback (most recent call last)

    c:\python37\lib\site-packages\xlrd\book.py in sheet_by_name(self, sheet_name)
        473         try:
    --> 474             sheetx = self._sheet_names.index(sheet_name)
        475         except ValueError:
    

    ValueError: 'Sheet must exist' is not in list

    
    During handling of the above exception, another exception occurred:
    

    XLRDError                                 Traceback (most recent call last)

    <ipython-input-22-991ea9dd7742> in <module>
          1 # If the sheet doesn't exist, an error is raised.
    ----> 2 workbook.sheet_by_name('Sheet must exist')
    

    c:\python37\lib\site-packages\xlrd\book.py in sheet_by_name(self, sheet_name)
        474             sheetx = self._sheet_names.index(sheet_name)
        475         except ValueError:
    --> 476             raise XLRDError('No sheet named <%r>' % sheet_name)
        477         return self.sheet_by_index(sheetx)
        478 
    

    XLRDError: No sheet named <'Sheet must exist'>


There are many other methods. For more details, see the API documentation.

### Explore `xlrd` Sheet

To work with Excel workbook sheets, [Sheet](https://xlrd.readthedocs.io/en/latest/api.html#xlrd.sheet.Sheet) class is used.

We will cover following topics:
* Get sheet information:
  - Number of rows: `.nrows` attribute
  - Number of colums: `.ncols` attribute
  - Sheet name: `.name` attribute
* Access cells columnwise:
  - `.col(colx)` method for Cell sequence
  - `.col_values(colx, start_rowx, end_rowx)` method for value slice
  - `.col_slice(colx, start_rowx, end_rowx)` method for Cell slice
* Access cells rowwise:
  - `.row(rowx)` method for Cell sequence
  - `.row_values(rowx, start_colx, end_colx)` method for value slice
  - `.row_slice(rowx, start_rowx, end_rowx)` method for Cell slice
  - `.get_rows()` generator for iterating over rows
* Access individual cells:
  - `.cell(rowx, colx)` method for Cell object
  - `.cell_value(rowx, colx)` method for cell value


```python
sheet = workbook.sheet_by_name(sheet_name)

# Sheet is an instance of Sheet class
sheet
```




    <xlrd.sheet.Sheet at 0x2aa7eed9be0>



`Sheet.ncols` and `Sheet.nrows` attributes return number of columns and number of rows in a sheet.


```python
print("Sheet {} has {} rows and {} columns.".format(sheet.name, sheet.nrows, sheet.ncols))
```

    Sheet CPI 2013 has 180 rows and 26 columns.
    

`Sheet.col(colx)` returns a list of `Cell` objects, representing cells in the column.


```python
# Get top 3 cells of column 4 (in Excel this is column D)
sheet.col(3)[:3]
```




    [empty:'', empty:'', text:'IFS Code']



`Sheet.book` attribute holds a reference to the workbook.


```python
sheet.book.nsheets
```




    1



`Sheet.cell(rowx,colx)` returns a `Cell` object, representing content of the cell at given row and column.


```python
sheet.cell(3,3)
```




    number:128.0



`Sheet.cell_value(rowx, colx)` returns value of the cell at given row and column. The value is a Python data type.


```python
sheet.cell_value(3,3)
```




    128.0



`Sheet.row(rowx)` returns a sequence of `Cell` objects in a given row.


```python
# Get first 5 Cell objects in row 5
sheet.row(4)[:5]
```




    [number:1.0, text:'New Zealand', text:'NZL', number:196.0, text:'AP']



`Sheet.get_rows()` returns a generator for iterating through each row. Note that there is no `.get_cols()` method.


```python
# Count rows in a sheet iterating over the rows using .get_rows() generator.
nrows = 0
for row in sheet.get_rows():
    nrows += 1
print("Sheet has {} rows.".format(nrows))
```

    Sheet has 180 rows.
    

`Sheet.row_values(rowx, start_colx=0, end_colx=None)` returns a slice of the values of the cells in the given row.


```python
# Get the values from cells 4 to 6 at row 3
sheet.row_values(2)[3:6]

# Same result, passing slice start_colx and end_colx.
sheet.row_values(2,3,6)
```




    ['IFS Code', 'Region', '']



`Sheet.row_slice(rowx, start_colx=0, end_colx=None)` returns a slice of the `Cell` objects in the given row.


```python
# Get Cell objects for cells 4 to 6 at row 3.
sheet.row_slice(2,3,6)
```




    [text:'IFS Code', text:'Region', empty:'']



`Sheet.col_slice(colx, start_rowx=0, end_rowx=None)` returns a slice of the Cell objects in the given column.


```python
# Get Cell objects for rows 4 to 6 at column 3
sheet.col_slice(2,3,6)
```




    [text:'DNK', text:'NZL', text:'FIN']



`Sheet.col_values(colx, start_rowx=0, end_rowx=None)` returns a slice of the values of the cells in the given column.


```python
# Get cell values from rows 4 to 6 at column 3
sheet.col_values(2,3,6)
```




    ['DNK', 'NZL', 'FIN']



`Sheet.col_types(colx, start_rowx=0, end_rowx=None)` returns a slice of the types of the cells in the given column. For the types see [Cell](https://xlrd.readthedocs.io/en/latest/api.html#xlrd.sheet.Cell) class.


```python
# Get cell types from rows 4 to 6 at column 3
sheet.col_types(2,3,6)
```




    [1, 1, 1]



`Sheet.row_types(colx, start_rowx=0, end_rowx=None)` returns a slice of the types of the cells in the given row. For the types see Cell class.


```python
# Get cell types from column 4 to 6 at row 3
sheet.row_types(2,3,6)
```




    array('B', [1, 1, 1])



### Explore `xlrd` Cell

[`Cell`](https://xlrd.readthedocs.io/en/latest/api.html#xlrd.sheet.Cell) objects represent cell content in Excel worksheet.


```python
cell = sheet.cell(5,4)
cell
```




    text:'EU'



`Cell.value` attribute holds the value of the cell.


```python
cell.value
```




    'EU'



`Cell.ctype` attribute holds integer, representing the type of the cell. For more information see [`Cell` API Reference](https://xlrd.readthedocs.io/en/latest/api.html#xlrd.sheet.Cell).


```python
cell.ctype
```




    1



### Excel Reader

We are going to create a generic class which allows to easily process Excel sheets in row-by-row fashion.



```python
class PivanExcelReader:
    """Read Excel Worksheet and iterate over rows calling a given function.
    """
    def __init__(self, fname, sheet_name, skip_rows = 0, header_rows = 0):
        self.fname = fname
        self.skip_rows = skip_rows
        self.header_rows = header_rows
        self.book = xlrd.open_workbook(fname)
        self.sheet_name = sheet_name
        self.sheet = self.book.sheet_by_name(sheet_name)
        self.nrow = 0
    
    def getMergedHeader(self):
        """Returns a sequence of cell values for a header row.
        
        If the header contains multiple rows, cells are concatenated vertically.
        Cell values are speared, using space ' ' character.
        """
        skip_rows, header_rows = (self.skip_rows, self.header_rows)
        if self.header_rows < 1:
            raise ValueError('header_rows must be greater than 0 to call getMergedHeader()')
        header_row = [cell for cell in sheet.row_values(skip_rows)]
        for nrow in range(skip_rows + 1, skip_rows + header_rows):
            row = sheet.row_values(nrow)
            for ncell in range(sheet.ncols):
                if header_row[ncell] != '' and row[ncell] != '':
                    header_row[ncell] = header_row[ncell] + ' ' + row[ncell]
                else:
                    header_row[ncell] = header_row[ncell] + row[ncell]
        return (header_row)
        
    def mapRows(self, func, cell_formatter = None, nrows = None):
        """Iterate over "data" rows in a sheet.
        
        skip_rows + header_rows number of rows are skipped as non-data rows.
        
        Each cell value is processed using the `cell_formatter` function. 
        `cell_formatter` takes one argument - Cell object.
        
        Each row, as a sequence of cell values is passed to the `func` function.
        `func` takes one argument - a sequence of cell values.
        
        If `nrows` is specified, at max `nrows` rows are processed. Otherwise all rows
        are processed.
        """
        if (nrows is None):
            last_rowx = self.sheet.nrows
        else:
            last_rowx = self.skip_rows + self.header_rows + nrows
            if (last_rowx > self.sheet.nrows):
                last_rowx = self.sheet.nrows
        for nrow in range(self.skip_rows + self.header_rows, last_rowx):
            if cell_formatter is None:
                row = self.sheet.row_values(nrow)
            else:
                row = [cell_formatter(cell) for cell in self.sheet.row(nrow)]
            func(row)

```

#### Example: Convert Excel Worksheet to CSV



```python
import csv
import sys
writer = csv.writer(sys.stdout)

def cell_formatter(cell):
    """Format cell values.
    
    Convert float values with zero fractional part to integer.
    """
    val = cell.value
    if cell.ctype == xlrd.XL_CELL_NUMBER:
        if int(val) == val:
            val = int(val)
    return val

per = PivanExcelReader(workbook_file, sheet_name, skip_rows = skip_rows, header_rows = header_rows)
writer.writerow(per.getMergedHeader())
per.mapRows(writer.writerow, cell_formatter=cell_formatter, nrows=5)
```

    Country Rank,Country / Territory,WB Code,IFS Code,Region,Country Rank,CPI 2013 Score,Surveys Used,Standard Error,90% Confidence interval Lower,Upper,Scores range MIN,MAX,Data sources AFDB,BF (SGI),BF (BTI),IMD,ICRG,WB,WEF,WJP,EIU,GI,PERC,TI,FH
    1,Denmark,DNK,128,EU,1,91,7,2.2,87,95,83,98,0,97,0,96,98,0,87,89,88,83,0,0,0
    1,New Zealand,NZL,196,AP,1,91,7,2.3,87,95,83,98,0,97,0,89,98,0,96,86,88,83,0,0,0
    3,Finland,FIN,172,EU,3,89,7,1.7,86,92,83,98,0,89,0,89,98,0,90,87,88,83,0,0,0
    3,Sweden,SWE,144,EU,3,89,7,2.3,85,93,83,98,0,97,0,83,98,0,86,89,88,83,0,0,0
    5,Norway,NOR,142,EU,5,86,7,2.3,82,90,80,98,0,81,0,80,98,0,85,88,88,83,0,0,0



```python

```
