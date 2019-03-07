## Getting Started
This is a PySpark application that helps calculate how much revenue the client is getting from external Search Engines, 
such as Google, Yahoo and MSN and which keywords are performing the best based on revenue.

#### Environment
Setup should be performed in a python 3 virtualenv. 

The following command makes the environment:
```bash
python3 -m venv /path/to/new/virtual/environment
```

After creation, you must activate the environment:
```bash
source /path/to/new/virtual/environment/bin/activate 
```

#### Requirements
Once in the environment, install all project requirements:
```bash
pip install -r requirements.txt
```

#### Build
CD into the adobe_ominiture repo and copy the repo's files into the environment:
```bash
python setup.py install
```

#### Execute
Run the "Search Engine Revenue" spark job with spark-submit. The 'source' and 'target' path specify the location where the source file is located and the target folder where 
the results can be stored. The 'target' folder path requires the word 'DATE' in the name, so the system can replace the word('DATE') with the current date(2019-02-07). 
Here is an example of how to execute the job locally: 
```bash
spark-submit adobe/omniture/se_revenue/se_revenue_driver.py --source "tests/resources/data.sql" --target "tests/results/DATE_SearchKeywordPerformance.tab"
```
The results of the run will stored in the target folder path as a CSV file . The CSV file will have the following file name prefix(and a csv extension): part-00000-*.csv 
```bash
C02VC1CAHTDD: kavenkatesan$ pwd
/Users/kavenkatesan/dev/adobe_omniture/tests/results/2019-03-07_SearchKeywordPerformance.tab
C02VC1CAHTDD: kavenkatesan$ ls -l
total 8
-rw-r--r--  1 kavenkatesan  staff    0 Mar  7 09:36 _SUCCESS
-rw-r--r--  1 kavenkatesan  staff  114 Mar  7 09:36 part-00000-93e65a6d-6eb3-4f1e-9e63-79420e50c688-c000.csv
```

#### Pytest

To run all project unit tests, execute:
```bash
pytest tests/
```
Here is an example of the output with the file dataframe schema and the output:
```markdown
==================================================================================================================== test session starts =====================================================================================================================
platform darwin -- Python 3.6.5, pytest-3.3.0, py-1.8.0, pluggy-0.6.0 -- /Users/kavenkatesan/dev/adobe_omniture/venv/bin/python
cachedir: .cache
rootdir: /Users/kavenkatesan/dev/adobe_omniture, inifile: setup.cfg
plugins: mock-1.10.0, cov-2.5.1
collected 3 items                                                                                                                                                                                                                                            

tests/test_bad_schema.py::test_schema_check 2019-03-07 11:46:53 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
2019-03-07 11:46:58 WARN  ObjectStore:6666 - Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 1.2.0
2019-03-07 11:46:58 WARN  ObjectStore:568 - Failed to get database default, returning NoSuchObjectException
2019-03-07 11:46:59 WARN  ObjectStore:568 - Failed to get database global_temp, returning NoSuchObjectException
2019-03-07 11:47:02,139.139 INFO /Users/kavenkatesan/dev/adobe_omniture/tests/test_bad_schema.py test_bad_schema - test_schema_check: Corrupt Recoreds Received: 1
PASSED                                                                                                                                                                                                     [ 33%]
tests/test_df_transformer.py::test_unpack_product_list_and_capture_search_keyword PASSED                                                                                                                                                               [ 66%]
tests/test_se_revenue.py::test_se_revenue_pipeline 2019-03-07 11:47:03 WARN  CacheManager:66 - Asked to cache already cached data.
2019-03-07 11:47:03,715.715 INFO /Users/kavenkatesan/dev/adobe_omniture/adobe/omniture/se_revenue/se_revenue_driver.py se_revenue_driver - run_job: Corrupt Recoreds Received: 0
2019-03-07 11:47:03,855.855 INFO /Users/kavenkatesan/dev/adobe_omniture/adobe/omniture/se_revenue/se_revenue_driver.py se_revenue_driver - run_job: Records Processed: 21
root
 |-- Search Engine Domain: string (nullable = true)
 |-- Search Keyword: string (nullable = true)
 |-- Revenue: string (nullable = true)

+--------------------+--------------+-------+                                   
|Search Engine Domain|Search Keyword|Revenue|
+--------------------+--------------+-------+
|      www.google.com|          Ipod|    290|
|        www.bing.com|          Zune|    250|
|      www.google.com|          ipod|    190|
+--------------------+--------------+-------+

PASSED                                                                                                                                                                                              [100%]

================================================================================================================= 3 passed in 20.59 seconds ==================================================================================================================
```
A target folder with the csv file will also be generated. 
```bash
C02VC1CAHTDD: kavenkatesan$ pwd
/Users/kavenkatesan/dev/adobe_omniture/tests/results/2019-03-07_SearchKeywordPerformance.tab
C02VC1CAHTDD: kavenkatesan$ ls -l
total 8
-rw-r--r--  1 kavenkatesan  staff    0 Mar  7 09:36 _SUCCESS
-rw-r--r--  1 kavenkatesan  staff  114 Mar  7 09:36 part-00000-93e65a6d-6eb3-4f1e-9e63-79420e50c688-c000.csv
```

Note that Java 8 & Spark 2.2.1 must be installed for `pytest` to run Spark: some unit tests use Spark.