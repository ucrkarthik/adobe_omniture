## Getting Started
This is a PySpark application that helps calculate how much revenue the client is getting from external Search Engines,
such as Google, Yahoo and MSN and which keywords are performing the best based on revenue.

#### Environment Variables
Verify the SPARK and PYTHON environment variables in the .bash_profile file are up-to-date:
```bash
export PYTHON_HOME=/usr/local/Cellar/python/3.7.2_2
export PATH=$PYTHON_HOME/bin:$PATH

export SPARK_HOME=/Users/karthik/spark-2.3.3-bin-hadoop2.7

export IPYTHON=1
export PYSPARK_PYTHON=$PYTHON_HOME
export PYSPARK_DRIVER_PYTHON=$PYTHON_HOME
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip
```

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
Copy the repo's files into the environment:
```bash
python3 setup.py install
```

#### Execute
Run the "Search Engine Revenue" spark job with spark-submit. The 'source' and 'target' path specify the location where the source file is located and the target folder where
the tsv file will be stored.
Here is an example of how to execute the job locally:
```bash
spark-submit adobe/omniture/se_revenue/se_revenue_driver.py --source "tests/resources/data.sql" --target "tests/results/"
```
The results of the run will stored in the target folder path as a CSV file . The CSV file will have the following file name prefix(and a csv extension): part-00000-*.csv
```bash
(venv) C02VC1CAHTDD:results kavenkatesan$ pwd
/Users/kavenkatesan/dev/adobe_omniture/tests/results
(venv) C02VC1CAHTDD:results kavenkatesan$ ls -l
total 8
-rw-r--r--  1 kavenkatesan  staff  121 Mar 19 08:31 2019-03-19_SearchKeywordPerformance.tsv
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
A target folder with the tsv file will also be generated.
```bash
(venv) C02VC1CAHTDD:results kavenkatesan$ pwd
/Users/kavenkatesan/dev/adobe_omniture/tests/results
(venv) C02VC1CAHTDD:results kavenkatesan$ ls -l
total 8
-rw-r--r--  1 kavenkatesan  staff  121 Mar 19 08:31 2019-03-19_SearchKeywordPerformance.tsv
```

Note that Python3, Java 8 & Spark 2.3.3 must be installed for `pytest` to run Spark: some unit tests use Spark.



#### AWS-EMR (Work in Progress)
The instructions below are to help deploy the artifact in the AWS EMR cluster. The best approach for deploying a python
application in AWS EMR will be to pull the artifact from an artifact repository during the cluster bootstrap. Since we
dont have access to an artifact repository, the artifact file and the bootstrap script file will be stored in S3. During the
cluster creationg, bootstrap execution will pull the artifact from S3 and install it in all the nodes on the cluster.

Run the following command to create the project archive file in a tar.gz file.
```bash
python3 setup.py sdist
```
Use the aws-cli command to copy the bootstrap script and project archive file to a location in S3.
```bash
aws s3 cp aws_emr/bootstrap.sh s3://adobeomniture/bootstrap.sh
aws s3 cp dist/adobe-omniture-1.0.0.tar.gz s3://adobeomniture/adobe-omniture-1.0.0.tar.gz
```
Change directory to aws_emr folder and run the aws_emr_script python script to create the cluster and submit the step:
```bash
cd aws_emr
python3 aws_emr_script.py
```
