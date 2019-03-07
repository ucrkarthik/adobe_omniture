## Getting Started

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
Run the "Search Engine Revenue" spark job with spark-submit. Specify the 'source' and 'target' location as follows:
```bash
spark-submit adobe/omniture/se_revenue/se_revenue_driver.py --source "tests/resources/data.sql" --target "tests/results/DATE_SearchKeywordPerformance.tab"
```

#### Pytest

To run all project unit tests, execute:
```bash
pytest tests/
```

Note that Java 8 & Spark 2.2.1 must be installed for `pytest` to run Spark: some unit tests use Spark.