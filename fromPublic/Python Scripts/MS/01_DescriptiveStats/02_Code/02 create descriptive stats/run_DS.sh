#!/bin/bash
#!/usr/bin/php

sudo spark-submit --packages com.databricks:spark-csv_2.11:1.3.0 --deploy-mode client --master yarn Descriptive_Statistics_DS_CS.py 'DS' 0
sudo spark-submit --packages com.databricks:spark-csv_2.11:1.3.0 --deploy-mode client --master yarn Descriptive_Statistics_DS_CS.py 'DS' 1
sudo spark-submit --packages com.databricks:spark-csv_2.11:1.3.0 --deploy-mode client --master yarn Descriptive_Statistics_DS_CS.py 'DS' 2
sudo spark-submit --packages com.databricks:spark-csv_2.11:1.3.0 --deploy-mode client --master yarn Descriptive_Statistics_DS_CS.py 'DS' 3
sudo spark-submit --packages com.databricks:spark-csv_2.11:1.3.0 --deploy-mode client --master yarn Descriptive_Statistics_DS_CS.py 'DS' 4
