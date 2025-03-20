pip install wheel setuptools


Change RDS URL :
-------------------
glue_config.py change rds url 


Build 
----------------
python setup.py bdist_wheel


Copy wheel file to S3
-----------------------

aws s3 cp dist/customer_analytics-0.1.0-py3-none-any.whl s3://feb2025-training-bucket/code/customer_analytics/



Copy glue script files 
-----------------------

aws s3 cp glue_upload_script s3://feb2025-training-bucket/code/customer_analytics/ --recursive
