pip install wheel setuptools



Build 
----------------
python setup.py bdist_wheel


Copy wheel file to S3
-----------------------

aws s3 cp dist/customer_analytics-0.1.0-py3-none-any.whl s3://feb2025-training-bucket/code/customer_analytics/
