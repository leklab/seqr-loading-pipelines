#!/usr/bin/env python3
import sys
import hail as hl
import luigi
import os

print("Initializing hail first!")

spark_cluster = os.environ.get('SPARK_CLUSTER', 'spark_cluster')
s3a_access_key = os.environ.get('S3A_ACCESS_KEY', 's3a_access_key')
s3a_secret_key = os.environ.get('S3A_SECRET_KEY', 's3a_secret_key')

os.environ['AWS_ACCESS_KEY_ID'] = s3a_access_key
os.environ['AWS_SECRET_ACCESS_KEY'] = s3a_secret_key

hail_tmp_dir=os.environ.get('HAIL_TMP_DIR', 'hail_tmp_dir')

spark_conf = { 'spark.driver.port': '60100',
               'spark.blockManager.port': '60200',
               "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
               "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
               'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
               'spark.driver.extraClassPath': 'aws-java-sdk-bundle-1.12.262.jar,hadoop-aws-3.3.4.jar',
               'spark.executor.extraClassPath': 'aws-java-sdk-bundle-1.12.262.jar,hadoop-aws-3.3.4.jar',
               'spark.hadoop.fs.s3a.endpoint': 's3.amazonaws.com',
               'spark.hadoop.fs.s3a.fast.upload': 'true',
               'spark.hadoop.fs.s3a.path.style.access': 'true',
               'spark.executor.extraJavaOptions': '-Dcom.amazonaws.services.s3.enableV4=true',
               'spark.driver.extraJavaOptions': '-Dcom.amazonaws.services.s3.enableV4=true',
               'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
               'spark.local.dir': hail_tmp_dir}

#Unelegant work around for some reason it doesn't work when in the dictionary above
spark_conf['spark.hadoop.fs.s3a.access.key'] = str(s3a_access_key)
spark_conf['spark.hadoop.fs.s3a.secret.key'] = str(s3a_secret_key)

hl.init(master=spark_cluster,  tmp_dir=hail_tmp_dir, local_tmpdir=hail_tmp_dir, spark_conf = spark_conf)

from loading_pipeline.lib.tasks import *  # noqa: F403

if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
    #sys.exit(1)
