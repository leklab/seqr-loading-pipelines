#!/usr/bin/env python3
import sys
import hail as hl
import luigi

print("Initializing hail first!")

spark_cluster = 'spark://compute-5-0-0.rc.tch.harvard.edu:7077'
your_temp_dir="/lab-share/RC-Data-Science-NR-e2/Public/HAIL_TEMP_DIR"

spark_conf = { 'spark.driver.port': '60100',
               'spark.blockManager.port': '60200',
               "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
               "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
               'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
               'spark.driver.extraClassPath': 'aws-java-sdk-bundle-1.12.262.jar,hadoop-aws-3.3.4.jar',
               'spark.executor.extraClassPath': 'aws-java-sdk-bundle-1.12.262.jar,hadoop-aws-3.3.4.jar',
               'spark.hadoop.fs.s3a.access.key': '',
               'spark.hadoop.fs.s3a.secret.key': '',
               'spark.hadoop.fs.s3a.endpoint': 's3.amazonaws.com',
               'spark.hadoop.fs.s3a.fast.upload': 'true',
               'spark.hadoop.fs.s3a.path.style.access': 'true',
               'spark.executor.extraJavaOptions': '-Dcom.amazonaws.services.s3.enableV4=true',
               'spark.driver.extraJavaOptions': '-Dcom.amazonaws.services.s3.enableV4=true',
               'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
               'spark.local.dir': your_temp_dir}

hl.init(master=spark_cluster,  tmp_dir=your_temp_dir, local_tmpdir=your_temp_dir, spark_conf = spark_conf)

from loading_pipeline.lib.tasks import *  # noqa: F403

if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
    #sys.exit(1)
