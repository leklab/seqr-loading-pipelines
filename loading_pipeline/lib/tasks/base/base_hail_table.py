import hail as hl
import luigi

from loading_pipeline.lib.annotations.liftover import remove_liftover
from loading_pipeline.lib.core import Env
from loading_pipeline.lib.logger import get_logger
from loading_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from loading_pipeline.lib.tasks.files import GCSorLocalFolderTarget

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingPipelineParams)
class BaseHailTableTask(luigi.Task):
    def output(self) -> luigi.Target:
        raise NotImplementedError

    def complete(self) -> bool:
        logger.info(f'BaseHailTableTask: checking if {self.output().path} exists')
        return GCSorLocalFolderTarget(self.output().path).exists()

    def init_hail(self):

        # spark configuration
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
                       'spark.local.dir': Env.HAIL_TMP_DIR}

        # Need to use the GCP bucket as temp storage for very large callset joins
        hl.init(tmp_dir=Env.HAIL_TMP_DIR, local_tmpdir=Env.HAIL_TMP_DIR, idempotent=True, spark_conf = spark_conf)
        logger.info(f'Initialized hail w/ tmp_dir {Env.HAIL_TMP_DIR}')

        # Interval ref data join causes shuffle death, this prevents it
        hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001

        # Ensure any cached liftover files within Hail are cleared
        # to provide a clean context free of hidden state.
        # This runs "before" a task to account for situations where
        # the Hail write fails and we do not have the chance to
        # run this method in the "after".
        remove_liftover()


# NB: these are defined over luigi.Task instead of the BaseHailTableTask so that
# they work on file dependencies.


@luigi.Task.event_handler(luigi.Event.DEPENDENCY_DISCOVERED)
def dependency_discovered(task, dependency):
    logger.info(f'{task} dependency_discovered {dependency} at {task.output()}')


@luigi.Task.event_handler(luigi.Event.DEPENDENCY_MISSING)
def dependency_missing(task):
    logger.info(f'{task} dependency_missing at {task.output()}')


@luigi.Task.event_handler(luigi.Event.DEPENDENCY_PRESENT)
def dependency_present(task):
    logger.info(f'{task} dependency_present at {task.output()}')


@luigi.Task.event_handler(luigi.Event.START)
def start(task):
    logger.info(f'{task} start')


@luigi.Task.event_handler(luigi.Event.FAILURE)
def failure(task, _):
    logger.exception(f'{task} failure')


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def success(task):
    logger.info(f'{task} success')
