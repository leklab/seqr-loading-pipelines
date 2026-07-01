import luigi

from loading_pipeline.lib.misc.clickhouse import (
    ClickhouseReferenceDataset,
)

from loading_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)


@luigi.util.inherits(BaseLoadingPipelineParams)
class BaseReferenceUpdateParams(luigi.Task):
    # The difference between the "Loading Run" params
    # and the "Loading Pipeline" params:
    # - These params are used during standard "runs"
    # of the pipeline that add a callset to the backend
    # data store.
    # - The "Loading Pipeline" params are shared with
    # tasks that may remove data from or change the
    # structure of the persisted Hail Tables.
    run_id = luigi.Parameter()
    clickhouse_reference_dataset = luigi.EnumParameter(enum=ClickhouseReferenceDataset)
