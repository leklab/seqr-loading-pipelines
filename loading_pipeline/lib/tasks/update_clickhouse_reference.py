import luigi
import luigi.util

from loading_pipeline.lib.tasks.base.base_reference_update import (
    BaseReferenceUpdateParams,
)

from loading_pipeline.lib.misc.clickhouse import (
    ClickhouseReferenceDataset,
    refresh_clickhouse_reference_data,
)

@luigi.util.inherits(BaseReferenceUpdateParams)
class UpdateClickhouseReferenceTask(luigi.Task):

    def run(self) -> None:
        refresh_clickhouse_reference_data(
            self.reference_genome,
            self.dataset_type,
            self.run_id,
            self.clickhouse_reference_dataset,
        )
