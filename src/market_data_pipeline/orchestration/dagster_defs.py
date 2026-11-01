"""Repository Dagster entrypoint; each pipeline owns its assets and schedules."""

import dagster as dg

from market_data_pipeline.pipelines.aaii.dagster_defs import (
    AaiiConfig as AaiiConfig,
)
from market_data_pipeline.pipelines.aaii.dagster_defs import (
    aaii_sentiment as aaii_sentiment,
)
from market_data_pipeline.pipelines.aaii.dagster_defs import defs as aaii_defs
from market_data_pipeline.pipelines.aaii.dagster_defs import (
    job as job,
)
from market_data_pipeline.pipelines.aaii.dagster_defs import (
    live_schedule as live_schedule,
)
from market_data_pipeline.pipelines.aaii.dagster_defs import (
    schedule as schedule,
)
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.dagster_defs import (
    defs as naaim_defs,
)

defs = dg.Definitions.merge(aaii_defs, naaim_defs)
