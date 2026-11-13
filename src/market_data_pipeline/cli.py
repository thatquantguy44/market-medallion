"""Pipeline-aware CLI; legacy unprefixed commands keep their AAII behavior."""

import sys

from market_data_pipeline.pipelines.aaii.cli import main as aaii_main


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments[:1] == ["presidential"]:
        from market_data_pipeline.pipelines.presidential_research.cli import (
            main as presidential_main,
        )

        return presidential_main(arguments[1:])
    if arguments[:1] == ["fomc"]:
        from market_data_pipeline.pipelines.fomc_meeting_minutes.cli import main as fomc_main

        return fomc_main(arguments[1:])
    return aaii_main(arguments)
