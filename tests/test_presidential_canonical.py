"""PR016-R06: reproducible JavaScript-compatible numeric identity."""

import math

import pytest

from market_data_pipeline.pipelines.presidential_research.canonical import canonical_json


def test_canonical_numbers_match_javascript_contract():
    assert (
        canonical_json({"z": -0.0, "a": [1.0, 1e-7, 1e-6, 1e20, 1e21]})
        == '{"a":[1,1e-7,0.000001,100000000000000000000,1e+21],"z":0}'
    )
    assert canonical_json({"text": "é\ud800"}) == '{"text":"é\\ud800"}'


@pytest.mark.parametrize("value", [math.inf, math.nan, 9007199254740993])
def test_canonical_rejects_nonfinite_or_unsafe_integer(value):
    with pytest.raises(ValueError):
        canonical_json(value)
