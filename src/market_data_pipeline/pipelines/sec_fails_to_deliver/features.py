"""Coverage-labeled FTD balance features; never reinterpret balances as flows."""

from collections import defaultdict


def prior_balance_changes(rows):
    grouped = defaultdict(list)
    for row in rows:
        if row["row_state"] == "REPORTED":
            grouped[row["cusip"]].append(row)
    output = []
    for cusip, values in grouped.items():
        values.sort(key=lambda row: row["settlement_date"])
        previous = None
        for row in values:
            output.append(
                {
                    "cusip": cusip,
                    "settlement_date": row["settlement_date"],
                    "quantity_fails": row["quantity_fails"],
                    "prior_observation_date": previous["settlement_date"] if previous else None,
                    "prior_quantity_fails": previous["quantity_fails"] if previous else None,
                    "balance_change": (
                        row["quantity_fails"] - previous["quantity_fails"] if previous else None
                    ),
                    "observation_gap_days": (
                        (row["settlement_date"] - previous["settlement_date"]).days
                        if previous
                        else None
                    ),
                    "interpretation": "OUTSTANDING_BALANCE_CHANGE_NOT_NEW_FAILS",
                }
            )
            previous = row
    return sorted(output, key=lambda row: (row["settlement_date"], row["cusip"]))


def coverage_qualified_counts(rows, complete_dates):
    complete_dates = set(complete_dates)
    counts = defaultdict(int)
    for row in rows:
        if row["row_state"] == "REPORTED" and row["settlement_date"] in complete_dates:
            counts[row["cusip"]] += 1
    return dict(sorted(counts.items()))
