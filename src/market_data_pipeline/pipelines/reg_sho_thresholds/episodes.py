"""Coverage-qualified observed threshold membership episodes."""

from datetime import date


def derive_episodes(membership, coverage, expected_dates):
    dates = tuple(sorted(set(expected_dates)))
    if any(type(item) is not date for item in dates):
        raise ValueError("episode calendar requires date labels")
    coverage_by_scope = {}
    for row in coverage:
        key = (row["provider"], row["market"])
        coverage_by_scope.setdefault(key, {})[row["list_date"]] = row["coverage_state"]
    present = {}
    for row in membership:
        if row["row_state"] != "REPORTED":
            continue
        scope = (row["provider"], row["market"])
        key = (*scope, row["raw_symbol"], row["rule_family"])
        present.setdefault(key, set()).add(row["list_date"])
    episodes = []
    for key, membership_dates in sorted(present.items()):
        provider, market, symbol, rule = key
        scope_coverage = coverage_by_scope.get((provider, market), {})
        active = None
        previous_state = None
        for current in dates:
            coverage_state = scope_coverage.get(current)
            if coverage_state not in ("COMPLETE", "COMPLETE_EMPTY"):
                state = "GAP"
            else:
                state = "PRESENT" if current in membership_dates else "ABSENT"
            if state == "PRESENT":
                if active is None:
                    active = {
                        "provider": provider,
                        "market": market,
                        "raw_symbol": symbol,
                        "rule_family": rule,
                        "start_date": current,
                        "end_date": current,
                        "observed_present_count": 1,
                        "entry_confirmed": previous_state == "ABSENT",
                        "exit_confirmed": False,
                        "left_censored": previous_state != "ABSENT",
                        "right_censored": False,
                        "gap_before": previous_state == "GAP",
                        "gap_after": False,
                    }
                else:
                    active["end_date"] = current
                    active["observed_present_count"] += 1
            elif active is not None:
                active["exit_confirmed"] = state == "ABSENT"
                active["right_censored"] = state == "GAP"
                active["gap_after"] = state == "GAP"
                episodes.append(active)
                active = None
            previous_state = state
        if active is not None:
            active["right_censored"] = True
            episodes.append(active)
    return episodes
