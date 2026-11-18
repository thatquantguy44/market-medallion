"""Inspect/provision source-owned PostgreSQL namespaces; never activate pipelines.

Run with the password variable named by --config already exported. --apply is
explicit and intended only after a verified operator backup. Existing data and
reader grants are preserved; FOMC object ownership moves to its dedicated writer.
"""

import argparse
import json
import os
from pathlib import Path

import psycopg
import yaml
from psycopg import sql

TARGETS = {
    "aaii": "aaii_writer",
    "fomc_meeting_minutes": "fomc_writer",
    "sec_ftd": "sec_ftd_writer",
    "reg_sho_thresholds": "reg_sho_writer",
    "finra_short_sale_volume": "finra_ssv_writer",
    "finra_otc_transparency": "finra_otc_writer",
    "presidential_research": "presidential_writer",
    "naaim_exposure_public_delayed": "naaim_writer",
}


def inventory(db):
    return [
        dict(zip(("schema", "owner", "tables"), r, strict=True))
        for r in db.execute(
            "SELECT n.nspname,pg_get_userbyid(n.nspowner),"
            "count(c.oid) FILTER(WHERE c.relkind IN ('r','p')) "
            "FROM pg_namespace n LEFT JOIN pg_class c ON c.relnamespace=n.oid "
            "WHERE n.nspname=ANY(%s) GROUP BY n.oid ORDER BY n.nspname",
            (list(TARGETS),),
        )
    ]


def provision(db, login):
    before = inventory(db)
    reader = db.execute("SELECT nspacl::text FROM pg_namespace WHERE nspname='aaii'").fetchone()
    for schema, role in TARGETS.items():
        current = db.execute(
            "SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls "
            "FROM pg_roles WHERE rolname=%s",
            (role,),
        ).fetchone()
        if current is None:
            db.execute(
                sql.SQL(
                    "CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(role))
            )
        elif any(current):
            raise ValueError("Existing writer has unexpected elevated/login privileges")
        db.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(role), sql.Identifier(login)))
        owner = db.execute(
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=%s", (schema,)
        ).fetchone()
        if owner is None:
            db.execute(
                sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
                    sql.Identifier(schema), sql.Identifier(role)
                )
            )
        elif owner[0] != role:
            if schema != "fomc_meeting_minutes" or owner[0] != login:
                raise ValueError("Existing namespace has an unexpected owner")
            db.execute(
                sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                    sql.Identifier(schema), sql.Identifier(role)
                )
            )
            kinds = {
                "r": "TABLE",
                "p": "TABLE",
                "v": "VIEW",
                "m": "MATERIALIZED VIEW",
                "S": "SEQUENCE",
                "f": "FOREIGN TABLE",
            }
            for name, kind in db.execute(
                "SELECT relname,relkind FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=%s AND c.relkind::text=ANY(%s) "
                "AND (c.relkind<>'S' OR NOT EXISTS "
                "(SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass "
                "AND d.objid=c.oid AND d.deptype IN ('a','i')))",
                (schema, list(kinds)),
            ):
                db.execute(
                    sql.SQL("ALTER " + kinds[kind] + " {}.{} OWNER TO {}").format(
                        sql.Identifier(schema), sql.Identifier(name), sql.Identifier(role)
                    )
                )
        db.execute(sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(schema)))
    after = inventory(db)
    assert {v["schema"]: v["tables"] for v in before} == {
        v["schema"]: v["tables"] for v in after if v["schema"] in {x["schema"] for x in before}
    }
    assert (
        db.execute("SELECT nspacl::text FROM pg_namespace WHERE nspname='aaii'").fetchone()
        == reader
    )
    for own, role in TARGETS.items():
        for schema in TARGETS:
            grants = db.execute(
                "SELECT has_schema_privilege(%s,%s,'USAGE'),has_schema_privilege(%s,%s,'CREATE')",
                (role, schema, role, schema),
            ).fetchone()
            assert grants == (own == schema, own == schema), (role, schema, grants)
    return after


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = yaml.safe_load(args.config.read_text())["postgres"]
    with psycopg.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["user"],
        password=os.environ[config["password_env"]],
        connect_timeout=5,
    ) as db:
        if not args.apply:
            db.execute("SET TRANSACTION READ ONLY")
        result = {
            "database": config["database"],
            "schemas": provision(db, config["user"]) if args.apply else inventory(db),
            "applied": args.apply,
        }
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
