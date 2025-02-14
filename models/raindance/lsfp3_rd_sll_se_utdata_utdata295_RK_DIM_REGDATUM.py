
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'AR': 'varchar(max)',
 'AR_TEXT': 'varchar(max)',
 'BOKFORINGSAR': 'varchar(max)',
 'BOKFORINGSARSLUT': 'varchar(max)',
 'BOKFORINGSAR_TEXT': 'varchar(max)',
 'DAG': 'varchar(max)',
 'DAG_TEXT': 'varchar(max)',
 'DATUM6B_TEXT': 'varchar(max)',
 'DATUM6_TEXT': 'varchar(max)',
 'DATUM8_TEXT': 'varchar(max)',
 'KVARTAL': 'varchar(max)',
 'KVARTALNR': 'varchar(max)',
 'KVARTALNR_TEXT': 'varchar(max)',
 'KVARTAL_TEXT': 'varchar(max)',
 'MANAD': 'varchar(max)',
 'MANADNR': 'varchar(max)',
 'MANADNR_TEXT': 'varchar(max)',
 'MANADSNAMN': 'varchar(max)',
 'MANAD_TEXT': 'varchar(max)',
 'PERIOD': 'varchar(max)',
 'PERIODSLUT': 'varchar(max)',
 'PERIODSTATUS': 'varchar(max)',
 'PERIODSTATUS_TEXT': 'varchar(max)',
 'PERIOD_TEXT': 'varchar(max)',
 'REGDATUM': 'varchar(max)',
 'REGDATUM_TEXT': 'varchar(max)',
 'TERTIAL': 'varchar(max)',
 'TERTIALNR': 'varchar(max)',
 'TERTIALNR_TEXT': 'varchar(max)',
 'TERTIAL_TEXT': 'varchar(max)',
 'VECKA': 'varchar(max)',
 'VECKA_TEXT': 'varchar(max)',
 'VECKODAG': 'varchar(max)',
 'VECKODAG_TEXT': 'varchar(max)',
 'VECKONR': 'varchar(max)',
 'VECKONR_TEXT': 'varchar(max)',
 'VECKO_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(AR AS VARCHAR(MAX)) AS AR,
CAST(AR_TEXT AS VARCHAR(MAX)) AS AR_TEXT,
CAST(BOKFORINGSAR AS VARCHAR(MAX)) AS BOKFORINGSAR,
CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS BOKFORINGSAR_TEXT,
CAST(BOKFORINGSARSLUT AS VARCHAR(MAX)) AS BOKFORINGSARSLUT,
CAST(DAG AS VARCHAR(MAX)) AS DAG,
CAST(DAG_TEXT AS VARCHAR(MAX)) AS DAG_TEXT,
CAST(DATUM6_TEXT AS VARCHAR(MAX)) AS DATUM6_TEXT,
CAST(DATUM6B_TEXT AS VARCHAR(MAX)) AS DATUM6B_TEXT,
CAST(DATUM8_TEXT AS VARCHAR(MAX)) AS DATUM8_TEXT,
CAST(KVARTAL AS VARCHAR(MAX)) AS KVARTAL,
CAST(KVARTAL_TEXT AS VARCHAR(MAX)) AS KVARTAL_TEXT,
CAST(KVARTALNR AS VARCHAR(MAX)) AS KVARTALNR,
CAST(KVARTALNR_TEXT AS VARCHAR(MAX)) AS KVARTALNR_TEXT,
CAST(MANAD AS VARCHAR(MAX)) AS MANAD,
CAST(MANAD_TEXT AS VARCHAR(MAX)) AS MANAD_TEXT,
CAST(MANADNR AS VARCHAR(MAX)) AS MANADNR,
CAST(MANADNR_TEXT AS VARCHAR(MAX)) AS MANADNR_TEXT,
CAST(MANADSNAMN AS VARCHAR(MAX)) AS MANADSNAMN,
CAST(PERIOD AS VARCHAR(MAX)) AS PERIOD,
CAST(PERIOD_TEXT AS VARCHAR(MAX)) AS PERIOD_TEXT,
CAST(PERIODSLUT AS VARCHAR(MAX)) AS PERIODSLUT,
CAST(PERIODSTATUS AS VARCHAR(MAX)) AS PERIODSTATUS,
CAST(PERIODSTATUS_TEXT AS VARCHAR(MAX)) AS PERIODSTATUS_TEXT,
CAST(REGDATUM AS VARCHAR(MAX)) AS REGDATUM,
CAST(REGDATUM_TEXT AS VARCHAR(MAX)) AS REGDATUM_TEXT,
CAST(TERTIAL AS VARCHAR(MAX)) AS TERTIAL,
CAST(TERTIAL_TEXT AS VARCHAR(MAX)) AS TERTIAL_TEXT,
CAST(TERTIALNR AS VARCHAR(MAX)) AS TERTIALNR,
CAST(TERTIALNR_TEXT AS VARCHAR(MAX)) AS TERTIALNR_TEXT,
CAST(VECKA AS VARCHAR(MAX)) AS VECKA,
CAST(VECKA_TEXT AS VARCHAR(MAX)) AS VECKA_TEXT,
CAST(VECKO_TEXT AS VARCHAR(MAX)) AS VECKO_TEXT,
CAST(VECKODAG AS VARCHAR(MAX)) AS VECKODAG,
CAST(VECKODAG_TEXT AS VARCHAR(MAX)) AS VECKODAG_TEXT,
CAST(VECKONR AS VARCHAR(MAX)) AS VECKONR,
CAST(VECKONR_TEXT AS VARCHAR(MAX)) AS VECKONR_TEXT FROM utdata.utdata295.RK_DIM_REGDATUM"""
    return pipe(query=query)
