
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


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
    kind=ModelKindName.FULL,
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """
	SELECT TOP 1000 top 1000
 		CAST(AR AS VARCHAR(MAX)) AS ar,
		CAST(AR_TEXT AS VARCHAR(MAX)) AS ar_text,
		CONVERT(varchar(max), BOKFORINGSAR, 126) AS bokforingsar,
		CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS bokforingsar_text,
		CONVERT(varchar(max), BOKFORINGSARSLUT, 126) AS bokforingsarslut,
		CAST(DAG AS VARCHAR(MAX)) AS dag,
		CAST(DAG_TEXT AS VARCHAR(MAX)) AS dag_text,
		CAST(DATUM6_TEXT AS VARCHAR(MAX)) AS datum6_text,
		CAST(DATUM6B_TEXT AS VARCHAR(MAX)) AS datum6b_text,
		CAST(DATUM8_TEXT AS VARCHAR(MAX)) AS datum8_text,
		CAST(KVARTAL AS VARCHAR(MAX)) AS kvartal,
		CAST(KVARTAL_TEXT AS VARCHAR(MAX)) AS kvartal_text,
		CAST(KVARTALNR AS VARCHAR(MAX)) AS kvartalnr,
		CAST(KVARTALNR_TEXT AS VARCHAR(MAX)) AS kvartalnr_text,
		CAST(MANAD AS VARCHAR(MAX)) AS manad,
		CAST(MANAD_TEXT AS VARCHAR(MAX)) AS manad_text,
		CAST(MANADNR AS VARCHAR(MAX)) AS manadnr,
		CAST(MANADNR_TEXT AS VARCHAR(MAX)) AS manadnr_text,
		CAST(MANADSNAMN AS VARCHAR(MAX)) AS manadsnamn,
		CONVERT(varchar(max), PERIOD, 126) AS period,
		CAST(PERIOD_TEXT AS VARCHAR(MAX)) AS period_text,
		CONVERT(varchar(max), PERIODSLUT, 126) AS periodslut,
		CAST(PERIODSTATUS AS VARCHAR(MAX)) AS periodstatus,
		CAST(PERIODSTATUS_TEXT AS VARCHAR(MAX)) AS periodstatus_text,
		CONVERT(varchar(max), REGDATUM, 126) AS regdatum,
		CAST(REGDATUM_TEXT AS VARCHAR(MAX)) AS regdatum_text,
		CAST(TERTIAL AS VARCHAR(MAX)) AS tertial,
		CAST(TERTIAL_TEXT AS VARCHAR(MAX)) AS tertial_text,
		CAST(TERTIALNR AS VARCHAR(MAX)) AS tertialnr,
		CAST(TERTIALNR_TEXT AS VARCHAR(MAX)) AS tertialnr_text,
		CAST(VECKA AS VARCHAR(MAX)) AS vecka,
		CAST(VECKA_TEXT AS VARCHAR(MAX)) AS vecka_text,
		CAST(VECKO_TEXT AS VARCHAR(MAX)) AS vecko_text,
		CAST(VECKODAG AS VARCHAR(MAX)) AS veckodag,
		CAST(VECKODAG_TEXT AS VARCHAR(MAX)) AS veckodag_text,
		CAST(VECKONR AS VARCHAR(MAX)) AS veckonr,
		CAST(VECKONR_TEXT AS VARCHAR(MAX)) AS veckonr_text 
	FROM utdata.utdata295.RK_DIM_REGDATUM
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
