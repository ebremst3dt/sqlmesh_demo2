
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AR': 'numeric',
 'AR_TEXT': 'varchar(30)',
 'BOKFORINGSAR': 'datetime',
 'BOKFORINGSARSLUT': 'datetime',
 'BOKFORINGSAR_TEXT': 'varchar(30)',
 'DAG': 'numeric',
 'DAG_TEXT': 'varchar(2)',
 'DATUM6B_TEXT': 'varchar(6)',
 'DATUM6_TEXT': 'varchar(6)',
 'DATUM8_TEXT': 'varchar(8)',
 'KVARTAL': 'numeric',
 'KVARTALNR': 'numeric',
 'KVARTALNR_TEXT': 'varchar(30)',
 'KVARTAL_TEXT': 'varchar(30)',
 'MANAD': 'numeric',
 'MANADNR': 'numeric',
 'MANADNR_TEXT': 'varchar(30)',
 'MANADSNAMN': 'varchar(30)',
 'MANAD_TEXT': 'varchar(30)',
 'PERIOD': 'datetime',
 'PERIODSLUT': 'datetime',
 'PERIODSTATUS': 'varchar(1)',
 'PERIODSTATUS_TEXT': 'varchar(30)',
 'PERIOD_TEXT': 'varchar(30)',
 'RANTEDEBDATUM': 'datetime',
 'RANTEDEBDATUM_TEXT': 'varchar(30)',
 'TERTIAL': 'numeric',
 'TERTIALNR': 'numeric',
 'TERTIALNR_TEXT': 'varchar(30)',
 'TERTIAL_TEXT': 'varchar(30)',
 'VECKA': 'numeric',
 'VECKA_TEXT': 'varchar(3)',
 'VECKODAG': 'numeric',
 'VECKODAG_TEXT': 'varchar(30)',
 'VECKONR': 'numeric',
 'VECKONR_TEXT': 'varchar(30)',
 'VECKO_TEXT': 'varchar(30)'},
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
	SELECT top 1000
 		CAST(AR AS VARCHAR(MAX)) AS ar,
		CAST(AR_TEXT AS VARCHAR(MAX)) AS ar_text,
		CAST(BOKFORINGSAR AS VARCHAR(MAX)) AS bokforingsar,
		CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS bokforingsar_text,
		CAST(BOKFORINGSARSLUT AS VARCHAR(MAX)) AS bokforingsarslut,
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
		CAST(PERIOD AS VARCHAR(MAX)) AS period,
		CAST(PERIOD_TEXT AS VARCHAR(MAX)) AS period_text,
		CAST(PERIODSLUT AS VARCHAR(MAX)) AS periodslut,
		CAST(PERIODSTATUS AS VARCHAR(MAX)) AS periodstatus,
		CAST(PERIODSTATUS_TEXT AS VARCHAR(MAX)) AS periodstatus_text,
		CAST(RANTEDEBDATUM AS VARCHAR(MAX)) AS rantedebdatum,
		CAST(RANTEDEBDATUM_TEXT AS VARCHAR(MAX)) AS rantedebdatum_text,
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
	FROM utdata.utdata295.RK_DIM_RANTEDATUM
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
