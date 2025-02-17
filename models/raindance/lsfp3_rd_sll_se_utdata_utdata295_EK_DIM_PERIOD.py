
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
 'PERIOD10_TEXT': 'varchar(10)',
 'PERIOD4_TEXT': 'varchar(4)',
 'PERIOD6B_TEXT': 'varchar(6)',
 'PERIOD6_TEXT': 'varchar(6)',
 'PERIOD7_TEXT': 'varchar(7)',
 'PERIOD8_TEXT': 'varchar(8)',
 'PERIODSLUT': 'datetime',
 'PERIODSTATUS': 'varchar(1)',
 'PERIODSTATUS_TEXT': 'varchar(30)',
 'PERIOD_TEXT': 'varchar(30)',
 'TERTIAL': 'numeric',
 'TERTIALNR': 'numeric',
 'TERTIALNR_TEXT': 'varchar(30)',
 'TERTIAL_TEXT': 'varchar(30)'},
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
		CONVERT(varchar(max), BOKFORINGSAR, 126) AS bokforingsar,
		CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS bokforingsar_text,
		CONVERT(varchar(max), BOKFORINGSARSLUT, 126) AS bokforingsarslut,
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
		CAST(PERIOD10_TEXT AS VARCHAR(MAX)) AS period10_text,
		CAST(PERIOD4_TEXT AS VARCHAR(MAX)) AS period4_text,
		CAST(PERIOD6_TEXT AS VARCHAR(MAX)) AS period6_text,
		CAST(PERIOD6B_TEXT AS VARCHAR(MAX)) AS period6b_text,
		CAST(PERIOD7_TEXT AS VARCHAR(MAX)) AS period7_text,
		CAST(PERIOD8_TEXT AS VARCHAR(MAX)) AS period8_text,
		CONVERT(varchar(max), PERIODSLUT, 126) AS periodslut,
		CAST(PERIODSTATUS AS VARCHAR(MAX)) AS periodstatus,
		CAST(PERIODSTATUS_TEXT AS VARCHAR(MAX)) AS periodstatus_text,
		CAST(TERTIAL AS VARCHAR(MAX)) AS tertial,
		CAST(TERTIAL_TEXT AS VARCHAR(MAX)) AS tertial_text,
		CAST(TERTIALNR AS VARCHAR(MAX)) AS tertialnr,
		CAST(TERTIALNR_TEXT AS VARCHAR(MAX)) AS tertialnr_text 
	FROM utdata.utdata295.EK_DIM_PERIOD
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
