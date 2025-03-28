
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AR': 'varchar(max)', 'AR_TEXT': 'varchar(max)', 'BOKFORINGSAR': 'varchar(max)', 'BOKFORINGSARSLUT': 'varchar(max)', 'BOKFORINGSAR_TEXT': 'varchar(max)', 'DAG': 'varchar(max)', 'DAG_TEXT': 'varchar(max)', 'DATUM6B_TEXT': 'varchar(max)', 'DATUM6_TEXT': 'varchar(max)', 'DATUM8_TEXT': 'varchar(max)', 'KVARTAL': 'varchar(max)', 'KVARTALNR': 'varchar(max)', 'KVARTALNR_TEXT': 'varchar(max)', 'KVARTAL_TEXT': 'varchar(max)', 'MANAD': 'varchar(max)', 'MANADNR': 'varchar(max)', 'MANADNR_TEXT': 'varchar(max)', 'MANADSNAMN': 'varchar(max)', 'MANAD_TEXT': 'varchar(max)', 'PERIOD': 'varchar(max)', 'PERIODSLUT': 'varchar(max)', 'PERIODSTATUS': 'varchar(max)', 'PERIODSTATUS_TEXT': 'varchar(max)', 'PERIOD_TEXT': 'varchar(max)', 'SENASTBETDATUM': 'varchar(max)', 'SENASTBETDATUM_TEXT': 'varchar(max)', 'TERTIAL': 'varchar(max)', 'TERTIALNR': 'varchar(max)', 'TERTIALNR_TEXT': 'varchar(max)', 'TERTIAL_TEXT': 'varchar(max)', 'VECKA': 'varchar(max)', 'VECKA_TEXT': 'varchar(max)', 'VECKODAG': 'varchar(max)', 'VECKODAG_TEXT': 'varchar(max)', 'VECKONR': 'varchar(max)', 'VECKONR_TEXT': 'varchar(max)', 'VECKO_TEXT': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.FULL
    ),
    cron="@daily"
)

        
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = f"""
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'dsp_rd_sll_se_raindance_udp_udp_150' as _source,
		CAST(AR AS VARCHAR(MAX)) AS AR,
		CAST(AR_TEXT AS VARCHAR(MAX)) AS AR_TEXT,
		CONVERT(varchar(max), BOKFORINGSAR, 126) AS BOKFORINGSAR,
		CONVERT(varchar(max), BOKFORINGSARSLUT, 126) AS BOKFORINGSARSLUT,
		CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS BOKFORINGSAR_TEXT,
		CAST(DAG AS VARCHAR(MAX)) AS DAG,
		CAST(DAG_TEXT AS VARCHAR(MAX)) AS DAG_TEXT,
		CAST(DATUM6B_TEXT AS VARCHAR(MAX)) AS DATUM6B_TEXT,
		CAST(DATUM6_TEXT AS VARCHAR(MAX)) AS DATUM6_TEXT,
		CAST(DATUM8_TEXT AS VARCHAR(MAX)) AS DATUM8_TEXT,
		CAST(KVARTAL AS VARCHAR(MAX)) AS KVARTAL,
		CAST(KVARTALNR AS VARCHAR(MAX)) AS KVARTALNR,
		CAST(KVARTALNR_TEXT AS VARCHAR(MAX)) AS KVARTALNR_TEXT,
		CAST(KVARTAL_TEXT AS VARCHAR(MAX)) AS KVARTAL_TEXT,
		CAST(MANAD AS VARCHAR(MAX)) AS MANAD,
		CAST(MANADNR AS VARCHAR(MAX)) AS MANADNR,
		CAST(MANADNR_TEXT AS VARCHAR(MAX)) AS MANADNR_TEXT,
		CAST(MANADSNAMN AS VARCHAR(MAX)) AS MANADSNAMN,
		CAST(MANAD_TEXT AS VARCHAR(MAX)) AS MANAD_TEXT,
		CONVERT(varchar(max), PERIOD, 126) AS PERIOD,
		CONVERT(varchar(max), PERIODSLUT, 126) AS PERIODSLUT,
		CAST(PERIODSTATUS AS VARCHAR(MAX)) AS PERIODSTATUS,
		CAST(PERIODSTATUS_TEXT AS VARCHAR(MAX)) AS PERIODSTATUS_TEXT,
		CAST(PERIOD_TEXT AS VARCHAR(MAX)) AS PERIOD_TEXT,
		CONVERT(varchar(max), SENASTBETDATUM, 126) AS SENASTBETDATUM,
		CAST(SENASTBETDATUM_TEXT AS VARCHAR(MAX)) AS SENASTBETDATUM_TEXT,
		CAST(TERTIAL AS VARCHAR(MAX)) AS TERTIAL,
		CAST(TERTIALNR AS VARCHAR(MAX)) AS TERTIALNR,
		CAST(TERTIALNR_TEXT AS VARCHAR(MAX)) AS TERTIALNR_TEXT,
		CAST(TERTIAL_TEXT AS VARCHAR(MAX)) AS TERTIAL_TEXT,
		CAST(VECKA AS VARCHAR(MAX)) AS VECKA,
		CAST(VECKA_TEXT AS VARCHAR(MAX)) AS VECKA_TEXT,
		CAST(VECKODAG AS VARCHAR(MAX)) AS VECKODAG,
		CAST(VECKODAG_TEXT AS VARCHAR(MAX)) AS VECKODAG_TEXT,
		CAST(VECKONR AS VARCHAR(MAX)) AS VECKONR,
		CAST(VECKONR_TEXT AS VARCHAR(MAX)) AS VECKONR_TEXT,
		CAST(VECKO_TEXT AS VARCHAR(MAX)) AS VECKO_TEXT 
	FROM raindance_udp.udp_150.RK_DIM_SENASTBETDATUM ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    