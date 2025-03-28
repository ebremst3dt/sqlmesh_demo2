
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AR': 'varchar(max)', 'AR_TEXT': 'varchar(max)', 'BOKFORINGSAR': 'varchar(max)', 'BOKFORINGSARSLUT': 'varchar(max)', 'BOKFORINGSAR_TEXT': 'varchar(max)', 'KVARTAL': 'varchar(max)', 'KVARTALNR': 'varchar(max)', 'KVARTALNR_TEXT': 'varchar(max)', 'KVARTAL_TEXT': 'varchar(max)', 'MANAD': 'varchar(max)', 'MANADNR': 'varchar(max)', 'MANADNR_TEXT': 'varchar(max)', 'MANADSNAMN': 'varchar(max)', 'MANAD_TEXT': 'varchar(max)', 'PERIOD': 'varchar(max)', 'PERIOD10_TEXT': 'varchar(max)', 'PERIOD4_TEXT': 'varchar(max)', 'PERIOD6B_TEXT': 'varchar(max)', 'PERIOD6_TEXT': 'varchar(max)', 'PERIOD7_TEXT': 'varchar(max)', 'PERIOD8_TEXT': 'varchar(max)', 'PERIODSLUT': 'varchar(max)', 'PERIODSTATUS': 'varchar(max)', 'PERIODSTATUS_TEXT': 'varchar(max)', 'PERIOD_TEXT': 'varchar(max)', 'TERTIAL': 'varchar(max)', 'TERTIALNR': 'varchar(max)', 'TERTIALNR_TEXT': 'varchar(max)', 'TERTIAL_TEXT': 'varchar(max)'},
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
		'stsp_rd_sll_se_stsudp_udp_858' as _source,
		CAST(AR AS VARCHAR(MAX)) AS AR,
		CAST(AR_TEXT AS VARCHAR(MAX)) AS AR_TEXT,
		CONVERT(varchar(max), BOKFORINGSAR, 126) AS BOKFORINGSAR,
		CONVERT(varchar(max), BOKFORINGSARSLUT, 126) AS BOKFORINGSARSLUT,
		CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS BOKFORINGSAR_TEXT,
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
		CAST(PERIOD10_TEXT AS VARCHAR(MAX)) AS PERIOD10_TEXT,
		CAST(PERIOD4_TEXT AS VARCHAR(MAX)) AS PERIOD4_TEXT,
		CAST(PERIOD6B_TEXT AS VARCHAR(MAX)) AS PERIOD6B_TEXT,
		CAST(PERIOD6_TEXT AS VARCHAR(MAX)) AS PERIOD6_TEXT,
		CAST(PERIOD7_TEXT AS VARCHAR(MAX)) AS PERIOD7_TEXT,
		CAST(PERIOD8_TEXT AS VARCHAR(MAX)) AS PERIOD8_TEXT,
		CONVERT(varchar(max), PERIODSLUT, 126) AS PERIODSLUT,
		CAST(PERIODSTATUS AS VARCHAR(MAX)) AS PERIODSTATUS,
		CAST(PERIODSTATUS_TEXT AS VARCHAR(MAX)) AS PERIODSTATUS_TEXT,
		CAST(PERIOD_TEXT AS VARCHAR(MAX)) AS PERIOD_TEXT,
		CAST(TERTIAL AS VARCHAR(MAX)) AS TERTIAL,
		CAST(TERTIALNR AS VARCHAR(MAX)) AS TERTIALNR,
		CAST(TERTIALNR_TEXT AS VARCHAR(MAX)) AS TERTIALNR_TEXT,
		CAST(TERTIAL_TEXT AS VARCHAR(MAX)) AS TERTIAL_TEXT 
	FROM stsudp.udp_858.EK_DIM_PERIOD ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    