
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'ANVID_TEXT': 'varchar(max)', 'BILDNR_TEXT': 'varchar(max)', 'BILDN_TEXT': 'varchar(max)', 'DATUM_FOM': 'varchar(max)', 'DATUM_TOM': 'varchar(max)', 'DELSYS_TEXT': 'varchar(max)', 'HHMMSS_TEXT': 'varchar(max)', 'LOPNUMMER': 'varchar(max)', 'TIDSQL_V': 'varchar(max)', 'TID_V': 'varchar(max)', 'URVAL_TEXT': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VMNR_TEXT': 'varchar(max)', 'VMN_TEXT': 'varchar(max)'},
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
    query = """
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(ANVID_TEXT AS VARCHAR(MAX)) AS anvid_text,
		CAST(BILDNR_TEXT AS VARCHAR(MAX)) AS bildnr_text,
		CAST(BILDN_TEXT AS VARCHAR(MAX)) AS bildn_text,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(DELSYS_TEXT AS VARCHAR(MAX)) AS delsys_text,
		CAST(HHMMSS_TEXT AS VARCHAR(MAX)) AS hhmmss_text,
		CAST(LOPNUMMER AS VARCHAR(MAX)) AS lopnummer,
		CAST(TIDSQL_V AS VARCHAR(MAX)) AS tidsql_v,
		CAST(TID_V AS VARCHAR(MAX)) AS tid_v,
		CAST(URVAL_TEXT AS VARCHAR(MAX)) AS urval_text,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VMNR_TEXT AS VARCHAR(MAX)) AS vmnr_text,
		CAST(VMN_TEXT AS VARCHAR(MAX)) AS vmn_text 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    