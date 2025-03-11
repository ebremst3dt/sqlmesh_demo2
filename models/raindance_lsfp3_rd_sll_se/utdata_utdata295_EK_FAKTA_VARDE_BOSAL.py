
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'ANDRAD_AV': 'varchar(max)', 'ANDRAD_DATUM': 'varchar(max)', 'ANDRAD_TID': 'varchar(max)', 'DATUM_FOM': 'varchar(max)', 'DATUM_TOM': 'varchar(max)', 'IB1_V': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'UB1_V': 'varchar(max)', 'UB2_V': 'varchar(max)', 'UB3_V': 'varchar(max)', 'UB4_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VERDATUM': 'varchar(max)'},
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
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(ANDRAD_AV AS VARCHAR(MAX)) AS andrad_av,
		CONVERT(varchar(max), ANDRAD_DATUM, 126) AS andrad_datum,
		CAST(ANDRAD_TID AS VARCHAR(MAX)) AS andrad_tid,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(IB1_V AS VARCHAR(MAX)) AS ib1_v,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(UB1_V AS VARCHAR(MAX)) AS ub1_v,
		CAST(UB2_V AS VARCHAR(MAX)) AS ub2_v,
		CAST(UB3_V AS VARCHAR(MAX)) AS ub3_v,
		CAST(UB4_V AS VARCHAR(MAX)) AS ub4_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BOSAL) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    