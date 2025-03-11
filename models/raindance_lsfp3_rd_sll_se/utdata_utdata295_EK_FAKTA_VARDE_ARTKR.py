
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ARTKR_ID': 'varchar(max)', 'AVTBES_ID': 'varchar(max)', 'DATUM_FOM': 'varchar(max)', 'DATUM_TOM': 'varchar(max)', 'FRI1_ID': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KST_ID': 'varchar(max)', 'MOMS_ID': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'TEXT2_TEXT': 'varchar(max)', 'TEXT_TEXT': 'varchar(max)', 'UTFALL_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VERDATUM': 'varchar(max)'},
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
		CAST(ARTKR_ID AS VARCHAR(MAX)) AS artkr_id,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MOMS_ID AS VARCHAR(MAX)) AS moms_id,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(TEXT2_TEXT AS VARCHAR(MAX)) AS text2_text,
		CAST(TEXT_TEXT AS VARCHAR(MAX)) AS text_text,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_ARTKR) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    