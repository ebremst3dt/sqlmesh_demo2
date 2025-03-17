
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FAKTCE_GILTIG_FOM': 'varchar(max)', 'FAKTCE_GILTIG_TOM': 'varchar(max)', 'FAKTCE_ID': 'varchar(max)', 'FAKTCE_ID_TEXT': 'varchar(max)', 'FAKTCE_PASSIV': 'varchar(max)', 'FAKTCE_TEXT': 'varchar(max)', 'VERK_GILTIG_FOM': 'varchar(max)', 'VERK_GILTIG_TOM': 'varchar(max)', 'VERK_ID': 'varchar(max)', 'VERK_ID_TEXT': 'varchar(max)', 'VERK_PASSIV': 'varchar(max)', 'VERK_TEXT': 'varchar(max)'},
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
		'step_rd_sll_se_steudp_udp_600' as _source,
		CONVERT(varchar(max), FAKTCE_GILTIG_FOM, 126) AS faktce_giltig_fom,
		CONVERT(varchar(max), FAKTCE_GILTIG_TOM, 126) AS faktce_giltig_tom,
		CAST(FAKTCE_ID AS VARCHAR(MAX)) AS faktce_id,
		CAST(FAKTCE_ID_TEXT AS VARCHAR(MAX)) AS faktce_id_text,
		CAST(FAKTCE_PASSIV AS VARCHAR(MAX)) AS faktce_passiv,
		CAST(FAKTCE_TEXT AS VARCHAR(MAX)) AS faktce_text,
		CONVERT(varchar(max), VERK_GILTIG_FOM, 126) AS verk_giltig_fom,
		CONVERT(varchar(max), VERK_GILTIG_TOM, 126) AS verk_giltig_tom,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id,
		CAST(VERK_ID_TEXT AS VARCHAR(MAX)) AS verk_id_text,
		CAST(VERK_PASSIV AS VARCHAR(MAX)) AS verk_passiv,
		CAST(VERK_TEXT AS VARCHAR(MAX)) AS verk_text 
	FROM steudp.udp_600.EK_DIM_OBJ_VERK ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    