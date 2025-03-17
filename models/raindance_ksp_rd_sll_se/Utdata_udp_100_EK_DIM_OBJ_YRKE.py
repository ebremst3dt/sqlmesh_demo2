
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'SJKYRK_GILTIG_FOM': 'varchar(max)', 'SJKYRK_GILTIG_TOM': 'varchar(max)', 'SJKYRK_ID': 'varchar(max)', 'SJKYRK_ID_TEXT': 'varchar(max)', 'SJKYRK_PASSIV': 'varchar(max)', 'SJKYRK_TEXT': 'varchar(max)', 'YRKE_GILTIG_FOM': 'varchar(max)', 'YRKE_GILTIG_TOM': 'varchar(max)', 'YRKE_ID': 'varchar(max)', 'YRKE_ID_TEXT': 'varchar(max)', 'YRKE_PASSIV': 'varchar(max)', 'YRKE_TEXT': 'varchar(max)'},
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
		'ksp_rd_sll_se_Utdata_udp_100' as _source,
		CONVERT(varchar(max), SJKYRK_GILTIG_FOM, 126) AS sjkyrk_giltig_fom,
		CONVERT(varchar(max), SJKYRK_GILTIG_TOM, 126) AS sjkyrk_giltig_tom,
		CAST(SJKYRK_ID AS VARCHAR(MAX)) AS sjkyrk_id,
		CAST(SJKYRK_ID_TEXT AS VARCHAR(MAX)) AS sjkyrk_id_text,
		CAST(SJKYRK_PASSIV AS VARCHAR(MAX)) AS sjkyrk_passiv,
		CAST(SJKYRK_TEXT AS VARCHAR(MAX)) AS sjkyrk_text,
		CONVERT(varchar(max), YRKE_GILTIG_FOM, 126) AS yrke_giltig_fom,
		CONVERT(varchar(max), YRKE_GILTIG_TOM, 126) AS yrke_giltig_tom,
		CAST(YRKE_ID AS VARCHAR(MAX)) AS yrke_id,
		CAST(YRKE_ID_TEXT AS VARCHAR(MAX)) AS yrke_id_text,
		CAST(YRKE_PASSIV AS VARCHAR(MAX)) AS yrke_passiv,
		CAST(YRKE_TEXT AS VARCHAR(MAX)) AS yrke_text 
	FROM Utdata.udp_100.EK_DIM_OBJ_YRKE ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    