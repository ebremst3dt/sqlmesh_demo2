
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'VGREN_GILTIG_FOM': 'varchar(max)', 'VGREN_GILTIG_TOM': 'varchar(max)', 'VGREN_ID': 'varchar(max)', 'VGREN_ID_TEXT': 'varchar(max)', 'VGREN_PASSIV': 'varchar(max)', 'VGREN_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata290' as _source,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS kst_giltig_fom,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS vgren_giltig_fom,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS vgren_giltig_tom,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS vgren_id,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS vgren_id_text,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS vgren_passiv,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS vgren_text 
	FROM utdata.utdata290.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    