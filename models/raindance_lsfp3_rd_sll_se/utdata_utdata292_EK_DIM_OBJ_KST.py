
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANS1_GILTIG_FOM': 'varchar(max)', 'ANS1_GILTIG_TOM': 'varchar(max)', 'ANS1_ID': 'varchar(max)', 'ANS1_ID_TEXT': 'varchar(max)', 'ANS1_PASSIV': 'varchar(max)', 'ANS1_TEXT': 'varchar(max)', 'ANS2_GILTIG_FOM': 'varchar(max)', 'ANS2_GILTIG_TOM': 'varchar(max)', 'ANS2_ID': 'varchar(max)', 'ANS2_ID_TEXT': 'varchar(max)', 'ANS2_PASSIV': 'varchar(max)', 'ANS2_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'VO_GILTIG_FOM': 'varchar(max)', 'VO_GILTIG_TOM': 'varchar(max)', 'VO_ID': 'varchar(max)', 'VO_ID_TEXT': 'varchar(max)', 'VO_PASSIV': 'varchar(max)', 'VO_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata292' as _source,
		CONVERT(varchar(max), ANS1_GILTIG_FOM, 126) AS ANS1_GILTIG_FOM,
		CONVERT(varchar(max), ANS1_GILTIG_TOM, 126) AS ANS1_GILTIG_TOM,
		CAST(ANS1_ID AS VARCHAR(MAX)) AS ANS1_ID,
		CAST(ANS1_ID_TEXT AS VARCHAR(MAX)) AS ANS1_ID_TEXT,
		CAST(ANS1_PASSIV AS VARCHAR(MAX)) AS ANS1_PASSIV,
		CAST(ANS1_TEXT AS VARCHAR(MAX)) AS ANS1_TEXT,
		CONVERT(varchar(max), ANS2_GILTIG_FOM, 126) AS ANS2_GILTIG_FOM,
		CONVERT(varchar(max), ANS2_GILTIG_TOM, 126) AS ANS2_GILTIG_TOM,
		CAST(ANS2_ID AS VARCHAR(MAX)) AS ANS2_ID,
		CAST(ANS2_ID_TEXT AS VARCHAR(MAX)) AS ANS2_ID_TEXT,
		CAST(ANS2_PASSIV AS VARCHAR(MAX)) AS ANS2_PASSIV,
		CAST(ANS2_TEXT AS VARCHAR(MAX)) AS ANS2_TEXT,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS KST_GILTIG_FOM,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS KST_GILTIG_TOM,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS KST_ID_TEXT,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS KST_PASSIV,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS KST_TEXT,
		CONVERT(varchar(max), VO_GILTIG_FOM, 126) AS VO_GILTIG_FOM,
		CONVERT(varchar(max), VO_GILTIG_TOM, 126) AS VO_GILTIG_TOM,
		CAST(VO_ID AS VARCHAR(MAX)) AS VO_ID,
		CAST(VO_ID_TEXT AS VARCHAR(MAX)) AS VO_ID_TEXT,
		CAST(VO_PASSIV AS VARCHAR(MAX)) AS VO_PASSIV,
		CAST(VO_TEXT AS VARCHAR(MAX)) AS VO_TEXT 
	FROM utdata.utdata292.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    