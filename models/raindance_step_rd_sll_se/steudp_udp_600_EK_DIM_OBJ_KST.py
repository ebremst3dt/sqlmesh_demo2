
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DI_GILTIG_FOM': 'varchar(max)', 'DI_GILTIG_TOM': 'varchar(max)', 'DI_ID': 'varchar(max)', 'DI_ID_TEXT': 'varchar(max)', 'DI_PASSIV': 'varchar(max)', 'DI_TEXT': 'varchar(max)', 'KL_GILTIG_FOM': 'varchar(max)', 'KL_GILTIG_TOM': 'varchar(max)', 'KL_ID': 'varchar(max)', 'KL_ID_TEXT': 'varchar(max)', 'KL_PASSIV': 'varchar(max)', 'KL_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'SEK_GILTIG_FOM': 'varchar(max)', 'SEK_GILTIG_TOM': 'varchar(max)', 'SEK_ID': 'varchar(max)', 'SEK_ID_TEXT': 'varchar(max)', 'SEK_PASSIV': 'varchar(max)', 'SEK_TEXT': 'varchar(max)', 'VER_GILTIG_FOM': 'varchar(max)', 'VER_GILTIG_TOM': 'varchar(max)', 'VER_ID': 'varchar(max)', 'VER_ID_TEXT': 'varchar(max)', 'VER_PASSIV': 'varchar(max)', 'VER_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), DI_GILTIG_FOM, 126) AS DI_GILTIG_FOM,
		CONVERT(varchar(max), DI_GILTIG_TOM, 126) AS DI_GILTIG_TOM,
		CAST(DI_ID AS VARCHAR(MAX)) AS DI_ID,
		CAST(DI_ID_TEXT AS VARCHAR(MAX)) AS DI_ID_TEXT,
		CAST(DI_PASSIV AS VARCHAR(MAX)) AS DI_PASSIV,
		CAST(DI_TEXT AS VARCHAR(MAX)) AS DI_TEXT,
		CONVERT(varchar(max), KL_GILTIG_FOM, 126) AS KL_GILTIG_FOM,
		CONVERT(varchar(max), KL_GILTIG_TOM, 126) AS KL_GILTIG_TOM,
		CAST(KL_ID AS VARCHAR(MAX)) AS KL_ID,
		CAST(KL_ID_TEXT AS VARCHAR(MAX)) AS KL_ID_TEXT,
		CAST(KL_PASSIV AS VARCHAR(MAX)) AS KL_PASSIV,
		CAST(KL_TEXT AS VARCHAR(MAX)) AS KL_TEXT,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS KST_GILTIG_FOM,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS KST_GILTIG_TOM,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS KST_ID_TEXT,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS KST_PASSIV,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS KST_TEXT,
		CONVERT(varchar(max), SEK_GILTIG_FOM, 126) AS SEK_GILTIG_FOM,
		CONVERT(varchar(max), SEK_GILTIG_TOM, 126) AS SEK_GILTIG_TOM,
		CAST(SEK_ID AS VARCHAR(MAX)) AS SEK_ID,
		CAST(SEK_ID_TEXT AS VARCHAR(MAX)) AS SEK_ID_TEXT,
		CAST(SEK_PASSIV AS VARCHAR(MAX)) AS SEK_PASSIV,
		CAST(SEK_TEXT AS VARCHAR(MAX)) AS SEK_TEXT,
		CONVERT(varchar(max), VER_GILTIG_FOM, 126) AS VER_GILTIG_FOM,
		CONVERT(varchar(max), VER_GILTIG_TOM, 126) AS VER_GILTIG_TOM,
		CAST(VER_ID AS VARCHAR(MAX)) AS VER_ID,
		CAST(VER_ID_TEXT AS VARCHAR(MAX)) AS VER_ID_TEXT,
		CAST(VER_PASSIV AS VARCHAR(MAX)) AS VER_PASSIV,
		CAST(VER_TEXT AS VARCHAR(MAX)) AS VER_TEXT 
	FROM steudp.udp_600.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    