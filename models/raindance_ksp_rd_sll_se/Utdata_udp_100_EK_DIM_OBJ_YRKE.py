
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
		CONVERT(varchar(max), SJKYRK_GILTIG_FOM, 126) AS SJKYRK_GILTIG_FOM,
		CONVERT(varchar(max), SJKYRK_GILTIG_TOM, 126) AS SJKYRK_GILTIG_TOM,
		CAST(SJKYRK_ID AS VARCHAR(MAX)) AS SJKYRK_ID,
		CAST(SJKYRK_ID_TEXT AS VARCHAR(MAX)) AS SJKYRK_ID_TEXT,
		CAST(SJKYRK_PASSIV AS VARCHAR(MAX)) AS SJKYRK_PASSIV,
		CAST(SJKYRK_TEXT AS VARCHAR(MAX)) AS SJKYRK_TEXT,
		CONVERT(varchar(max), YRKE_GILTIG_FOM, 126) AS YRKE_GILTIG_FOM,
		CONVERT(varchar(max), YRKE_GILTIG_TOM, 126) AS YRKE_GILTIG_TOM,
		CAST(YRKE_ID AS VARCHAR(MAX)) AS YRKE_ID,
		CAST(YRKE_ID_TEXT AS VARCHAR(MAX)) AS YRKE_ID_TEXT,
		CAST(YRKE_PASSIV AS VARCHAR(MAX)) AS YRKE_PASSIV,
		CAST(YRKE_TEXT AS VARCHAR(MAX)) AS YRKE_TEXT 
	FROM Utdata.udp_100.EK_DIM_OBJ_YRKE ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    