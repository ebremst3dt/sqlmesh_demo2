
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVSLÅR_GILTIG_FOM': 'varchar(max)', 'AVSLÅR_GILTIG_TOM': 'varchar(max)', 'AVSLÅR_ID': 'varchar(max)', 'AVSLÅR_ID_TEXT': 'varchar(max)', 'AVSLÅR_PASSIV': 'varchar(max)', 'AVSLÅR_TEXT': 'varchar(max)', 'FIFORM_GILTIG_FOM': 'varchar(max)', 'FIFORM_GILTIG_TOM': 'varchar(max)', 'FIFORM_ID': 'varchar(max)', 'FIFORM_ID_TEXT': 'varchar(max)', 'FIFORM_PASSIV': 'varchar(max)', 'FIFORM_TEXT': 'varchar(max)', 'PROENH_GILTIG_FOM': 'varchar(max)', 'PROENH_GILTIG_TOM': 'varchar(max)', 'PROENH_ID': 'varchar(max)', 'PROENH_ID_TEXT': 'varchar(max)', 'PROENH_PASSIV': 'varchar(max)', 'PROENH_TEXT': 'varchar(max)', 'PROJL_GILTIG_FOM': 'varchar(max)', 'PROJL_GILTIG_TOM': 'varchar(max)', 'PROJL_ID': 'varchar(max)', 'PROJL_ID_TEXT': 'varchar(max)', 'PROJL_PASSIV': 'varchar(max)', 'PROJL_TEXT': 'varchar(max)', 'PROJ_GILTIG_FOM': 'varchar(max)', 'PROJ_GILTIG_TOM': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'PROJ_ID_TEXT': 'varchar(max)', 'PROJ_PASSIV': 'varchar(max)', 'PROJ_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), AVSLÅR_GILTIG_FOM, 126) AS AVSLÅR_GILTIG_FOM,
		CONVERT(varchar(max), AVSLÅR_GILTIG_TOM, 126) AS AVSLÅR_GILTIG_TOM,
		CAST(AVSLÅR_ID AS VARCHAR(MAX)) AS AVSLÅR_ID,
		CAST(AVSLÅR_ID_TEXT AS VARCHAR(MAX)) AS AVSLÅR_ID_TEXT,
		CAST(AVSLÅR_PASSIV AS VARCHAR(MAX)) AS AVSLÅR_PASSIV,
		CAST(AVSLÅR_TEXT AS VARCHAR(MAX)) AS AVSLÅR_TEXT,
		CONVERT(varchar(max), FIFORM_GILTIG_FOM, 126) AS FIFORM_GILTIG_FOM,
		CONVERT(varchar(max), FIFORM_GILTIG_TOM, 126) AS FIFORM_GILTIG_TOM,
		CAST(FIFORM_ID AS VARCHAR(MAX)) AS FIFORM_ID,
		CAST(FIFORM_ID_TEXT AS VARCHAR(MAX)) AS FIFORM_ID_TEXT,
		CAST(FIFORM_PASSIV AS VARCHAR(MAX)) AS FIFORM_PASSIV,
		CAST(FIFORM_TEXT AS VARCHAR(MAX)) AS FIFORM_TEXT,
		CONVERT(varchar(max), PROENH_GILTIG_FOM, 126) AS PROENH_GILTIG_FOM,
		CONVERT(varchar(max), PROENH_GILTIG_TOM, 126) AS PROENH_GILTIG_TOM,
		CAST(PROENH_ID AS VARCHAR(MAX)) AS PROENH_ID,
		CAST(PROENH_ID_TEXT AS VARCHAR(MAX)) AS PROENH_ID_TEXT,
		CAST(PROENH_PASSIV AS VARCHAR(MAX)) AS PROENH_PASSIV,
		CAST(PROENH_TEXT AS VARCHAR(MAX)) AS PROENH_TEXT,
		CONVERT(varchar(max), PROJL_GILTIG_FOM, 126) AS PROJL_GILTIG_FOM,
		CONVERT(varchar(max), PROJL_GILTIG_TOM, 126) AS PROJL_GILTIG_TOM,
		CAST(PROJL_ID AS VARCHAR(MAX)) AS PROJL_ID,
		CAST(PROJL_ID_TEXT AS VARCHAR(MAX)) AS PROJL_ID_TEXT,
		CAST(PROJL_PASSIV AS VARCHAR(MAX)) AS PROJL_PASSIV,
		CAST(PROJL_TEXT AS VARCHAR(MAX)) AS PROJL_TEXT,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS PROJ_GILTIG_FOM,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS PROJ_GILTIG_TOM,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS PROJ_ID_TEXT,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS PROJ_PASSIV,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS PROJ_TEXT 
	FROM utdata.utdata295.EK_DIM_OBJ_PROJ_DIR_28 ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    