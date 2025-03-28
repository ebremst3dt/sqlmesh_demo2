
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PROJA_GILTIG_FOM': 'varchar(max)', 'PROJA_GILTIG_TOM': 'varchar(max)', 'PROJA_ID': 'varchar(max)', 'PROJA_ID_TEXT': 'varchar(max)', 'PROJA_PASSIV': 'varchar(max)', 'PROJA_TEXT': 'varchar(max)', 'PROJT_GILTIG_FOM': 'varchar(max)', 'PROJT_GILTIG_TOM': 'varchar(max)', 'PROJT_ID': 'varchar(max)', 'PROJT_ID_TEXT': 'varchar(max)', 'PROJT_PASSIV': 'varchar(max)', 'PROJT_TEXT': 'varchar(max)', 'PROJ_GILTIG_FOM': 'varchar(max)', 'PROJ_GILTIG_TOM': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'PROJ_ID_TEXT': 'varchar(max)', 'PROJ_PASSIV': 'varchar(max)', 'PROJ_TEXT': 'varchar(max)'},
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
		'dsp_rd_sll_se_raindance_udp_udp_150' as _source,
		CONVERT(varchar(max), PROJA_GILTIG_FOM, 126) AS PROJA_GILTIG_FOM,
		CONVERT(varchar(max), PROJA_GILTIG_TOM, 126) AS PROJA_GILTIG_TOM,
		CAST(PROJA_ID AS VARCHAR(MAX)) AS PROJA_ID,
		CAST(PROJA_ID_TEXT AS VARCHAR(MAX)) AS PROJA_ID_TEXT,
		CAST(PROJA_PASSIV AS VARCHAR(MAX)) AS PROJA_PASSIV,
		CAST(PROJA_TEXT AS VARCHAR(MAX)) AS PROJA_TEXT,
		CONVERT(varchar(max), PROJT_GILTIG_FOM, 126) AS PROJT_GILTIG_FOM,
		CONVERT(varchar(max), PROJT_GILTIG_TOM, 126) AS PROJT_GILTIG_TOM,
		CAST(PROJT_ID AS VARCHAR(MAX)) AS PROJT_ID,
		CAST(PROJT_ID_TEXT AS VARCHAR(MAX)) AS PROJT_ID_TEXT,
		CAST(PROJT_PASSIV AS VARCHAR(MAX)) AS PROJT_PASSIV,
		CAST(PROJT_TEXT AS VARCHAR(MAX)) AS PROJT_TEXT,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS PROJ_GILTIG_FOM,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS PROJ_GILTIG_TOM,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS PROJ_ID_TEXT,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS PROJ_PASSIV,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS PROJ_TEXT 
	FROM raindance_udp.udp_150.EK_DIM_OBJ_PROJ ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    