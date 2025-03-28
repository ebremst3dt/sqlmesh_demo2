
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DELPRO_GILTIG_FOM': 'varchar(max)', 'DELPRO_GILTIG_TOM': 'varchar(max)', 'DELPRO_ID': 'varchar(max)', 'DELPRO_ID_TEXT': 'varchar(max)', 'DELPRO_PASSIV': 'varchar(max)', 'DELPRO_TEXT': 'varchar(max)', 'TPROJ_GILTIG_FOM': 'varchar(max)', 'TPROJ_GILTIG_TOM': 'varchar(max)', 'TPROJ_ID': 'varchar(max)', 'TPROJ_ID_TEXT': 'varchar(max)', 'TPROJ_PASSIV': 'varchar(max)', 'TPROJ_TEXT': 'varchar(max)'},
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
		'nksp_rd_sll_se_raindance_udp_udp_100' as _source,
		CONVERT(varchar(max), DELPRO_GILTIG_FOM, 126) AS DELPRO_GILTIG_FOM,
		CONVERT(varchar(max), DELPRO_GILTIG_TOM, 126) AS DELPRO_GILTIG_TOM,
		CAST(DELPRO_ID AS VARCHAR(MAX)) AS DELPRO_ID,
		CAST(DELPRO_ID_TEXT AS VARCHAR(MAX)) AS DELPRO_ID_TEXT,
		CAST(DELPRO_PASSIV AS VARCHAR(MAX)) AS DELPRO_PASSIV,
		CAST(DELPRO_TEXT AS VARCHAR(MAX)) AS DELPRO_TEXT,
		CONVERT(varchar(max), TPROJ_GILTIG_FOM, 126) AS TPROJ_GILTIG_FOM,
		CONVERT(varchar(max), TPROJ_GILTIG_TOM, 126) AS TPROJ_GILTIG_TOM,
		CAST(TPROJ_ID AS VARCHAR(MAX)) AS TPROJ_ID,
		CAST(TPROJ_ID_TEXT AS VARCHAR(MAX)) AS TPROJ_ID_TEXT,
		CAST(TPROJ_PASSIV AS VARCHAR(MAX)) AS TPROJ_PASSIV,
		CAST(TPROJ_TEXT AS VARCHAR(MAX)) AS TPROJ_TEXT 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_DELPRO ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    