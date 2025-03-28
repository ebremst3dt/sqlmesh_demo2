
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PPYRKE_GILTIG_FOM': 'varchar(max)', 'PPYRKE_GILTIG_TOM': 'varchar(max)', 'PPYRKE_ID': 'varchar(max)', 'PPYRKE_ID_TEXT': 'varchar(max)', 'PPYRKE_PASSIV': 'varchar(max)', 'PPYRKE_TEXT': 'varchar(max)'},
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
		'ftvddbs08_ftv_sll_se_ftvudp_ftv_400' as _source,
		CONVERT(varchar(max), PPYRKE_GILTIG_FOM, 126) AS PPYRKE_GILTIG_FOM,
		CONVERT(varchar(max), PPYRKE_GILTIG_TOM, 126) AS PPYRKE_GILTIG_TOM,
		CAST(PPYRKE_ID AS VARCHAR(MAX)) AS PPYRKE_ID,
		CAST(PPYRKE_ID_TEXT AS VARCHAR(MAX)) AS PPYRKE_ID_TEXT,
		CAST(PPYRKE_PASSIV AS VARCHAR(MAX)) AS PPYRKE_PASSIV,
		CAST(PPYRKE_TEXT AS VARCHAR(MAX)) AS PPYRKE_TEXT 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_PPYRKE ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    