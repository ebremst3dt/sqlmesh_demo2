
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'INVTYP_GILTIG_FOM': 'varchar(max)', 'INVTYP_GILTIG_TOM': 'varchar(max)', 'INVTYP_ID': 'varchar(max)', 'INVTYP_ID_TEXT': 'varchar(max)', 'INVTYP_PASSIV': 'varchar(max)', 'INVTYP_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), INVTYP_GILTIG_FOM, 126) AS INVTYP_GILTIG_FOM,
		CONVERT(varchar(max), INVTYP_GILTIG_TOM, 126) AS INVTYP_GILTIG_TOM,
		CAST(INVTYP_ID AS VARCHAR(MAX)) AS INVTYP_ID,
		CAST(INVTYP_ID_TEXT AS VARCHAR(MAX)) AS INVTYP_ID_TEXT,
		CAST(INVTYP_PASSIV AS VARCHAR(MAX)) AS INVTYP_PASSIV,
		CAST(INVTYP_TEXT AS VARCHAR(MAX)) AS INVTYP_TEXT 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_INVTYP ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    