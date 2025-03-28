
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'EXTERN': 'varchar(max)', 'EXTERN_TEXT': 'varchar(max)', 'RESKONTRA': 'varchar(max)', 'RESKONTRA_TEXT': 'varchar(max)', 'RESKTYP': 'varchar(max)', 'RESKTYP_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata805' as _source,
		CAST(EXTERN AS VARCHAR(MAX)) AS EXTERN,
		CAST(EXTERN_TEXT AS VARCHAR(MAX)) AS EXTERN_TEXT,
		CAST(RESKONTRA AS VARCHAR(MAX)) AS RESKONTRA,
		CAST(RESKONTRA_TEXT AS VARCHAR(MAX)) AS RESKONTRA_TEXT,
		CAST(RESKTYP AS VARCHAR(MAX)) AS RESKTYP,
		CAST(RESKTYP_TEXT AS VARCHAR(MAX)) AS RESKTYP_TEXT 
	FROM utdata.utdata805.RK_DIM_RESKONTRA ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    