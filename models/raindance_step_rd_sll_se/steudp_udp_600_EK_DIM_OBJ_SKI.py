
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'SKI_GILTIG_FOM': 'varchar(max)', 'SKI_GILTIG_TOM': 'varchar(max)', 'SKI_ID': 'varchar(max)', 'SKI_ID_TEXT': 'varchar(max)', 'SKI_PASSIV': 'varchar(max)', 'SKI_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), SKI_GILTIG_FOM, 126) AS SKI_GILTIG_FOM,
		CONVERT(varchar(max), SKI_GILTIG_TOM, 126) AS SKI_GILTIG_TOM,
		CAST(SKI_ID AS VARCHAR(MAX)) AS SKI_ID,
		CAST(SKI_ID_TEXT AS VARCHAR(MAX)) AS SKI_ID_TEXT,
		CAST(SKI_PASSIV AS VARCHAR(MAX)) AS SKI_PASSIV,
		CAST(SKI_TEXT AS VARCHAR(MAX)) AS SKI_TEXT 
	FROM steudp.udp_600.EK_DIM_OBJ_SKI ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    