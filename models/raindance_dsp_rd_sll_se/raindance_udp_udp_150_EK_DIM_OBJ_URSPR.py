
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'URSPR_GILTIG_FOM': 'varchar(max)', 'URSPR_GILTIG_TOM': 'varchar(max)', 'URSPR_ID': 'varchar(max)', 'URSPR_ID_TEXT': 'varchar(max)', 'URSPR_PASSIV': 'varchar(max)', 'URSPR_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), URSPR_GILTIG_FOM, 126) AS URSPR_GILTIG_FOM,
		CONVERT(varchar(max), URSPR_GILTIG_TOM, 126) AS URSPR_GILTIG_TOM,
		CAST(URSPR_ID AS VARCHAR(MAX)) AS URSPR_ID,
		CAST(URSPR_ID_TEXT AS VARCHAR(MAX)) AS URSPR_ID_TEXT,
		CAST(URSPR_PASSIV AS VARCHAR(MAX)) AS URSPR_PASSIV,
		CAST(URSPR_TEXT AS VARCHAR(MAX)) AS URSPR_TEXT 
	FROM raindance_udp.udp_150.EK_DIM_OBJ_URSPR ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    