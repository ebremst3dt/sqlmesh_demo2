
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ARBETE_GILTIG_FOM': 'varchar(max)', 'ARBETE_GILTIG_TOM': 'varchar(max)', 'ARBETE_ID': 'varchar(max)', 'ARBETE_ID_TEXT': 'varchar(max)', 'ARBETE_PASSIV': 'varchar(max)', 'ARBETE_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), ARBETE_GILTIG_FOM, 126) AS ARBETE_GILTIG_FOM,
		CONVERT(varchar(max), ARBETE_GILTIG_TOM, 126) AS ARBETE_GILTIG_TOM,
		CAST(ARBETE_ID AS VARCHAR(MAX)) AS ARBETE_ID,
		CAST(ARBETE_ID_TEXT AS VARCHAR(MAX)) AS ARBETE_ID_TEXT,
		CAST(ARBETE_PASSIV AS VARCHAR(MAX)) AS ARBETE_PASSIV,
		CAST(ARBETE_TEXT AS VARCHAR(MAX)) AS ARBETE_TEXT 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_ARBETE ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    