
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANST_GILTIG_FOM': 'varchar(max)', 'ANST_GILTIG_TOM': 'varchar(max)', 'ANST_ID': 'varchar(max)', 'ANST_ID_TEXT': 'varchar(max)', 'ANST_PASSIV': 'varchar(max)', 'ANST_TEXT': 'varchar(max)'},
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
		'mcp1_rd_sll_se_MediCarrierUDP_utdata100' as _source,
		CONVERT(varchar(max), ANST_GILTIG_FOM, 126) AS anst_giltig_fom,
		CONVERT(varchar(max), ANST_GILTIG_TOM, 126) AS anst_giltig_tom,
		CAST(ANST_ID AS VARCHAR(MAX)) AS anst_id,
		CAST(ANST_ID_TEXT AS VARCHAR(MAX)) AS anst_id_text,
		CAST(ANST_PASSIV AS VARCHAR(MAX)) AS anst_passiv,
		CAST(ANST_TEXT AS VARCHAR(MAX)) AS anst_text 
	FROM MediCarrierUDP.utdata100.EK_DIM_OBJ_ANST ) y

	"""
    return read(query=query, server_url="mcp1.rd.sll.se")
    