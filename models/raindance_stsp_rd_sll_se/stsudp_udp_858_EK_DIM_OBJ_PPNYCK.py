
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PPNYCK_GILTIG_FOM': 'varchar(max)', 'PPNYCK_GILTIG_TOM': 'varchar(max)', 'PPNYCK_ID': 'varchar(max)', 'PPNYCK_ID_TEXT': 'varchar(max)', 'PPNYCK_PASSIV': 'varchar(max)', 'PPNYCK_TEXT': 'varchar(max)'},
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
		'stsp_rd_sll_se_stsudp_udp_858' as _source,
		CONVERT(varchar(max), PPNYCK_GILTIG_FOM, 126) AS ppnyck_giltig_fom,
		CONVERT(varchar(max), PPNYCK_GILTIG_TOM, 126) AS ppnyck_giltig_tom,
		CAST(PPNYCK_ID AS VARCHAR(MAX)) AS ppnyck_id,
		CAST(PPNYCK_ID_TEXT AS VARCHAR(MAX)) AS ppnyck_id_text,
		CAST(PPNYCK_PASSIV AS VARCHAR(MAX)) AS ppnyck_passiv,
		CAST(PPNYCK_TEXT AS VARCHAR(MAX)) AS ppnyck_text 
	FROM stsudp.udp_858.EK_DIM_OBJ_PPNYCK ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    