
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BODTYP_GILTIG_FOM': 'varchar(max)', 'BODTYP_GILTIG_TOM': 'varchar(max)', 'BODTYP_ID': 'varchar(max)', 'BODTYP_ID_TEXT': 'varchar(max)', 'BODTYP_PASSIV': 'varchar(max)', 'BODTYP_TEXT': 'varchar(max)'},
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
		'sosp_rd_sll_se_raindance_udp_udp_220' as _source,
		CONVERT(varchar(max), BODTYP_GILTIG_FOM, 126) AS bodtyp_giltig_fom,
		CONVERT(varchar(max), BODTYP_GILTIG_TOM, 126) AS bodtyp_giltig_tom,
		CAST(BODTYP_ID AS VARCHAR(MAX)) AS bodtyp_id,
		CAST(BODTYP_ID_TEXT AS VARCHAR(MAX)) AS bodtyp_id_text,
		CAST(BODTYP_PASSIV AS VARCHAR(MAX)) AS bodtyp_passiv,
		CAST(BODTYP_TEXT AS VARCHAR(MAX)) AS bodtyp_text 
	FROM raindance_udp.udp_220.EK_DIM_OBJ_BODTYP ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    