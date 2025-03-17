
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANLTYP_GILTIG_FOM': 'varchar(max)', 'ANLTYP_GILTIG_TOM': 'varchar(max)', 'ANLTYP_ID': 'varchar(max)', 'ANLTYP_ID_TEXT': 'varchar(max)', 'ANLTYP_PASSIV': 'varchar(max)', 'ANLTYP_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), ANLTYP_GILTIG_FOM, 126) AS anltyp_giltig_fom,
		CONVERT(varchar(max), ANLTYP_GILTIG_TOM, 126) AS anltyp_giltig_tom,
		CAST(ANLTYP_ID AS VARCHAR(MAX)) AS anltyp_id,
		CAST(ANLTYP_ID_TEXT AS VARCHAR(MAX)) AS anltyp_id_text,
		CAST(ANLTYP_PASSIV AS VARCHAR(MAX)) AS anltyp_passiv,
		CAST(ANLTYP_TEXT AS VARCHAR(MAX)) AS anltyp_text 
	FROM stsudp.udp_858.EK_DIM_OBJ_ANLTYP ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    