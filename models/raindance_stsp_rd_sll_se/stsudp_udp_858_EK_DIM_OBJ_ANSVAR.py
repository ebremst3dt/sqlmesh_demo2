
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANSVAR_GILTIG_FOM': 'varchar(max)', 'ANSVAR_GILTIG_TOM': 'varchar(max)', 'ANSVAR_ID': 'varchar(max)', 'ANSVAR_ID_TEXT': 'varchar(max)', 'ANSVAR_PASSIV': 'varchar(max)', 'ANSVAR_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), ANSVAR_GILTIG_FOM, 126) AS ansvar_giltig_fom,
		CONVERT(varchar(max), ANSVAR_GILTIG_TOM, 126) AS ansvar_giltig_tom,
		CAST(ANSVAR_ID AS VARCHAR(MAX)) AS ansvar_id,
		CAST(ANSVAR_ID_TEXT AS VARCHAR(MAX)) AS ansvar_id_text,
		CAST(ANSVAR_PASSIV AS VARCHAR(MAX)) AS ansvar_passiv,
		CAST(ANSVAR_TEXT AS VARCHAR(MAX)) AS ansvar_text 
	FROM stsudp.udp_858.EK_DIM_OBJ_ANSVAR ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    