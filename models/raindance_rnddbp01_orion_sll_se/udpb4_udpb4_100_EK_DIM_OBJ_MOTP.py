
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MOTP_GILTIG_FOM': 'varchar(max)', 'MOTP_GILTIG_TOM': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTP_ID_TEXT': 'varchar(max)', 'MOTP_PASSIV': 'varchar(max)', 'MOTP_TEXT': 'varchar(max)'},
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
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS motp_giltig_fom,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS motp_giltig_tom,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS motp_id_text,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS motp_passiv,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS motp_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    