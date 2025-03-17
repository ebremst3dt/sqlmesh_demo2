
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PORGNR_GILTIG_FOM': 'varchar(max)', 'PORGNR_GILTIG_TOM': 'varchar(max)', 'PORGNR_ID': 'varchar(max)', 'PORGNR_ID_TEXT': 'varchar(max)', 'PORGNR_PASSIV': 'varchar(max)', 'PORGNR_TEXT': 'varchar(max)'},
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
		'ksp_rd_sll_se_Utdata_udp_100' as _source,
		CONVERT(varchar(max), PORGNR_GILTIG_FOM, 126) AS porgnr_giltig_fom,
		CONVERT(varchar(max), PORGNR_GILTIG_TOM, 126) AS porgnr_giltig_tom,
		CAST(PORGNR_ID AS VARCHAR(MAX)) AS porgnr_id,
		CAST(PORGNR_ID_TEXT AS VARCHAR(MAX)) AS porgnr_id_text,
		CAST(PORGNR_PASSIV AS VARCHAR(MAX)) AS porgnr_passiv,
		CAST(PORGNR_TEXT AS VARCHAR(MAX)) AS porgnr_text 
	FROM Utdata.udp_100.EK_DIM_OBJ_PORGNR ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    