
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PROC_GILTIG_FOM': 'varchar(max)', 'PROC_GILTIG_TOM': 'varchar(max)', 'PROC_ID': 'varchar(max)', 'PROC_ID_TEXT': 'varchar(max)', 'PROC_PASSIV': 'varchar(max)', 'PROC_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata288' as _source,
		CONVERT(varchar(max), PROC_GILTIG_FOM, 126) AS proc_giltig_fom,
		CONVERT(varchar(max), PROC_GILTIG_TOM, 126) AS proc_giltig_tom,
		CAST(PROC_ID AS VARCHAR(MAX)) AS proc_id,
		CAST(PROC_ID_TEXT AS VARCHAR(MAX)) AS proc_id_text,
		CAST(PROC_PASSIV AS VARCHAR(MAX)) AS proc_passiv,
		CAST(PROC_TEXT AS VARCHAR(MAX)) AS proc_text 
	FROM utdata.utdata288.EK_DIM_OBJ_PROC ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    