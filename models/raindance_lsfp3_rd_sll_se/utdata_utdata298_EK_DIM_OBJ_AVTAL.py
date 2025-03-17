
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVTAL_GILTIG_FOM': 'varchar(max)', 'AVTAL_GILTIG_TOM': 'varchar(max)', 'AVTAL_ID': 'varchar(max)', 'AVTAL_ID_TEXT': 'varchar(max)', 'AVTAL_PASSIV': 'varchar(max)', 'AVTAL_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CONVERT(varchar(max), AVTAL_GILTIG_FOM, 126) AS avtal_giltig_fom,
		CONVERT(varchar(max), AVTAL_GILTIG_TOM, 126) AS avtal_giltig_tom,
		CAST(AVTAL_ID AS VARCHAR(MAX)) AS avtal_id,
		CAST(AVTAL_ID_TEXT AS VARCHAR(MAX)) AS avtal_id_text,
		CAST(AVTAL_PASSIV AS VARCHAR(MAX)) AS avtal_passiv,
		CAST(AVTAL_TEXT AS VARCHAR(MAX)) AS avtal_text 
	FROM utdata.utdata298.EK_DIM_OBJ_AVTAL ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    