
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'YKAT_GILTIG_FOM': 'varchar(max)', 'YKAT_GILTIG_TOM': 'varchar(max)', 'YKAT_ID': 'varchar(max)', 'YKAT_ID_TEXT': 'varchar(max)', 'YKAT_PASSIV': 'varchar(max)', 'YKAT_TEXT': 'varchar(max)'},
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
    query = """
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), YKAT_GILTIG_FOM, 126) AS ykat_giltig_fom,
		CONVERT(varchar(max), YKAT_GILTIG_TOM, 126) AS ykat_giltig_tom,
		CAST(YKAT_ID AS VARCHAR(MAX)) AS ykat_id,
		CAST(YKAT_ID_TEXT AS VARCHAR(MAX)) AS ykat_id_text,
		CAST(YKAT_PASSIV AS VARCHAR(MAX)) AS ykat_passiv,
		CAST(YKAT_TEXT AS VARCHAR(MAX)) AS ykat_text 
	FROM utdata.utdata295.EK_DIM_OBJ_YKAT) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    