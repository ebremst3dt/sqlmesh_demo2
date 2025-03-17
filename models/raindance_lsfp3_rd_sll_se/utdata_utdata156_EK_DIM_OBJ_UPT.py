
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'UPT_GILTIG_FOM': 'varchar(max)', 'UPT_GILTIG_TOM': 'varchar(max)', 'UPT_ID': 'varchar(max)', 'UPT_ID_TEXT': 'varchar(max)', 'UPT_PASSIV': 'varchar(max)', 'UPT_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata156' as _source,
		CONVERT(varchar(max), UPT_GILTIG_FOM, 126) AS upt_giltig_fom,
		CONVERT(varchar(max), UPT_GILTIG_TOM, 126) AS upt_giltig_tom,
		CAST(UPT_ID AS VARCHAR(MAX)) AS upt_id,
		CAST(UPT_ID_TEXT AS VARCHAR(MAX)) AS upt_id_text,
		CAST(UPT_PASSIV AS VARCHAR(MAX)) AS upt_passiv,
		CAST(UPT_TEXT AS VARCHAR(MAX)) AS upt_text 
	FROM utdata.utdata156.EK_DIM_OBJ_UPT ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    