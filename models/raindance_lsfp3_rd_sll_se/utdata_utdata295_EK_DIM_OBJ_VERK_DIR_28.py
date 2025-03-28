
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'VERK_GILTIG_FOM': 'varchar(max)', 'VERK_GILTIG_TOM': 'varchar(max)', 'VERK_ID': 'varchar(max)', 'VERK_ID_TEXT': 'varchar(max)', 'VERK_PASSIV': 'varchar(max)', 'VERK_TEXT': 'varchar(max)', 'VGREN_GILTIG_FOM': 'varchar(max)', 'VGREN_GILTIG_TOM': 'varchar(max)', 'VGREN_ID': 'varchar(max)', 'VGREN_ID_TEXT': 'varchar(max)', 'VGREN_PASSIV': 'varchar(max)', 'VGREN_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), VERK_GILTIG_FOM, 126) AS VERK_GILTIG_FOM,
		CONVERT(varchar(max), VERK_GILTIG_TOM, 126) AS VERK_GILTIG_TOM,
		CAST(VERK_ID AS VARCHAR(MAX)) AS VERK_ID,
		CAST(VERK_ID_TEXT AS VARCHAR(MAX)) AS VERK_ID_TEXT,
		CAST(VERK_PASSIV AS VARCHAR(MAX)) AS VERK_PASSIV,
		CAST(VERK_TEXT AS VARCHAR(MAX)) AS VERK_TEXT,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS VGREN_GILTIG_FOM,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS VGREN_GILTIG_TOM,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS VGREN_ID,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS VGREN_ID_TEXT,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS VGREN_PASSIV,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS VGREN_TEXT 
	FROM utdata.utdata295.EK_DIM_OBJ_VERK_DIR_28 ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    