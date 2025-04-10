
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FAC_GILTIG_FOM': 'varchar(max)', 'FAC_GILTIG_TOM': 'varchar(max)', 'FAC_ID': 'varchar(max)', 'FAC_ID_TEXT': 'varchar(max)', 'FAC_PASSIV': 'varchar(max)', 'FAC_TEXT': 'varchar(max)', 'GRUPP_GILTIG_FOM': 'varchar(max)', 'GRUPP_GILTIG_TOM': 'varchar(max)', 'GRUPP_ID': 'varchar(max)', 'GRUPP_ID_TEXT': 'varchar(max)', 'GRUPP_PASSIV': 'varchar(max)', 'GRUPP_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), FAC_GILTIG_FOM, 126) AS FAC_GILTIG_FOM,
		CONVERT(varchar(max), FAC_GILTIG_TOM, 126) AS FAC_GILTIG_TOM,
		CAST(FAC_ID AS VARCHAR(MAX)) AS FAC_ID,
		CAST(FAC_ID_TEXT AS VARCHAR(MAX)) AS FAC_ID_TEXT,
		CAST(FAC_PASSIV AS VARCHAR(MAX)) AS FAC_PASSIV,
		CAST(FAC_TEXT AS VARCHAR(MAX)) AS FAC_TEXT,
		CONVERT(varchar(max), GRUPP_GILTIG_FOM, 126) AS GRUPP_GILTIG_FOM,
		CONVERT(varchar(max), GRUPP_GILTIG_TOM, 126) AS GRUPP_GILTIG_TOM,
		CAST(GRUPP_ID AS VARCHAR(MAX)) AS GRUPP_ID,
		CAST(GRUPP_ID_TEXT AS VARCHAR(MAX)) AS GRUPP_ID_TEXT,
		CAST(GRUPP_PASSIV AS VARCHAR(MAX)) AS GRUPP_PASSIV,
		CAST(GRUPP_TEXT AS VARCHAR(MAX)) AS GRUPP_TEXT 
	FROM utdata.utdata156.EK_DIM_OBJ_GRUPP ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    