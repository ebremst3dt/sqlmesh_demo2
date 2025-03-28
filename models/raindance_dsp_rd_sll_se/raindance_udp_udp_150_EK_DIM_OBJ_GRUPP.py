
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FAKTCE_GILTIG_FOM': 'varchar(max)', 'FAKTCE_GILTIG_TOM': 'varchar(max)', 'FAKTCE_ID': 'varchar(max)', 'FAKTCE_ID_TEXT': 'varchar(max)', 'FAKTCE_PASSIV': 'varchar(max)', 'FAKTCE_TEXT': 'varchar(max)', 'GRUPP_GILTIG_FOM': 'varchar(max)', 'GRUPP_GILTIG_TOM': 'varchar(max)', 'GRUPP_ID': 'varchar(max)', 'GRUPP_ID_TEXT': 'varchar(max)', 'GRUPP_PASSIV': 'varchar(max)', 'GRUPP_TEXT': 'varchar(max)'},
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
		'dsp_rd_sll_se_raindance_udp_udp_150' as _source,
		CONVERT(varchar(max), FAKTCE_GILTIG_FOM, 126) AS FAKTCE_GILTIG_FOM,
		CONVERT(varchar(max), FAKTCE_GILTIG_TOM, 126) AS FAKTCE_GILTIG_TOM,
		CAST(FAKTCE_ID AS VARCHAR(MAX)) AS FAKTCE_ID,
		CAST(FAKTCE_ID_TEXT AS VARCHAR(MAX)) AS FAKTCE_ID_TEXT,
		CAST(FAKTCE_PASSIV AS VARCHAR(MAX)) AS FAKTCE_PASSIV,
		CAST(FAKTCE_TEXT AS VARCHAR(MAX)) AS FAKTCE_TEXT,
		CONVERT(varchar(max), GRUPP_GILTIG_FOM, 126) AS GRUPP_GILTIG_FOM,
		CONVERT(varchar(max), GRUPP_GILTIG_TOM, 126) AS GRUPP_GILTIG_TOM,
		CAST(GRUPP_ID AS VARCHAR(MAX)) AS GRUPP_ID,
		CAST(GRUPP_ID_TEXT AS VARCHAR(MAX)) AS GRUPP_ID_TEXT,
		CAST(GRUPP_PASSIV AS VARCHAR(MAX)) AS GRUPP_PASSIV,
		CAST(GRUPP_TEXT AS VARCHAR(MAX)) AS GRUPP_TEXT 
	FROM raindance_udp.udp_150.EK_DIM_OBJ_GRUPP ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    