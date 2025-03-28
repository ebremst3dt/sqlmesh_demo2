
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FAS_GILTIG_FOM': 'varchar(max)', 'FAS_GILTIG_TOM': 'varchar(max)', 'FAS_ID': 'varchar(max)', 'FAS_ID_TEXT': 'varchar(max)', 'FAS_PASSIV': 'varchar(max)', 'FAS_TEXT': 'varchar(max)', 'GRFAS_GILTIG_FOM': 'varchar(max)', 'GRFAS_GILTIG_TOM': 'varchar(max)', 'GRFAS_ID': 'varchar(max)', 'GRFAS_ID_TEXT': 'varchar(max)', 'GRFAS_PASSIV': 'varchar(max)', 'GRFAS_TEXT': 'varchar(max)'},
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
		'nksp_rd_sll_se_raindance_udp_udp_100' as _source,
		CONVERT(varchar(max), FAS_GILTIG_FOM, 126) AS FAS_GILTIG_FOM,
		CONVERT(varchar(max), FAS_GILTIG_TOM, 126) AS FAS_GILTIG_TOM,
		CAST(FAS_ID AS VARCHAR(MAX)) AS FAS_ID,
		CAST(FAS_ID_TEXT AS VARCHAR(MAX)) AS FAS_ID_TEXT,
		CAST(FAS_PASSIV AS VARCHAR(MAX)) AS FAS_PASSIV,
		CAST(FAS_TEXT AS VARCHAR(MAX)) AS FAS_TEXT,
		CONVERT(varchar(max), GRFAS_GILTIG_FOM, 126) AS GRFAS_GILTIG_FOM,
		CONVERT(varchar(max), GRFAS_GILTIG_TOM, 126) AS GRFAS_GILTIG_TOM,
		CAST(GRFAS_ID AS VARCHAR(MAX)) AS GRFAS_ID,
		CAST(GRFAS_ID_TEXT AS VARCHAR(MAX)) AS GRFAS_ID_TEXT,
		CAST(GRFAS_PASSIV AS VARCHAR(MAX)) AS GRFAS_PASSIV,
		CAST(GRFAS_TEXT AS VARCHAR(MAX)) AS GRFAS_TEXT 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_FAS ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    