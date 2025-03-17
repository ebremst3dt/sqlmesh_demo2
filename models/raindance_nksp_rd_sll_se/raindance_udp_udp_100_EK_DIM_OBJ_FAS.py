
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
		CONVERT(varchar(max), FAS_GILTIG_FOM, 126) AS fas_giltig_fom,
		CONVERT(varchar(max), FAS_GILTIG_TOM, 126) AS fas_giltig_tom,
		CAST(FAS_ID AS VARCHAR(MAX)) AS fas_id,
		CAST(FAS_ID_TEXT AS VARCHAR(MAX)) AS fas_id_text,
		CAST(FAS_PASSIV AS VARCHAR(MAX)) AS fas_passiv,
		CAST(FAS_TEXT AS VARCHAR(MAX)) AS fas_text,
		CONVERT(varchar(max), GRFAS_GILTIG_FOM, 126) AS grfas_giltig_fom,
		CONVERT(varchar(max), GRFAS_GILTIG_TOM, 126) AS grfas_giltig_tom,
		CAST(GRFAS_ID AS VARCHAR(MAX)) AS grfas_id,
		CAST(GRFAS_ID_TEXT AS VARCHAR(MAX)) AS grfas_id_text,
		CAST(GRFAS_PASSIV AS VARCHAR(MAX)) AS grfas_passiv,
		CAST(GRFAS_TEXT AS VARCHAR(MAX)) AS grfas_text 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_FAS ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    