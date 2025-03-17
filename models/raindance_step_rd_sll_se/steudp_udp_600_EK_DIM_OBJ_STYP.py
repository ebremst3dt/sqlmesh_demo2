
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'STYP_GILTIG_FOM': 'varchar(max)', 'STYP_GILTIG_TOM': 'varchar(max)', 'STYP_ID': 'varchar(max)', 'STYP_ID_TEXT': 'varchar(max)', 'STYP_PASSIV': 'varchar(max)', 'STYP_TEXT': 'varchar(max)'},
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
		'step_rd_sll_se_steudp_udp_600' as _source,
		CONVERT(varchar(max), STYP_GILTIG_FOM, 126) AS styp_giltig_fom,
		CONVERT(varchar(max), STYP_GILTIG_TOM, 126) AS styp_giltig_tom,
		CAST(STYP_ID AS VARCHAR(MAX)) AS styp_id,
		CAST(STYP_ID_TEXT AS VARCHAR(MAX)) AS styp_id_text,
		CAST(STYP_PASSIV AS VARCHAR(MAX)) AS styp_passiv,
		CAST(STYP_TEXT AS VARCHAR(MAX)) AS styp_text 
	FROM steudp.udp_600.EK_DIM_OBJ_STYP ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    