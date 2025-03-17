
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'OTYP_GILTIG_FOM': 'varchar(max)', 'OTYP_GILTIG_TOM': 'varchar(max)', 'OTYP_ID': 'varchar(max)', 'OTYP_ID_TEXT': 'varchar(max)', 'OTYP_PASSIV': 'varchar(max)', 'OTYP_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), OTYP_GILTIG_FOM, 126) AS otyp_giltig_fom,
		CONVERT(varchar(max), OTYP_GILTIG_TOM, 126) AS otyp_giltig_tom,
		CAST(OTYP_ID AS VARCHAR(MAX)) AS otyp_id,
		CAST(OTYP_ID_TEXT AS VARCHAR(MAX)) AS otyp_id_text,
		CAST(OTYP_PASSIV AS VARCHAR(MAX)) AS otyp_passiv,
		CAST(OTYP_TEXT AS VARCHAR(MAX)) AS otyp_text 
	FROM steudp.udp_600.EK_DIM_OBJ_OTYP ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    