
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANSTFORM_ID': 'varchar(max)', 'ANSTFORM_ID_TEXT': 'varchar(max)', 'ANSTFORM_TEXT': 'varchar(max)'},
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
		'mcp1_rd_sll_se_MediCarrierUDP_utdata100' as _source,
		CAST(ANSTFORM_ID AS VARCHAR(MAX)) AS anstform_id,
		CAST(ANSTFORM_ID_TEXT AS VARCHAR(MAX)) AS anstform_id_text,
		CAST(ANSTFORM_TEXT AS VARCHAR(MAX)) AS anstform_text 
	FROM MediCarrierUDP.utdata100.EK_DIM_ANSTFORM ) y

	"""
    return read(query=query, server_url="mcp1.rd.sll.se")
    