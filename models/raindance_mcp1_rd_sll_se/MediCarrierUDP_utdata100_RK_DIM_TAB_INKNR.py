
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DUMMY2': 'varchar(max)', 'TAB_INKNR': 'varchar(max)', 'TAB_INKNR_ID_TEXT': 'varchar(max)', 'TAB_INKNR_TEXT': 'varchar(max)'},
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
		CAST(DUMMY2 AS VARCHAR(MAX)) AS DUMMY2,
		CAST(TAB_INKNR AS VARCHAR(MAX)) AS TAB_INKNR,
		CAST(TAB_INKNR_ID_TEXT AS VARCHAR(MAX)) AS TAB_INKNR_ID_TEXT,
		CAST(TAB_INKNR_TEXT AS VARCHAR(MAX)) AS TAB_INKNR_TEXT 
	FROM MediCarrierUDP.utdata100.RK_DIM_TAB_INKNR ) y

	"""
    return read(query=query, server_url="mcp1.rd.sll.se")
    