
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DUMMY2': 'varchar(max)', 'TAB_BILAGA': 'varchar(max)', 'TAB_BILAGA_ID_TEXT': 'varchar(max)', 'TAB_BILAGA_TEXT': 'varchar(max)'},
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
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CAST(DUMMY2 AS VARCHAR(MAX)) AS DUMMY2,
		CAST(TAB_BILAGA AS VARCHAR(MAX)) AS TAB_BILAGA,
		CAST(TAB_BILAGA_ID_TEXT AS VARCHAR(MAX)) AS TAB_BILAGA_ID_TEXT,
		CAST(TAB_BILAGA_TEXT AS VARCHAR(MAX)) AS TAB_BILAGA_TEXT 
	FROM udpb4.udpb4_100.RK_DIM_TAB_BILAGA ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    