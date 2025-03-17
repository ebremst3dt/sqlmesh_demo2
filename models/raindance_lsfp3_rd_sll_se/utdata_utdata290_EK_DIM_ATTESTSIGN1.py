
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATTESTSIGN1': 'varchar(max)', 'ATTESTSIGN12': 'varchar(max)', 'ATTESTSIGN12_ID_TEXT': 'varchar(max)', 'ATTESTSIGN1_ID_TEXT': 'varchar(max)', 'ATTESTSIGN1_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata290' as _source,
		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN12 AS VARCHAR(MAX)) AS attestsign12,
		CAST(ATTESTSIGN12_ID_TEXT AS VARCHAR(MAX)) AS attestsign12_id_text,
		CAST(ATTESTSIGN1_ID_TEXT AS VARCHAR(MAX)) AS attestsign1_id_text,
		CAST(ATTESTSIGN1_TEXT AS VARCHAR(MAX)) AS attestsign1_text 
	FROM utdata.utdata290.EK_DIM_ATTESTSIGN1 ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    