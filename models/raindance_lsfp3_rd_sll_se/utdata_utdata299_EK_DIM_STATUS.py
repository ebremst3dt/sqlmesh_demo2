
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'STATUS': 'varchar(max)', 'STATUSTYP': 'varchar(max)', 'STATUSTYP_TEXT': 'varchar(max)', 'STATUS_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata299' as _source,
		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(STATUSTYP AS VARCHAR(MAX)) AS statustyp,
		CAST(STATUSTYP_TEXT AS VARCHAR(MAX)) AS statustyp_text,
		CAST(STATUS_TEXT AS VARCHAR(MAX)) AS status_text 
	FROM utdata.utdata299.EK_DIM_STATUS ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    