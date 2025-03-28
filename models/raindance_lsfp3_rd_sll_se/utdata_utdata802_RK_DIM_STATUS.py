
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FAKTSTATUS': 'varchar(max)', 'FAKTSTATUSTYP': 'varchar(max)', 'FAKTSTATUSTYP_TEXT': 'varchar(max)', 'FAKTSTATUS_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata802' as _source,
		CAST(FAKTSTATUS AS VARCHAR(MAX)) AS FAKTSTATUS,
		CAST(FAKTSTATUSTYP AS VARCHAR(MAX)) AS FAKTSTATUSTYP,
		CAST(FAKTSTATUSTYP_TEXT AS VARCHAR(MAX)) AS FAKTSTATUSTYP_TEXT,
		CAST(FAKTSTATUS_TEXT AS VARCHAR(MAX)) AS FAKTSTATUS_TEXT 
	FROM utdata.utdata802.RK_DIM_STATUS ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    