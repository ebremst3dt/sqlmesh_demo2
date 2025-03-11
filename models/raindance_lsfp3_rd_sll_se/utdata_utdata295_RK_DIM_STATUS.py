
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'FAKTSTATUS': 'varchar(max)', 'FAKTSTATUSTYP': 'varchar(max)', 'FAKTSTATUSTYP_TEXT': 'varchar(max)', 'FAKTSTATUS_TEXT': 'varchar(max)'},
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
    query = """
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(FAKTSTATUS AS VARCHAR(MAX)) AS faktstatus,
		CAST(FAKTSTATUSTYP AS VARCHAR(MAX)) AS faktstatustyp,
		CAST(FAKTSTATUSTYP_TEXT AS VARCHAR(MAX)) AS faktstatustyp_text,
		CAST(FAKTSTATUS_TEXT AS VARCHAR(MAX)) AS faktstatus_text 
	FROM utdata.utdata295.RK_DIM_STATUS) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    