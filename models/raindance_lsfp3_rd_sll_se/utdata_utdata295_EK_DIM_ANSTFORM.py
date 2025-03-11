
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'ANSTFORM_ID': 'varchar(max)', 'ANSTFORM_ID_TEXT': 'varchar(max)', 'ANSTFORM_TEXT': 'varchar(max)'},
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
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(ANSTFORM_ID AS VARCHAR(MAX)) AS anstform_id,
		CAST(ANSTFORM_ID_TEXT AS VARCHAR(MAX)) AS anstform_id_text,
		CAST(ANSTFORM_TEXT AS VARCHAR(MAX)) AS anstform_text 
	FROM utdata.utdata295.EK_DIM_ANSTFORM

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    