
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DUMMY2': 'varchar(max)', 'TAB_UBF': 'varchar(max)', 'TAB_UBF_ID_TEXT': 'varchar(max)', 'TAB_UBF_TEXT': 'varchar(max)', 'VARDE1': 'varchar(max)', 'VARDE2': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CAST(DUMMY2 AS VARCHAR(MAX)) AS DUMMY2,
		CAST(TAB_UBF AS VARCHAR(MAX)) AS TAB_UBF,
		CAST(TAB_UBF_ID_TEXT AS VARCHAR(MAX)) AS TAB_UBF_ID_TEXT,
		CAST(TAB_UBF_TEXT AS VARCHAR(MAX)) AS TAB_UBF_TEXT,
		CAST(VARDE1 AS VARCHAR(MAX)) AS VARDE1,
		CAST(VARDE2 AS VARCHAR(MAX)) AS VARDE2 
	FROM utdata.utdata298.RK_DIM_TAB_UBF ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    