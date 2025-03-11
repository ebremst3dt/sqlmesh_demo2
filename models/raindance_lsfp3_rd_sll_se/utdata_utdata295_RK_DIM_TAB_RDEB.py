
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'DUMMY2': 'varchar(max)', 'TAB_RDEB': 'varchar(max)', 'TAB_RDEB_ID_TEXT': 'varchar(max)', 'TAB_RDEB_TEXT': 'varchar(max)', 'VARDE1': 'varchar(max)', 'VARDE2': 'varchar(max)'},
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
		CAST(DUMMY2 AS VARCHAR(MAX)) AS dummy2,
		CAST(TAB_RDEB AS VARCHAR(MAX)) AS tab_rdeb,
		CAST(TAB_RDEB_ID_TEXT AS VARCHAR(MAX)) AS tab_rdeb_id_text,
		CAST(TAB_RDEB_TEXT AS VARCHAR(MAX)) AS tab_rdeb_text,
		CAST(VARDE1 AS VARCHAR(MAX)) AS varde1,
		CAST(VARDE2 AS VARCHAR(MAX)) AS varde2 
	FROM utdata.utdata295.RK_DIM_TAB_RDEB

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    