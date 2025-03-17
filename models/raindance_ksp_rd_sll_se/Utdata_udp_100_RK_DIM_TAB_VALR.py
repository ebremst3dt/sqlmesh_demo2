
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DUMMY2': 'varchar(max)', 'TAB_VALR': 'varchar(max)', 'TAB_VALR_ID_TEXT': 'varchar(max)', 'TAB_VALR_TEXT': 'varchar(max)', 'VARDE2': 'varchar(max)'},
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
		'ksp_rd_sll_se_Utdata_udp_100' as _source,
		CAST(DUMMY2 AS VARCHAR(MAX)) AS dummy2,
		CAST(TAB_VALR AS VARCHAR(MAX)) AS tab_valr,
		CAST(TAB_VALR_ID_TEXT AS VARCHAR(MAX)) AS tab_valr_id_text,
		CAST(TAB_VALR_TEXT AS VARCHAR(MAX)) AS tab_valr_text,
		CAST(VARDE2 AS VARCHAR(MAX)) AS varde2 
	FROM Utdata.udp_100.RK_DIM_TAB_VALR ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    