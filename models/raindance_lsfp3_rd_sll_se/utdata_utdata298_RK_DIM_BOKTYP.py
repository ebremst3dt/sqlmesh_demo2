
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BOKTYP': 'varchar(max)', 'BOKTYP_ID': 'varchar(max)', 'BOKTYP_ID_TEXT': 'varchar(max)', 'BOKTYP_NR': 'varchar(max)', 'BOKTYP_TEXT': 'varchar(max)'},
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
		CAST(BOKTYP AS VARCHAR(MAX)) AS BOKTYP,
		CAST(BOKTYP_ID AS VARCHAR(MAX)) AS BOKTYP_ID,
		CAST(BOKTYP_ID_TEXT AS VARCHAR(MAX)) AS BOKTYP_ID_TEXT,
		CAST(BOKTYP_NR AS VARCHAR(MAX)) AS BOKTYP_NR,
		CAST(BOKTYP_TEXT AS VARCHAR(MAX)) AS BOKTYP_TEXT 
	FROM utdata.utdata298.RK_DIM_BOKTYP ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    