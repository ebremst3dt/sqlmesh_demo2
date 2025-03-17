
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DUMMY2': 'varchar(max)', 'TAB_ARTIST': 'varchar(max)', 'TAB_ARTIST_ID_TEXT': 'varchar(max)', 'TAB_ARTIST_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata361' as _source,
		CAST(DUMMY2 AS VARCHAR(MAX)) AS dummy2,
		CAST(TAB_ARTIST AS VARCHAR(MAX)) AS tab_artist,
		CAST(TAB_ARTIST_ID_TEXT AS VARCHAR(MAX)) AS tab_artist_id_text,
		CAST(TAB_ARTIST_TEXT AS VARCHAR(MAX)) AS tab_artist_text 
	FROM utdata.utdata361.RK_DIM_TAB_ARTIST ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    