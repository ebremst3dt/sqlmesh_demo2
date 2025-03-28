
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DELSYS': 'varchar(max)', 'DELSYS_TEXT': 'varchar(max)', 'DOKUMENTTYP': 'varchar(max)', 'EXTERNID': 'varchar(max)', 'EXTERNID2': 'varchar(max)', 'EXTERNID2_ID_TEXT': 'varchar(max)', 'EXTERNID_GRUPP': 'varchar(max)', 'EXTERNID_ID_TEXT': 'varchar(max)', 'EXTERNID_TEXT': 'varchar(max)', 'NAMN2': 'varchar(max)'},
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
		CAST(DELSYS AS VARCHAR(MAX)) AS DELSYS,
		CAST(DELSYS_TEXT AS VARCHAR(MAX)) AS DELSYS_TEXT,
		CAST(DOKUMENTTYP AS VARCHAR(MAX)) AS DOKUMENTTYP,
		CAST(EXTERNID AS VARCHAR(MAX)) AS EXTERNID,
		CAST(EXTERNID2 AS VARCHAR(MAX)) AS EXTERNID2,
		CAST(EXTERNID2_ID_TEXT AS VARCHAR(MAX)) AS EXTERNID2_ID_TEXT,
		CAST(EXTERNID_GRUPP AS VARCHAR(MAX)) AS EXTERNID_GRUPP,
		CAST(EXTERNID_ID_TEXT AS VARCHAR(MAX)) AS EXTERNID_ID_TEXT,
		CAST(EXTERNID_TEXT AS VARCHAR(MAX)) AS EXTERNID_TEXT,
		CAST(NAMN2 AS VARCHAR(MAX)) AS NAMN2 
	FROM utdata.utdata361.EK_DIM_EXTERNID ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    