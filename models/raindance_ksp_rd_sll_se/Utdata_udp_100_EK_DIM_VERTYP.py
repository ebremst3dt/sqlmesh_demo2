
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DELSYSTEM': 'varchar(max)', 'DELSYSTEM_TEXT': 'varchar(max)', 'VERTYP': 'varchar(max)', 'VERTYP_PASSIV': 'varchar(max)', 'VERTYP_TEXT': 'varchar(max)'},
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
		CAST(DELSYSTEM AS VARCHAR(MAX)) AS delsystem,
		CAST(DELSYSTEM_TEXT AS VARCHAR(MAX)) AS delsystem_text,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(VERTYP_PASSIV AS VARCHAR(MAX)) AS vertyp_passiv,
		CAST(VERTYP_TEXT AS VARCHAR(MAX)) AS vertyp_text 
	FROM Utdata.udp_100.EK_DIM_VERTYP ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    