
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DATUM_FOM': 'varchar(max)', 'DATUM_TOM': 'varchar(max)', 'NUMERISK_VANSTER': 'varchar(max)', 'STYRD_ID': 'varchar(max)', 'STYRD_NR': 'varchar(max)', 'URVAL': 'varchar(max)'},
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
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(NUMERISK_VANSTER AS VARCHAR(MAX)) AS numerisk_vanster,
		CAST(STYRD_ID AS VARCHAR(MAX)) AS styrd_id,
		CAST(STYRD_NR AS VARCHAR(MAX)) AS styrd_nr,
		CAST(URVAL AS VARCHAR(MAX)) AS urval 
	FROM utdata.utdata295.EK_SAMBAND_STYRDA) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    