
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AR': 'varchar(max)', 'INTERNVERNR': 'varchar(max)', 'INTERNVERNR_TEXT': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERNRGRUPP': 'varchar(max)'},
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
		CAST(AR AS VARCHAR(MAX)) AS ar,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CAST(INTERNVERNR_TEXT AS VARCHAR(MAX)) AS internvernr_text,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERNRGRUPP AS VARCHAR(MAX)) AS vernrgrupp 
	FROM Utdata.udp_100.EK_DIM_VERNR ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    