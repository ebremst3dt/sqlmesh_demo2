
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BOKFORINGSAR': 'varchar(max)', 'BOKFORINGSARSLUT': 'varchar(max)', 'BOKFORINGSAR_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), BOKFORINGSAR, 126) AS BOKFORINGSAR,
		CONVERT(varchar(max), BOKFORINGSARSLUT, 126) AS BOKFORINGSARSLUT,
		CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS BOKFORINGSAR_TEXT 
	FROM Utdata.udp_100.EK_DIM_BOKFORINGSAR ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    