
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DUMMY2': 'varchar(max)', 'TAB_MOTP': 'varchar(max)', 'TAB_MOTP_ID_TEXT': 'varchar(max)', 'TAB_MOTP_TEXT': 'varchar(max)'},
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
		'stsp_rd_sll_se_stsudp_udp_858' as _source,
		CAST(DUMMY2 AS VARCHAR(MAX)) AS DUMMY2,
		CAST(TAB_MOTP AS VARCHAR(MAX)) AS TAB_MOTP,
		CAST(TAB_MOTP_ID_TEXT AS VARCHAR(MAX)) AS TAB_MOTP_ID_TEXT,
		CAST(TAB_MOTP_TEXT AS VARCHAR(MAX)) AS TAB_MOTP_TEXT 
	FROM stsudp.udp_858.RK_DIM_TAB_MOTP ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    