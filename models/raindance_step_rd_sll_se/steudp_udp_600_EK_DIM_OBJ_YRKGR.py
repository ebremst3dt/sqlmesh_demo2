
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'YRKGR_GILTIG_FOM': 'varchar(max)', 'YRKGR_GILTIG_TOM': 'varchar(max)', 'YRKGR_ID': 'varchar(max)', 'YRKGR_ID_TEXT': 'varchar(max)', 'YRKGR_PASSIV': 'varchar(max)', 'YRKGR_TEXT': 'varchar(max)', 'YRTOT_GILTIG_FOM': 'varchar(max)', 'YRTOT_GILTIG_TOM': 'varchar(max)', 'YRTOT_ID': 'varchar(max)', 'YRTOT_ID_TEXT': 'varchar(max)', 'YRTOT_PASSIV': 'varchar(max)', 'YRTOT_TEXT': 'varchar(max)'},
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
		'step_rd_sll_se_steudp_udp_600' as _source,
		CONVERT(varchar(max), YRKGR_GILTIG_FOM, 126) AS YRKGR_GILTIG_FOM,
		CONVERT(varchar(max), YRKGR_GILTIG_TOM, 126) AS YRKGR_GILTIG_TOM,
		CAST(YRKGR_ID AS VARCHAR(MAX)) AS YRKGR_ID,
		CAST(YRKGR_ID_TEXT AS VARCHAR(MAX)) AS YRKGR_ID_TEXT,
		CAST(YRKGR_PASSIV AS VARCHAR(MAX)) AS YRKGR_PASSIV,
		CAST(YRKGR_TEXT AS VARCHAR(MAX)) AS YRKGR_TEXT,
		CONVERT(varchar(max), YRTOT_GILTIG_FOM, 126) AS YRTOT_GILTIG_FOM,
		CONVERT(varchar(max), YRTOT_GILTIG_TOM, 126) AS YRTOT_GILTIG_TOM,
		CAST(YRTOT_ID AS VARCHAR(MAX)) AS YRTOT_ID,
		CAST(YRTOT_ID_TEXT AS VARCHAR(MAX)) AS YRTOT_ID_TEXT,
		CAST(YRTOT_PASSIV AS VARCHAR(MAX)) AS YRTOT_PASSIV,
		CAST(YRTOT_TEXT AS VARCHAR(MAX)) AS YRTOT_TEXT 
	FROM steudp.udp_600.EK_DIM_OBJ_YRKGR ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    