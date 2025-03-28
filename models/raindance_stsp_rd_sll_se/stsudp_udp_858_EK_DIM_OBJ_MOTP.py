
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MOTP_GILTIG_FOM': 'varchar(max)', 'MOTP_GILTIG_TOM': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTP_ID_TEXT': 'varchar(max)', 'MOTP_PASSIV': 'varchar(max)', 'MOTP_TEXT': 'varchar(max)', 'MOTSLL_GILTIG_FOM': 'varchar(max)', 'MOTSLL_GILTIG_TOM': 'varchar(max)', 'MOTSLL_ID': 'varchar(max)', 'MOTSLL_ID_TEXT': 'varchar(max)', 'MOTSLL_PASSIV': 'varchar(max)', 'MOTSLL_TEXT': 'varchar(max)', 'MPANSV_GILTIG_FOM': 'varchar(max)', 'MPANSV_GILTIG_TOM': 'varchar(max)', 'MPANSV_ID': 'varchar(max)', 'MPANSV_ID_TEXT': 'varchar(max)', 'MPANSV_PASSIV': 'varchar(max)', 'MPANSV_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS MOTP_GILTIG_FOM,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS MOTP_GILTIG_TOM,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS MOTP_ID_TEXT,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS MOTP_PASSIV,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS MOTP_TEXT,
		CONVERT(varchar(max), MOTSLL_GILTIG_FOM, 126) AS MOTSLL_GILTIG_FOM,
		CONVERT(varchar(max), MOTSLL_GILTIG_TOM, 126) AS MOTSLL_GILTIG_TOM,
		CAST(MOTSLL_ID AS VARCHAR(MAX)) AS MOTSLL_ID,
		CAST(MOTSLL_ID_TEXT AS VARCHAR(MAX)) AS MOTSLL_ID_TEXT,
		CAST(MOTSLL_PASSIV AS VARCHAR(MAX)) AS MOTSLL_PASSIV,
		CAST(MOTSLL_TEXT AS VARCHAR(MAX)) AS MOTSLL_TEXT,
		CONVERT(varchar(max), MPANSV_GILTIG_FOM, 126) AS MPANSV_GILTIG_FOM,
		CONVERT(varchar(max), MPANSV_GILTIG_TOM, 126) AS MPANSV_GILTIG_TOM,
		CAST(MPANSV_ID AS VARCHAR(MAX)) AS MPANSV_ID,
		CAST(MPANSV_ID_TEXT AS VARCHAR(MAX)) AS MPANSV_ID_TEXT,
		CAST(MPANSV_PASSIV AS VARCHAR(MAX)) AS MPANSV_PASSIV,
		CAST(MPANSV_TEXT AS VARCHAR(MAX)) AS MPANSV_TEXT 
	FROM stsudp.udp_858.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    