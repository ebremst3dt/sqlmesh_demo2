
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MOTDIV_GILTIG_FOM': 'varchar(max)', 'MOTDIV_GILTIG_TOM': 'varchar(max)', 'MOTDIV_ID': 'varchar(max)', 'MOTDIV_ID_TEXT': 'varchar(max)', 'MOTDIV_PASSIV': 'varchar(max)', 'MOTDIV_TEXT': 'varchar(max)', 'MOTKLI_GILTIG_FOM': 'varchar(max)', 'MOTKLI_GILTIG_TOM': 'varchar(max)', 'MOTKLI_ID': 'varchar(max)', 'MOTKLI_ID_TEXT': 'varchar(max)', 'MOTKLI_PASSIV': 'varchar(max)', 'MOTKLI_TEXT': 'varchar(max)', 'MOTP_GILTIG_FOM': 'varchar(max)', 'MOTP_GILTIG_TOM': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTP_ID_TEXT': 'varchar(max)', 'MOTP_PASSIV': 'varchar(max)', 'MOTP_TEXT': 'varchar(max)', 'MOTSEK_GILTIG_FOM': 'varchar(max)', 'MOTSEK_GILTIG_TOM': 'varchar(max)', 'MOTSEK_ID': 'varchar(max)', 'MOTSEK_ID_TEXT': 'varchar(max)', 'MOTSEK_PASSIV': 'varchar(max)', 'MOTSEK_TEXT': 'varchar(max)', 'MOTSLL_GILTIG_FOM': 'varchar(max)', 'MOTSLL_GILTIG_TOM': 'varchar(max)', 'MOTSLL_ID': 'varchar(max)', 'MOTSLL_ID_TEXT': 'varchar(max)', 'MOTSLL_PASSIV': 'varchar(max)', 'MOTSLL_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), MOTDIV_GILTIG_FOM, 126) AS MOTDIV_GILTIG_FOM,
		CONVERT(varchar(max), MOTDIV_GILTIG_TOM, 126) AS MOTDIV_GILTIG_TOM,
		CAST(MOTDIV_ID AS VARCHAR(MAX)) AS MOTDIV_ID,
		CAST(MOTDIV_ID_TEXT AS VARCHAR(MAX)) AS MOTDIV_ID_TEXT,
		CAST(MOTDIV_PASSIV AS VARCHAR(MAX)) AS MOTDIV_PASSIV,
		CAST(MOTDIV_TEXT AS VARCHAR(MAX)) AS MOTDIV_TEXT,
		CONVERT(varchar(max), MOTKLI_GILTIG_FOM, 126) AS MOTKLI_GILTIG_FOM,
		CONVERT(varchar(max), MOTKLI_GILTIG_TOM, 126) AS MOTKLI_GILTIG_TOM,
		CAST(MOTKLI_ID AS VARCHAR(MAX)) AS MOTKLI_ID,
		CAST(MOTKLI_ID_TEXT AS VARCHAR(MAX)) AS MOTKLI_ID_TEXT,
		CAST(MOTKLI_PASSIV AS VARCHAR(MAX)) AS MOTKLI_PASSIV,
		CAST(MOTKLI_TEXT AS VARCHAR(MAX)) AS MOTKLI_TEXT,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS MOTP_GILTIG_FOM,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS MOTP_GILTIG_TOM,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS MOTP_ID_TEXT,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS MOTP_PASSIV,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS MOTP_TEXT,
		CONVERT(varchar(max), MOTSEK_GILTIG_FOM, 126) AS MOTSEK_GILTIG_FOM,
		CONVERT(varchar(max), MOTSEK_GILTIG_TOM, 126) AS MOTSEK_GILTIG_TOM,
		CAST(MOTSEK_ID AS VARCHAR(MAX)) AS MOTSEK_ID,
		CAST(MOTSEK_ID_TEXT AS VARCHAR(MAX)) AS MOTSEK_ID_TEXT,
		CAST(MOTSEK_PASSIV AS VARCHAR(MAX)) AS MOTSEK_PASSIV,
		CAST(MOTSEK_TEXT AS VARCHAR(MAX)) AS MOTSEK_TEXT,
		CONVERT(varchar(max), MOTSLL_GILTIG_FOM, 126) AS MOTSLL_GILTIG_FOM,
		CONVERT(varchar(max), MOTSLL_GILTIG_TOM, 126) AS MOTSLL_GILTIG_TOM,
		CAST(MOTSLL_ID AS VARCHAR(MAX)) AS MOTSLL_ID,
		CAST(MOTSLL_ID_TEXT AS VARCHAR(MAX)) AS MOTSLL_ID_TEXT,
		CAST(MOTSLL_PASSIV AS VARCHAR(MAX)) AS MOTSLL_PASSIV,
		CAST(MOTSLL_TEXT AS VARCHAR(MAX)) AS MOTSLL_TEXT 
	FROM Utdata.udp_100.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    