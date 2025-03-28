
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MIE_GILTIG_FOM': 'varchar(max)', 'MIE_GILTIG_TOM': 'varchar(max)', 'MIE_ID': 'varchar(max)', 'MIE_ID_TEXT': 'varchar(max)', 'MIE_PASSIV': 'varchar(max)', 'MIE_TEXT': 'varchar(max)', 'MOTPGR_GILTIG_FOM': 'varchar(max)', 'MOTPGR_GILTIG_TOM': 'varchar(max)', 'MOTPGR_ID': 'varchar(max)', 'MOTPGR_ID_TEXT': 'varchar(max)', 'MOTPGR_PASSIV': 'varchar(max)', 'MOTPGR_TEXT': 'varchar(max)', 'MOTP_GILTIG_FOM': 'varchar(max)', 'MOTP_GILTIG_TOM': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTP_ID_TEXT': 'varchar(max)', 'MOTP_PASSIV': 'varchar(max)', 'MOTP_TEXT': 'varchar(max)', 'SMOT_GILTIG_FOM': 'varchar(max)', 'SMOT_GILTIG_TOM': 'varchar(max)', 'SMOT_ID': 'varchar(max)', 'SMOT_ID_TEXT': 'varchar(max)', 'SMOT_PASSIV': 'varchar(max)', 'SMOT_TEXT': 'varchar(max)'},
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
		'sosp_rd_sll_se_raindance_udp_udp_220' as _source,
		CONVERT(varchar(max), MIE_GILTIG_FOM, 126) AS MIE_GILTIG_FOM,
		CONVERT(varchar(max), MIE_GILTIG_TOM, 126) AS MIE_GILTIG_TOM,
		CAST(MIE_ID AS VARCHAR(MAX)) AS MIE_ID,
		CAST(MIE_ID_TEXT AS VARCHAR(MAX)) AS MIE_ID_TEXT,
		CAST(MIE_PASSIV AS VARCHAR(MAX)) AS MIE_PASSIV,
		CAST(MIE_TEXT AS VARCHAR(MAX)) AS MIE_TEXT,
		CONVERT(varchar(max), MOTPGR_GILTIG_FOM, 126) AS MOTPGR_GILTIG_FOM,
		CONVERT(varchar(max), MOTPGR_GILTIG_TOM, 126) AS MOTPGR_GILTIG_TOM,
		CAST(MOTPGR_ID AS VARCHAR(MAX)) AS MOTPGR_ID,
		CAST(MOTPGR_ID_TEXT AS VARCHAR(MAX)) AS MOTPGR_ID_TEXT,
		CAST(MOTPGR_PASSIV AS VARCHAR(MAX)) AS MOTPGR_PASSIV,
		CAST(MOTPGR_TEXT AS VARCHAR(MAX)) AS MOTPGR_TEXT,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS MOTP_GILTIG_FOM,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS MOTP_GILTIG_TOM,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS MOTP_ID_TEXT,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS MOTP_PASSIV,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS MOTP_TEXT,
		CONVERT(varchar(max), SMOT_GILTIG_FOM, 126) AS SMOT_GILTIG_FOM,
		CONVERT(varchar(max), SMOT_GILTIG_TOM, 126) AS SMOT_GILTIG_TOM,
		CAST(SMOT_ID AS VARCHAR(MAX)) AS SMOT_ID,
		CAST(SMOT_ID_TEXT AS VARCHAR(MAX)) AS SMOT_ID_TEXT,
		CAST(SMOT_PASSIV AS VARCHAR(MAX)) AS SMOT_PASSIV,
		CAST(SMOT_TEXT AS VARCHAR(MAX)) AS SMOT_TEXT 
	FROM raindance_udp.udp_220.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    