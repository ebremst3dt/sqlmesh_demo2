
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KMOTP_GILTIG_FOM': 'varchar(max)', 'KMOTP_GILTIG_TOM': 'varchar(max)', 'KMOTP_ID': 'varchar(max)', 'KMOTP_ID_TEXT': 'varchar(max)', 'KMOTP_PASSIV': 'varchar(max)', 'KMOTP_TEXT': 'varchar(max)', 'MGRP_GILTIG_FOM': 'varchar(max)', 'MGRP_GILTIG_TOM': 'varchar(max)', 'MGRP_ID': 'varchar(max)', 'MGRP_ID_TEXT': 'varchar(max)', 'MGRP_PASSIV': 'varchar(max)', 'MGRP_TEXT': 'varchar(max)', 'MOTP_GILTIG_FOM': 'varchar(max)', 'MOTP_GILTIG_TOM': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTP_ID_TEXT': 'varchar(max)', 'MOTP_PASSIV': 'varchar(max)', 'MOTP_TEXT': 'varchar(max)'},
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
		'dsp_rd_sll_se_raindance_udp_udp_150' as _source,
		CONVERT(varchar(max), KMOTP_GILTIG_FOM, 126) AS KMOTP_GILTIG_FOM,
		CONVERT(varchar(max), KMOTP_GILTIG_TOM, 126) AS KMOTP_GILTIG_TOM,
		CAST(KMOTP_ID AS VARCHAR(MAX)) AS KMOTP_ID,
		CAST(KMOTP_ID_TEXT AS VARCHAR(MAX)) AS KMOTP_ID_TEXT,
		CAST(KMOTP_PASSIV AS VARCHAR(MAX)) AS KMOTP_PASSIV,
		CAST(KMOTP_TEXT AS VARCHAR(MAX)) AS KMOTP_TEXT,
		CONVERT(varchar(max), MGRP_GILTIG_FOM, 126) AS MGRP_GILTIG_FOM,
		CONVERT(varchar(max), MGRP_GILTIG_TOM, 126) AS MGRP_GILTIG_TOM,
		CAST(MGRP_ID AS VARCHAR(MAX)) AS MGRP_ID,
		CAST(MGRP_ID_TEXT AS VARCHAR(MAX)) AS MGRP_ID_TEXT,
		CAST(MGRP_PASSIV AS VARCHAR(MAX)) AS MGRP_PASSIV,
		CAST(MGRP_TEXT AS VARCHAR(MAX)) AS MGRP_TEXT,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS MOTP_GILTIG_FOM,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS MOTP_GILTIG_TOM,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS MOTP_ID_TEXT,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS MOTP_PASSIV,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS MOTP_TEXT 
	FROM raindance_udp.udp_150.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    