
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
		CONVERT(varchar(max), KMOTP_GILTIG_FOM, 126) AS kmotp_giltig_fom,
		CONVERT(varchar(max), KMOTP_GILTIG_TOM, 126) AS kmotp_giltig_tom,
		CAST(KMOTP_ID AS VARCHAR(MAX)) AS kmotp_id,
		CAST(KMOTP_ID_TEXT AS VARCHAR(MAX)) AS kmotp_id_text,
		CAST(KMOTP_PASSIV AS VARCHAR(MAX)) AS kmotp_passiv,
		CAST(KMOTP_TEXT AS VARCHAR(MAX)) AS kmotp_text,
		CONVERT(varchar(max), MGRP_GILTIG_FOM, 126) AS mgrp_giltig_fom,
		CONVERT(varchar(max), MGRP_GILTIG_TOM, 126) AS mgrp_giltig_tom,
		CAST(MGRP_ID AS VARCHAR(MAX)) AS mgrp_id,
		CAST(MGRP_ID_TEXT AS VARCHAR(MAX)) AS mgrp_id_text,
		CAST(MGRP_PASSIV AS VARCHAR(MAX)) AS mgrp_passiv,
		CAST(MGRP_TEXT AS VARCHAR(MAX)) AS mgrp_text,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS motp_giltig_fom,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS motp_giltig_tom,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS motp_id_text,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS motp_passiv,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS motp_text 
	FROM raindance_udp.udp_150.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    