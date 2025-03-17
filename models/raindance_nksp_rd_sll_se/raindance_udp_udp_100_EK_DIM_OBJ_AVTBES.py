
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVTBES_GILTIG_FOM': 'varchar(max)', 'AVTBES_GILTIG_TOM': 'varchar(max)', 'AVTBES_ID': 'varchar(max)', 'AVTBES_ID_TEXT': 'varchar(max)', 'AVTBES_PASSIV': 'varchar(max)', 'AVTBES_TEXT': 'varchar(max)', 'BCHEF_GILTIG_FOM': 'varchar(max)', 'BCHEF_GILTIG_TOM': 'varchar(max)', 'BCHEF_ID': 'varchar(max)', 'BCHEF_ID_TEXT': 'varchar(max)', 'BCHEF_PASSIV': 'varchar(max)', 'BCHEF_TEXT': 'varchar(max)', 'BGRP_GILTIG_FOM': 'varchar(max)', 'BGRP_GILTIG_TOM': 'varchar(max)', 'BGRP_ID': 'varchar(max)', 'BGRP_ID_TEXT': 'varchar(max)', 'BGRP_PASSIV': 'varchar(max)', 'BGRP_TEXT': 'varchar(max)', 'BHANDL_GILTIG_FOM': 'varchar(max)', 'BHANDL_GILTIG_TOM': 'varchar(max)', 'BHANDL_ID': 'varchar(max)', 'BHANDL_ID_TEXT': 'varchar(max)', 'BHANDL_PASSIV': 'varchar(max)', 'BHANDL_TEXT': 'varchar(max)', 'BSTAT_GILTIG_FOM': 'varchar(max)', 'BSTAT_GILTIG_TOM': 'varchar(max)', 'BSTAT_ID': 'varchar(max)', 'BSTAT_ID_TEXT': 'varchar(max)', 'BSTAT_PASSIV': 'varchar(max)', 'BSTAT_TEXT': 'varchar(max)'},
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
		'nksp_rd_sll_se_raindance_udp_udp_100' as _source,
		CONVERT(varchar(max), AVTBES_GILTIG_FOM, 126) AS avtbes_giltig_fom,
		CONVERT(varchar(max), AVTBES_GILTIG_TOM, 126) AS avtbes_giltig_tom,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(AVTBES_ID_TEXT AS VARCHAR(MAX)) AS avtbes_id_text,
		CAST(AVTBES_PASSIV AS VARCHAR(MAX)) AS avtbes_passiv,
		CAST(AVTBES_TEXT AS VARCHAR(MAX)) AS avtbes_text,
		CONVERT(varchar(max), BCHEF_GILTIG_FOM, 126) AS bchef_giltig_fom,
		CONVERT(varchar(max), BCHEF_GILTIG_TOM, 126) AS bchef_giltig_tom,
		CAST(BCHEF_ID AS VARCHAR(MAX)) AS bchef_id,
		CAST(BCHEF_ID_TEXT AS VARCHAR(MAX)) AS bchef_id_text,
		CAST(BCHEF_PASSIV AS VARCHAR(MAX)) AS bchef_passiv,
		CAST(BCHEF_TEXT AS VARCHAR(MAX)) AS bchef_text,
		CONVERT(varchar(max), BGRP_GILTIG_FOM, 126) AS bgrp_giltig_fom,
		CONVERT(varchar(max), BGRP_GILTIG_TOM, 126) AS bgrp_giltig_tom,
		CAST(BGRP_ID AS VARCHAR(MAX)) AS bgrp_id,
		CAST(BGRP_ID_TEXT AS VARCHAR(MAX)) AS bgrp_id_text,
		CAST(BGRP_PASSIV AS VARCHAR(MAX)) AS bgrp_passiv,
		CAST(BGRP_TEXT AS VARCHAR(MAX)) AS bgrp_text,
		CONVERT(varchar(max), BHANDL_GILTIG_FOM, 126) AS bhandl_giltig_fom,
		CONVERT(varchar(max), BHANDL_GILTIG_TOM, 126) AS bhandl_giltig_tom,
		CAST(BHANDL_ID AS VARCHAR(MAX)) AS bhandl_id,
		CAST(BHANDL_ID_TEXT AS VARCHAR(MAX)) AS bhandl_id_text,
		CAST(BHANDL_PASSIV AS VARCHAR(MAX)) AS bhandl_passiv,
		CAST(BHANDL_TEXT AS VARCHAR(MAX)) AS bhandl_text,
		CONVERT(varchar(max), BSTAT_GILTIG_FOM, 126) AS bstat_giltig_fom,
		CONVERT(varchar(max), BSTAT_GILTIG_TOM, 126) AS bstat_giltig_tom,
		CAST(BSTAT_ID AS VARCHAR(MAX)) AS bstat_id,
		CAST(BSTAT_ID_TEXT AS VARCHAR(MAX)) AS bstat_id_text,
		CAST(BSTAT_PASSIV AS VARCHAR(MAX)) AS bstat_passiv,
		CAST(BSTAT_TEXT AS VARCHAR(MAX)) AS bstat_text 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_AVTBES ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    