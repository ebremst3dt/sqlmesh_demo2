
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
		CONVERT(varchar(max), AVTBES_GILTIG_FOM, 126) AS AVTBES_GILTIG_FOM,
		CONVERT(varchar(max), AVTBES_GILTIG_TOM, 126) AS AVTBES_GILTIG_TOM,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS AVTBES_ID,
		CAST(AVTBES_ID_TEXT AS VARCHAR(MAX)) AS AVTBES_ID_TEXT,
		CAST(AVTBES_PASSIV AS VARCHAR(MAX)) AS AVTBES_PASSIV,
		CAST(AVTBES_TEXT AS VARCHAR(MAX)) AS AVTBES_TEXT,
		CONVERT(varchar(max), BCHEF_GILTIG_FOM, 126) AS BCHEF_GILTIG_FOM,
		CONVERT(varchar(max), BCHEF_GILTIG_TOM, 126) AS BCHEF_GILTIG_TOM,
		CAST(BCHEF_ID AS VARCHAR(MAX)) AS BCHEF_ID,
		CAST(BCHEF_ID_TEXT AS VARCHAR(MAX)) AS BCHEF_ID_TEXT,
		CAST(BCHEF_PASSIV AS VARCHAR(MAX)) AS BCHEF_PASSIV,
		CAST(BCHEF_TEXT AS VARCHAR(MAX)) AS BCHEF_TEXT,
		CONVERT(varchar(max), BGRP_GILTIG_FOM, 126) AS BGRP_GILTIG_FOM,
		CONVERT(varchar(max), BGRP_GILTIG_TOM, 126) AS BGRP_GILTIG_TOM,
		CAST(BGRP_ID AS VARCHAR(MAX)) AS BGRP_ID,
		CAST(BGRP_ID_TEXT AS VARCHAR(MAX)) AS BGRP_ID_TEXT,
		CAST(BGRP_PASSIV AS VARCHAR(MAX)) AS BGRP_PASSIV,
		CAST(BGRP_TEXT AS VARCHAR(MAX)) AS BGRP_TEXT,
		CONVERT(varchar(max), BHANDL_GILTIG_FOM, 126) AS BHANDL_GILTIG_FOM,
		CONVERT(varchar(max), BHANDL_GILTIG_TOM, 126) AS BHANDL_GILTIG_TOM,
		CAST(BHANDL_ID AS VARCHAR(MAX)) AS BHANDL_ID,
		CAST(BHANDL_ID_TEXT AS VARCHAR(MAX)) AS BHANDL_ID_TEXT,
		CAST(BHANDL_PASSIV AS VARCHAR(MAX)) AS BHANDL_PASSIV,
		CAST(BHANDL_TEXT AS VARCHAR(MAX)) AS BHANDL_TEXT,
		CONVERT(varchar(max), BSTAT_GILTIG_FOM, 126) AS BSTAT_GILTIG_FOM,
		CONVERT(varchar(max), BSTAT_GILTIG_TOM, 126) AS BSTAT_GILTIG_TOM,
		CAST(BSTAT_ID AS VARCHAR(MAX)) AS BSTAT_ID,
		CAST(BSTAT_ID_TEXT AS VARCHAR(MAX)) AS BSTAT_ID_TEXT,
		CAST(BSTAT_PASSIV AS VARCHAR(MAX)) AS BSTAT_PASSIV,
		CAST(BSTAT_TEXT AS VARCHAR(MAX)) AS BSTAT_TEXT 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_AVTBES ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    