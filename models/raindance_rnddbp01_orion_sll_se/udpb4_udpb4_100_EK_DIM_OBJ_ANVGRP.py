
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANVGRP_GILTIG_FOM': 'varchar(max)', 'ANVGRP_GILTIG_TOM': 'varchar(max)', 'ANVGRP_ID': 'varchar(max)', 'ANVGRP_ID_TEXT': 'varchar(max)', 'ANVGRP_PASSIV': 'varchar(max)', 'ANVGRP_TEXT': 'varchar(max)', 'FAKTCE_GILTIG_FOM': 'varchar(max)', 'FAKTCE_GILTIG_TOM': 'varchar(max)', 'FAKTCE_ID': 'varchar(max)', 'FAKTCE_ID_TEXT': 'varchar(max)', 'FAKTCE_PASSIV': 'varchar(max)', 'FAKTCE_TEXT': 'varchar(max)'},
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
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CONVERT(varchar(max), ANVGRP_GILTIG_FOM, 126) AS ANVGRP_GILTIG_FOM,
		CONVERT(varchar(max), ANVGRP_GILTIG_TOM, 126) AS ANVGRP_GILTIG_TOM,
		CAST(ANVGRP_ID AS VARCHAR(MAX)) AS ANVGRP_ID,
		CAST(ANVGRP_ID_TEXT AS VARCHAR(MAX)) AS ANVGRP_ID_TEXT,
		CAST(ANVGRP_PASSIV AS VARCHAR(MAX)) AS ANVGRP_PASSIV,
		CAST(ANVGRP_TEXT AS VARCHAR(MAX)) AS ANVGRP_TEXT,
		CONVERT(varchar(max), FAKTCE_GILTIG_FOM, 126) AS FAKTCE_GILTIG_FOM,
		CONVERT(varchar(max), FAKTCE_GILTIG_TOM, 126) AS FAKTCE_GILTIG_TOM,
		CAST(FAKTCE_ID AS VARCHAR(MAX)) AS FAKTCE_ID,
		CAST(FAKTCE_ID_TEXT AS VARCHAR(MAX)) AS FAKTCE_ID_TEXT,
		CAST(FAKTCE_PASSIV AS VARCHAR(MAX)) AS FAKTCE_PASSIV,
		CAST(FAKTCE_TEXT AS VARCHAR(MAX)) AS FAKTCE_TEXT 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_ANVGRP ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    