
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANVGRP_GILTIG_FOM': 'varchar(max)', 'ANVGRP_GILTIG_TOM': 'varchar(max)', 'ANVGRP_ID': 'varchar(max)', 'ANVGRP_ID_TEXT': 'varchar(max)', 'ANVGRP_PASSIV': 'varchar(max)', 'ANVGRP_TEXT': 'varchar(max)', 'ANVID_GILTIG_FOM': 'varchar(max)', 'ANVID_GILTIG_TOM': 'varchar(max)', 'ANVID_ID': 'varchar(max)', 'ANVID_ID_TEXT': 'varchar(max)', 'ANVID_PASSIV': 'varchar(max)', 'ANVID_TEXT': 'varchar(max)', 'FAKTCE_GILTIG_FOM': 'varchar(max)', 'FAKTCE_GILTIG_TOM': 'varchar(max)', 'FAKTCE_ID': 'varchar(max)', 'FAKTCE_ID_TEXT': 'varchar(max)', 'FAKTCE_PASSIV': 'varchar(max)', 'FAKTCE_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), ANVGRP_GILTIG_FOM, 126) AS anvgrp_giltig_fom,
		CONVERT(varchar(max), ANVGRP_GILTIG_TOM, 126) AS anvgrp_giltig_tom,
		CAST(ANVGRP_ID AS VARCHAR(MAX)) AS anvgrp_id,
		CAST(ANVGRP_ID_TEXT AS VARCHAR(MAX)) AS anvgrp_id_text,
		CAST(ANVGRP_PASSIV AS VARCHAR(MAX)) AS anvgrp_passiv,
		CAST(ANVGRP_TEXT AS VARCHAR(MAX)) AS anvgrp_text,
		CONVERT(varchar(max), ANVID_GILTIG_FOM, 126) AS anvid_giltig_fom,
		CONVERT(varchar(max), ANVID_GILTIG_TOM, 126) AS anvid_giltig_tom,
		CAST(ANVID_ID AS VARCHAR(MAX)) AS anvid_id,
		CAST(ANVID_ID_TEXT AS VARCHAR(MAX)) AS anvid_id_text,
		CAST(ANVID_PASSIV AS VARCHAR(MAX)) AS anvid_passiv,
		CAST(ANVID_TEXT AS VARCHAR(MAX)) AS anvid_text,
		CONVERT(varchar(max), FAKTCE_GILTIG_FOM, 126) AS faktce_giltig_fom,
		CONVERT(varchar(max), FAKTCE_GILTIG_TOM, 126) AS faktce_giltig_tom,
		CAST(FAKTCE_ID AS VARCHAR(MAX)) AS faktce_id,
		CAST(FAKTCE_ID_TEXT AS VARCHAR(MAX)) AS faktce_id_text,
		CAST(FAKTCE_PASSIV AS VARCHAR(MAX)) AS faktce_passiv,
		CAST(FAKTCE_TEXT AS VARCHAR(MAX)) AS faktce_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_ANVID ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    