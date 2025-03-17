
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ERSGRP_GILTIG_FOM': 'varchar(max)', 'ERSGRP_GILTIG_TOM': 'varchar(max)', 'ERSGRP_ID': 'varchar(max)', 'ERSGRP_ID_TEXT': 'varchar(max)', 'ERSGRP_PASSIV': 'varchar(max)', 'ERSGRP_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata361' as _source,
		CONVERT(varchar(max), ERSGRP_GILTIG_FOM, 126) AS ersgrp_giltig_fom,
		CONVERT(varchar(max), ERSGRP_GILTIG_TOM, 126) AS ersgrp_giltig_tom,
		CAST(ERSGRP_ID AS VARCHAR(MAX)) AS ersgrp_id,
		CAST(ERSGRP_ID_TEXT AS VARCHAR(MAX)) AS ersgrp_id_text,
		CAST(ERSGRP_PASSIV AS VARCHAR(MAX)) AS ersgrp_passiv,
		CAST(ERSGRP_TEXT AS VARCHAR(MAX)) AS ersgrp_text 
	FROM utdata.utdata361.EK_DIM_OBJ_ERSGRP ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    