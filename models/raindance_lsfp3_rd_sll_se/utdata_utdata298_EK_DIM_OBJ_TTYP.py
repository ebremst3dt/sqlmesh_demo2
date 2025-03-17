
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'TTYP_GILTIG_FOM': 'varchar(max)', 'TTYP_GILTIG_TOM': 'varchar(max)', 'TTYP_ID': 'varchar(max)', 'TTYP_ID_TEXT': 'varchar(max)', 'TTYP_PASSIV': 'varchar(max)', 'TTYP_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CONVERT(varchar(max), TTYP_GILTIG_FOM, 126) AS ttyp_giltig_fom,
		CONVERT(varchar(max), TTYP_GILTIG_TOM, 126) AS ttyp_giltig_tom,
		CAST(TTYP_ID AS VARCHAR(MAX)) AS ttyp_id,
		CAST(TTYP_ID_TEXT AS VARCHAR(MAX)) AS ttyp_id_text,
		CAST(TTYP_PASSIV AS VARCHAR(MAX)) AS ttyp_passiv,
		CAST(TTYP_TEXT AS VARCHAR(MAX)) AS ttyp_text 
	FROM utdata.utdata298.EK_DIM_OBJ_TTYP ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    