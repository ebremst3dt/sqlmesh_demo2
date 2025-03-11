
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'ARTKR_DATUM_FOM': 'varchar(max)', 'ARTKR_DATUM_TOM': 'varchar(max)', 'ARTKR_GILTIG_FOM': 'varchar(max)', 'ARTKR_GILTIG_TOM': 'varchar(max)', 'ARTKR_ID': 'varchar(max)', 'ARTKR_ID_TEXT': 'varchar(max)', 'ARTKR_PASSIV': 'varchar(max)', 'ARTKR_TEXT': 'varchar(max)'},
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
    query = """
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), ARTKR_DATUM_FOM, 126) AS artkr_datum_fom,
		CONVERT(varchar(max), ARTKR_DATUM_TOM, 126) AS artkr_datum_tom,
		CONVERT(varchar(max), ARTKR_GILTIG_FOM, 126) AS artkr_giltig_fom,
		CONVERT(varchar(max), ARTKR_GILTIG_TOM, 126) AS artkr_giltig_tom,
		CAST(ARTKR_ID AS VARCHAR(MAX)) AS artkr_id,
		CAST(ARTKR_ID_TEXT AS VARCHAR(MAX)) AS artkr_id_text,
		CAST(ARTKR_PASSIV AS VARCHAR(MAX)) AS artkr_passiv,
		CAST(ARTKR_TEXT AS VARCHAR(MAX)) AS artkr_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_ARTKR

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    