
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'VALUTA_GILTIG_FOM': 'varchar(max)', 'VALUTA_GILTIG_TOM': 'varchar(max)', 'VALUTA_ID': 'varchar(max)', 'VALUTA_ID_TEXT': 'varchar(max)', 'VALUTA_PASSIV': 'varchar(max)', 'VALUTA_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), VALUTA_GILTIG_FOM, 126) AS valuta_giltig_fom,
		CONVERT(varchar(max), VALUTA_GILTIG_TOM, 126) AS valuta_giltig_tom,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS valuta_id,
		CAST(VALUTA_ID_TEXT AS VARCHAR(MAX)) AS valuta_id_text,
		CAST(VALUTA_PASSIV AS VARCHAR(MAX)) AS valuta_passiv,
		CAST(VALUTA_TEXT AS VARCHAR(MAX)) AS valuta_text 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_VALUTA ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    