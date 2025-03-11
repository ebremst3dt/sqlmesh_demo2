
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRI1_DATUM_FOM': 'varchar(max)', 'FRI1_DATUM_TOM': 'varchar(max)', 'FRI1_GILTIG_FOM': 'varchar(max)', 'FRI1_GILTIG_TOM': 'varchar(max)', 'FRI1_ID': 'varchar(max)', 'FRI1_ID_TEXT': 'varchar(max)', 'FRI1_PASSIV': 'varchar(max)', 'FRI1_TEXT': 'varchar(max)'},
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
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), FRI1_DATUM_FOM, 126) AS fri1_datum_fom,
		CONVERT(varchar(max), FRI1_DATUM_TOM, 126) AS fri1_datum_tom,
		CONVERT(varchar(max), FRI1_GILTIG_FOM, 126) AS fri1_giltig_fom,
		CONVERT(varchar(max), FRI1_GILTIG_TOM, 126) AS fri1_giltig_tom,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI1_ID_TEXT AS VARCHAR(MAX)) AS fri1_id_text,
		CAST(FRI1_PASSIV AS VARCHAR(MAX)) AS fri1_passiv,
		CAST(FRI1_TEXT AS VARCHAR(MAX)) AS fri1_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_FRI1) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    