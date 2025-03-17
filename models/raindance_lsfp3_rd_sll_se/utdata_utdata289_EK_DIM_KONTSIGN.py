
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KONTSIGN': 'varchar(max)', 'KONTSIGN2': 'varchar(max)', 'KONTSIGN2_ID_TEXT': 'varchar(max)', 'KONTSIGN_ID_TEXT': 'varchar(max)', 'KONTSIGN_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata289' as _source,
		CAST(KONTSIGN AS VARCHAR(MAX)) AS kontsign,
		CAST(KONTSIGN2 AS VARCHAR(MAX)) AS kontsign2,
		CAST(KONTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS kontsign2_id_text,
		CAST(KONTSIGN_ID_TEXT AS VARCHAR(MAX)) AS kontsign_id_text,
		CAST(KONTSIGN_TEXT AS VARCHAR(MAX)) AS kontsign_text 
	FROM utdata.utdata289.EK_DIM_KONTSIGN ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    