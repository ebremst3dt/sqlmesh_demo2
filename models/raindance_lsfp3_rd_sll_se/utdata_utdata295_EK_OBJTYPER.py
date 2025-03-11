
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'EJITABELL_ID': 'varchar(max)', 'EJITABELL_NR': 'varchar(max)', 'INVIA_ID': 'varchar(max)', 'INVIA_NR': 'varchar(max)', 'OBJTYPLANGD': 'varchar(max)', 'OBJTYP_ID': 'varchar(max)', 'OBJTYP_ID_TEXT': 'varchar(max)', 'OBJTYP_NR': 'varchar(max)', 'OBJTYP_TEXT': 'varchar(max)'},
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
		CAST(EJITABELL_ID AS VARCHAR(MAX)) AS ejitabell_id,
		CAST(EJITABELL_NR AS VARCHAR(MAX)) AS ejitabell_nr,
		CAST(INVIA_ID AS VARCHAR(MAX)) AS invia_id,
		CAST(INVIA_NR AS VARCHAR(MAX)) AS invia_nr,
		CAST(OBJTYPLANGD AS VARCHAR(MAX)) AS objtyplangd,
		CAST(OBJTYP_ID AS VARCHAR(MAX)) AS objtyp_id,
		CAST(OBJTYP_ID_TEXT AS VARCHAR(MAX)) AS objtyp_id_text,
		CAST(OBJTYP_NR AS VARCHAR(MAX)) AS objtyp_nr,
		CAST(OBJTYP_TEXT AS VARCHAR(MAX)) AS objtyp_text 
	FROM utdata.utdata295.EK_OBJTYPER

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    