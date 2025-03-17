
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DETALJTYP': 'varchar(max)', 'DETALJTYP_ID': 'varchar(max)', 'DETALJTYP_ID_TEXT': 'varchar(max)', 'DETALJTYP_NR': 'varchar(max)', 'DETALJTYP_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata293' as _source,
		CAST(DETALJTYP AS VARCHAR(MAX)) AS detaljtyp,
		CAST(DETALJTYP_ID AS VARCHAR(MAX)) AS detaljtyp_id,
		CAST(DETALJTYP_ID_TEXT AS VARCHAR(MAX)) AS detaljtyp_id_text,
		CAST(DETALJTYP_NR AS VARCHAR(MAX)) AS detaljtyp_nr,
		CAST(DETALJTYP_TEXT AS VARCHAR(MAX)) AS detaljtyp_text 
	FROM utdata.utdata293.RK_DIM_DETALJTYP ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    