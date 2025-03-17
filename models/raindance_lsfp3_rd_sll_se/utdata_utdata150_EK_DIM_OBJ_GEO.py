
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'GEO_GILTIG_FOM': 'varchar(max)', 'GEO_GILTIG_TOM': 'varchar(max)', 'GEO_ID': 'varchar(max)', 'GEO_ID_TEXT': 'varchar(max)', 'GEO_PASSIV': 'varchar(max)', 'GEO_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata150' as _source,
		CONVERT(varchar(max), GEO_GILTIG_FOM, 126) AS geo_giltig_fom,
		CONVERT(varchar(max), GEO_GILTIG_TOM, 126) AS geo_giltig_tom,
		CAST(GEO_ID AS VARCHAR(MAX)) AS geo_id,
		CAST(GEO_ID_TEXT AS VARCHAR(MAX)) AS geo_id_text,
		CAST(GEO_PASSIV AS VARCHAR(MAX)) AS geo_passiv,
		CAST(GEO_TEXT AS VARCHAR(MAX)) AS geo_text 
	FROM utdata.utdata150.EK_DIM_OBJ_GEO ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    