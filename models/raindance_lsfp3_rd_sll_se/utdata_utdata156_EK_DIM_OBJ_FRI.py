
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRGR_GILTIG_FOM': 'varchar(max)', 'FRGR_GILTIG_TOM': 'varchar(max)', 'FRGR_ID': 'varchar(max)', 'FRGR_ID_TEXT': 'varchar(max)', 'FRGR_PASSIV': 'varchar(max)', 'FRGR_TEXT': 'varchar(max)', 'FRI_GILTIG_FOM': 'varchar(max)', 'FRI_GILTIG_TOM': 'varchar(max)', 'FRI_ID': 'varchar(max)', 'FRI_ID_TEXT': 'varchar(max)', 'FRI_PASSIV': 'varchar(max)', 'FRI_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata156' as _source,
		CONVERT(varchar(max), FRGR_GILTIG_FOM, 126) AS frgr_giltig_fom,
		CONVERT(varchar(max), FRGR_GILTIG_TOM, 126) AS frgr_giltig_tom,
		CAST(FRGR_ID AS VARCHAR(MAX)) AS frgr_id,
		CAST(FRGR_ID_TEXT AS VARCHAR(MAX)) AS frgr_id_text,
		CAST(FRGR_PASSIV AS VARCHAR(MAX)) AS frgr_passiv,
		CAST(FRGR_TEXT AS VARCHAR(MAX)) AS frgr_text,
		CONVERT(varchar(max), FRI_GILTIG_FOM, 126) AS fri_giltig_fom,
		CONVERT(varchar(max), FRI_GILTIG_TOM, 126) AS fri_giltig_tom,
		CAST(FRI_ID AS VARCHAR(MAX)) AS fri_id,
		CAST(FRI_ID_TEXT AS VARCHAR(MAX)) AS fri_id_text,
		CAST(FRI_PASSIV AS VARCHAR(MAX)) AS fri_passiv,
		CAST(FRI_TEXT AS VARCHAR(MAX)) AS fri_text 
	FROM utdata.utdata156.EK_DIM_OBJ_FRI ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    