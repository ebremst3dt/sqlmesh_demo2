
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'YRKG_GILTIG_FOM': 'varchar(max)', 'YRKG_GILTIG_TOM': 'varchar(max)', 'YRKG_ID': 'varchar(max)', 'YRKG_ID_TEXT': 'varchar(max)', 'YRKG_PASSIV': 'varchar(max)', 'YRKG_TEXT': 'varchar(max)'},
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
		'sosp_rd_sll_se_raindance_udp_udp_220' as _source,
		CONVERT(varchar(max), YRKG_GILTIG_FOM, 126) AS YRKG_GILTIG_FOM,
		CONVERT(varchar(max), YRKG_GILTIG_TOM, 126) AS YRKG_GILTIG_TOM,
		CAST(YRKG_ID AS VARCHAR(MAX)) AS YRKG_ID,
		CAST(YRKG_ID_TEXT AS VARCHAR(MAX)) AS YRKG_ID_TEXT,
		CAST(YRKG_PASSIV AS VARCHAR(MAX)) AS YRKG_PASSIV,
		CAST(YRKG_TEXT AS VARCHAR(MAX)) AS YRKG_TEXT 
	FROM raindance_udp.udp_220.EK_DIM_OBJ_YRKG ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    