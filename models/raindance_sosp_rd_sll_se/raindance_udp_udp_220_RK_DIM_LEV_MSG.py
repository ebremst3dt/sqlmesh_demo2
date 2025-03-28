
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BIT_PAF': 'varchar(max)', 'ENVELOPE_TRS': 'varchar(max)', 'FORMATV_RDF': 'varchar(max)', 'FREEVALUE': 'varchar(max)', 'LOGICALVALUE': 'varchar(max)', 'MEDIA_TRS': 'varchar(max)', 'MSGKEY': 'varchar(max)', 'MSGTYPEV': 'varchar(max)', 'MSGWAY': 'varchar(max)', 'PART': 'varchar(max)', 'SBID': 'varchar(max)'},
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
		CAST(BIT_PAF AS VARCHAR(MAX)) AS BIT_PAF,
		CAST(ENVELOPE_TRS AS VARCHAR(MAX)) AS ENVELOPE_TRS,
		CAST(FORMATV_RDF AS VARCHAR(MAX)) AS FORMATV_RDF,
		CAST(FREEVALUE AS VARCHAR(MAX)) AS FREEVALUE,
		CAST(LOGICALVALUE AS VARCHAR(MAX)) AS LOGICALVALUE,
		CAST(MEDIA_TRS AS VARCHAR(MAX)) AS MEDIA_TRS,
		CAST(MSGKEY AS VARCHAR(MAX)) AS MSGKEY,
		CAST(MSGTYPEV AS VARCHAR(MAX)) AS MSGTYPEV,
		CAST(MSGWAY AS VARCHAR(MAX)) AS MSGWAY,
		CAST(PART AS VARCHAR(MAX)) AS PART,
		CAST(SBID AS VARCHAR(MAX)) AS SBID 
	FROM raindance_udp.udp_220.RK_DIM_LEV_MSG ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    