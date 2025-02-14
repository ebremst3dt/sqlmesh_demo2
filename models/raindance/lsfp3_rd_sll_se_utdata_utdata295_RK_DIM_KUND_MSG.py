
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'BIT_PAF': 'varchar(max)',
 'ENVELOPE_TRS': 'varchar(max)',
 'FORMATV_RDF': 'varchar(max)',
 'FREEVALUE': 'varchar(max)',
 'LOGICALVALUE': 'varchar(max)',
 'MEDIA_TRS': 'varchar(max)',
 'MSGKEY': 'varchar(max)',
 'MSGTYPEV': 'varchar(max)',
 'MSGWAY': 'varchar(max)',
 'PART': 'varchar(max)',
 'SBID': 'varchar(max)'},
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
	SELECT top 1000
 		CAST(BIT_PAF AS VARCHAR(MAX)) AS bit_paf,
		CAST(ENVELOPE_TRS AS VARCHAR(MAX)) AS envelope_trs,
		CAST(FORMATV_RDF AS VARCHAR(MAX)) AS formatv_rdf,
		CAST(FREEVALUE AS VARCHAR(MAX)) AS freevalue,
		CAST(LOGICALVALUE AS VARCHAR(MAX)) AS logicalvalue,
		CAST(MEDIA_TRS AS VARCHAR(MAX)) AS media_trs,
		CAST(MSGKEY AS VARCHAR(MAX)) AS msgkey,
		CAST(MSGTYPEV AS VARCHAR(MAX)) AS msgtypev,
		CAST(MSGWAY AS VARCHAR(MAX)) AS msgway,
		CAST(PART AS VARCHAR(MAX)) AS part,
		CAST(SBID AS VARCHAR(MAX)) AS sbid 
	FROM utdata.utdata295.RK_DIM_KUND_MSG
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
