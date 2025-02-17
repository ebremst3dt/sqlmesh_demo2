
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BIT_PAF': 'numeric',
 'ENVELOPE_TRS': 'varchar(10)',
 'FORMATV_RDF': 'varchar(15)',
 'FREEVALUE': 'varchar(75)',
 'LOGICALVALUE': 'varchar(20)',
 'MEDIA_TRS': 'varchar(10)',
 'MSGKEY': 'numeric',
 'MSGTYPEV': 'varchar(15)',
 'MSGWAY': 'numeric',
 'PART': 'numeric',
 'SBID': 'varchar(16)'},
    kind=ModelKindName.FULL,
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
	FROM utdata.utdata295.RK_DIM_LEV_MSG
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
