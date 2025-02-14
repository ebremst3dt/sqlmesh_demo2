
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'compny': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'digcod': 'varchar(max)',
 'dignam': 'varchar(max)',
 'migcod': 'varchar(max)',
 'sigcod': 'varchar(max)',
 'srtnam': 'varchar(max)',
 'srtnum': 'varchar(max)',
 'txtdsc': 'varchar(max)'},
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
 		CAST(chgdat AS VARCHAR(MAX)) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(credat AS VARCHAR(MAX)) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(digcod AS VARCHAR(MAX)) AS digcod,
		CAST(dignam AS VARCHAR(MAX)) AS dignam,
		CAST(migcod AS VARCHAR(MAX)) AS migcod,
		CAST(sigcod AS VARCHAR(MAX)) AS sigcod,
		CAST(srtnam AS VARCHAR(MAX)) AS srtnam,
		CAST(srtnum AS VARCHAR(MAX)) AS srtnum,
		CAST(txtdsc AS VARCHAR(MAX)) AS txtdsc 
	FROM Rainbow_MD.rainbow.dig
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
