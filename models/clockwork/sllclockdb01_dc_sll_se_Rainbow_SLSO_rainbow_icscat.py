
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'compny': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'data_modified': 'date',
 'digcod': 'varchar(max)',
 'gencom': 'varchar(max)',
 'hidsrc': 'varchar(max)',
 'icscat': 'varchar(max)',
 'icscod': 'varchar(max)',
 'icsmap': 'varchar(max)',
 'icsnam': 'varchar(max)',
 'icsref': 'varchar(max)',
 'ikscat': 'varchar(max)',
 'iksref': 'varchar(max)',
 'lvlcod': 'varchar(max)',
 'migcod': 'varchar(max)',
 'ofmcod': 'varchar(max)',
 'parseq': 'varchar(max)',
 'prbuac': 'varchar(max)',
 'seqnum': 'varchar(max)',
 'sigcod': 'varchar(max)',
 'source_catalog': 'varchar(max)',
 'txtdsc': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="data_modified"
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
	SELECT 
 		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(digcod AS VARCHAR(MAX)) AS digcod,
		CAST(gencom AS VARCHAR(MAX)) AS gencom,
		CAST(hidsrc AS VARCHAR(MAX)) AS hidsrc,
		CAST(icscat AS VARCHAR(MAX)) AS icscat,
		CAST(icscod AS VARCHAR(MAX)) AS icscod,
		CAST(icsmap AS VARCHAR(MAX)) AS icsmap,
		CAST(icsnam AS VARCHAR(MAX)) AS icsnam,
		CAST(icsref AS VARCHAR(MAX)) AS icsref,
		CAST(ikscat AS VARCHAR(MAX)) AS ikscat,
		CAST(iksref AS VARCHAR(MAX)) AS iksref,
		CAST(lvlcod AS VARCHAR(MAX)) AS lvlcod,
		CAST(migcod AS VARCHAR(MAX)) AS migcod,
		CAST(ofmcod AS VARCHAR(MAX)) AS ofmcod,
		CAST(parseq AS VARCHAR(MAX)) AS parseq,
		CAST(prbuac AS VARCHAR(MAX)) AS prbuac,
		CAST(seqnum AS VARCHAR(MAX)) AS seqnum,
		CAST(sigcod AS VARCHAR(MAX)) AS sigcod,
		CAST(txtdsc AS VARCHAR(MAX)) AS txtdsc,
		CAST(
                COALESCE(
                    CASE
                        WHEN credat > chgdat OR chgdat IS NULL THEN credat
                        WHEN chgdat > credat OR credat IS NULL THEN chgdat
                        ELSE credat
                    END,
                    chgdat,
                    credat
                ) AS DATE
            ) AS data_modified,
		'Rainbow_SLSO' as source_catalog 
	FROM Rainbow_SLSO.rainbow.icscat
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
        