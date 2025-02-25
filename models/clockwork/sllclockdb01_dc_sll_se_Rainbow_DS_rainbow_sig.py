
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'astcod': 'varchar(max)',
 'basunt': 'varchar(max)',
 'budunt': 'varchar(max)',
 'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'cnsunt': 'varchar(max)',
 'compny': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'csccod': 'varchar(max)',
 'dtlunt': 'varchar(max)',
 'fcpcod': 'varchar(max)',
 'ictcod': 'varchar(max)',
 'idgcod': 'varchar(max)',
 'itycod': 'varchar(max)',
 'logcod': 'varchar(max)',
 'migcod': 'varchar(max)',
 'pfmcod': 'varchar(max)',
 'pgrcod': 'varchar(max)',
 'plgcod': 'varchar(max)',
 'pmdcod': 'varchar(max)',
 'pmgcod': 'varchar(max)',
 'pplcod': 'varchar(max)',
 'prbuac': 'varchar(max)',
 'prccod': 'varchar(max)',
 'prcunt': 'varchar(max)',
 'prncod': 'varchar(max)',
 'qamcod': 'varchar(max)',
 'qlmcod': 'varchar(max)',
 'reinit': 'varchar(max)',
 'rrbbud': 'varchar(max)',
 'rrbcns': 'varchar(max)',
 'rrbdtl': 'varchar(max)',
 'rrbprc': 'varchar(max)',
 'rrbsal': 'varchar(max)',
 'rtbbud': 'varchar(max)',
 'rtbcns': 'varchar(max)',
 'rtbdtl': 'varchar(max)',
 'rtbprc': 'varchar(max)',
 'rtbsal': 'varchar(max)',
 'salunt': 'varchar(max)',
 'sapcod': 'varchar(max)',
 'sigcod': 'varchar(max)',
 'signam': 'varchar(max)',
 'srtnam': 'varchar(max)',
 'srtnum': 'varchar(max)',
 'sublvl': 'varchar(max)',
 'txgcod': 'varchar(max)',
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
 		CAST(astcod AS VARCHAR(MAX)) AS astcod,
		CAST(basunt AS VARCHAR(MAX)) AS basunt,
		CAST(budunt AS VARCHAR(MAX)) AS budunt,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(cnsunt AS VARCHAR(MAX)) AS cnsunt,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(csccod AS VARCHAR(MAX)) AS csccod,
		CAST(dtlunt AS VARCHAR(MAX)) AS dtlunt,
		CAST(fcpcod AS VARCHAR(MAX)) AS fcpcod,
		CAST(ictcod AS VARCHAR(MAX)) AS ictcod,
		CAST(idgcod AS VARCHAR(MAX)) AS idgcod,
		CAST(itycod AS VARCHAR(MAX)) AS itycod,
		CAST(logcod AS VARCHAR(MAX)) AS logcod,
		CAST(migcod AS VARCHAR(MAX)) AS migcod,
		CAST(pfmcod AS VARCHAR(MAX)) AS pfmcod,
		CAST(pgrcod AS VARCHAR(MAX)) AS pgrcod,
		CAST(plgcod AS VARCHAR(MAX)) AS plgcod,
		CAST(pmdcod AS VARCHAR(MAX)) AS pmdcod,
		CAST(pmgcod AS VARCHAR(MAX)) AS pmgcod,
		CAST(pplcod AS VARCHAR(MAX)) AS pplcod,
		CAST(prbuac AS VARCHAR(MAX)) AS prbuac,
		CAST(prccod AS VARCHAR(MAX)) AS prccod,
		CAST(prcunt AS VARCHAR(MAX)) AS prcunt,
		CAST(prncod AS VARCHAR(MAX)) AS prncod,
		CAST(qamcod AS VARCHAR(MAX)) AS qamcod,
		CAST(qlmcod AS VARCHAR(MAX)) AS qlmcod,
		CAST(reinit AS VARCHAR(MAX)) AS reinit,
		CAST(rrbbud AS VARCHAR(MAX)) AS rrbbud,
		CAST(rrbcns AS VARCHAR(MAX)) AS rrbcns,
		CAST(rrbdtl AS VARCHAR(MAX)) AS rrbdtl,
		CAST(rrbprc AS VARCHAR(MAX)) AS rrbprc,
		CAST(rrbsal AS VARCHAR(MAX)) AS rrbsal,
		CAST(rtbbud AS VARCHAR(MAX)) AS rtbbud,
		CAST(rtbcns AS VARCHAR(MAX)) AS rtbcns,
		CAST(rtbdtl AS VARCHAR(MAX)) AS rtbdtl,
		CAST(rtbprc AS VARCHAR(MAX)) AS rtbprc,
		CAST(rtbsal AS VARCHAR(MAX)) AS rtbsal,
		CAST(salunt AS VARCHAR(MAX)) AS salunt,
		CAST(sapcod AS VARCHAR(MAX)) AS sapcod,
		CAST(sigcod AS VARCHAR(MAX)) AS sigcod,
		CAST(signam AS VARCHAR(MAX)) AS signam,
		CAST(srtnam AS VARCHAR(MAX)) AS srtnam,
		CAST(srtnum AS VARCHAR(MAX)) AS srtnum,
		CAST(sublvl AS VARCHAR(MAX)) AS sublvl,
		CAST(txgcod AS VARCHAR(MAX)) AS txgcod,
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
		'Rainbow_DS' as source_catalog 
	FROM Rainbow_DS.rainbow.sig
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
        