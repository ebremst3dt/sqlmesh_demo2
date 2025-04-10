
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.clockwork import start

    
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'compny': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'digcod': 'varchar(max)', 'gencom': 'varchar(max)', 'hidsrc': 'varchar(max)', 'icscat': 'varchar(max)', 'icscod': 'varchar(max)', 'icsmap': 'varchar(max)', 'icsnam': 'varchar(max)', 'icsref': 'varchar(max)', 'ikscat': 'varchar(max)', 'iksref': 'varchar(max)', 'lvlcod': 'varchar(max)', 'migcod': 'varchar(max)', 'ofmcod': 'varchar(max)', 'parseq': 'varchar(max)', 'prbuac': 'varchar(max)', 'seqnum': 'varchar(max)', 'sigcod': 'varchar(max)', 'txtdsc': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['compny', 'icscat', 'seqnum']
    ),
    start=start,
    cron="0 2 * * *",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_ks_rainbow_icscat_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_ks_rainbow_icscat (_data_modified_utc)"]
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
 		CAST(
        CAST(
            COALESCE(
                CASE
                    WHEN credat > chgdat or chgdat IS NULL then credat
                    WHEN chgdat > credat or credat is NULL then chgdat
                    ELSE credat
                END,
                chgdat,
                credat
            ) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC'
        AS datetime2
    ) AS DATE ) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'Rainbow_KS' as _source_catalog,
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
		CAST(txtdsc AS VARCHAR(MAX)) AS txtdsc 
	FROM Rainbow_KS.rainbow.icscat
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    