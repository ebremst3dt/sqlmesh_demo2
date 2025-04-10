
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.clockwork import start

    
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'agrnum': 'varchar(max)', 'appdat': 'varchar(max)', 'appsts': 'varchar(max)', 'appusr': 'varchar(max)', 'balseq': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'cmpdat': 'varchar(max)', 'cmpsts': 'varchar(max)', 'cmpusr': 'varchar(max)', 'cnvexc': 'varchar(max)', 'compny': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'crtcod': 'varchar(max)', 'curcod': 'varchar(max)', 'dfiseq': 'varchar(max)', 'dlgdat': 'varchar(max)', 'dlgidc': 'varchar(max)', 'dlgidt': 'varchar(max)', 'dlgsts': 'varchar(max)', 'dlvdat': 'varchar(max)', 'dlvusr': 'varchar(max)', 'dspcod': 'varchar(max)', 'dspdat': 'varchar(max)', 'dspnam': 'varchar(max)', 'dsptyp': 'varchar(max)', 'dstper': 'varchar(max)', 'enttyp': 'varchar(max)', 'excrat': 'varchar(max)', 'extitm': 'varchar(max)', 'extnam': 'varchar(max)', 'extseq': 'varchar(max)', 'faiseq': 'varchar(max)', 'fivdat': 'varchar(max)', 'fivsts': 'varchar(max)', 'fivusr': 'varchar(max)', 'hidprc': 'varchar(max)', 'hndseq': 'varchar(max)', 'icscat': 'varchar(max)', 'icsref': 'varchar(max)', 'idncod': 'varchar(max)', 'infsts': 'varchar(max)', 'lincod': 'varchar(max)', 'linnam': 'varchar(max)', 'lintyp': 'varchar(max)', 'maploc': 'varchar(max)', 'mrqalt': 'varchar(max)', 'mrqnum': 'varchar(max)', 'mstseq': 'varchar(max)', 'ofmcod': 'varchar(max)', 'ordbk1': 'varchar(max)', 'orddat': 'varchar(max)', 'oreval': 'varchar(max)', 'otscod': 'varchar(max)', 'prccst': 'varchar(max)', 'rascat': 'varchar(max)', 'rasref': 'varchar(max)', 'ratsts': 'varchar(max)', 'rcvcod': 'varchar(max)', 'rcvdat': 'varchar(max)', 'rcvnam': 'varchar(max)', 'rcvtyp': 'varchar(max)', 'recusr': 'varchar(max)', 'reqdat': 'varchar(max)', 'reqqty': 'varchar(max)', 'reqrra': 'varchar(max)', 'reqrty': 'varchar(max)', 'reqsts': 'varchar(max)', 'requnt': 'varchar(max)', 'rfqsts': 'varchar(max)', 'rplref': 'varchar(max)', 'rsptyp': 'varchar(max)', 'rtndat': 'varchar(max)', 'seqnum': 'varchar(max)', 'splmst': 'varchar(max)', 'srctyp': 'varchar(max)', 'tadref': 'varchar(max)', 'tndnum': 'varchar(max)', 'tndseq': 'varchar(max)', 'txtext': 'varchar(max)', 'txtint': 'varchar(max)', 'valcur': 'varchar(max)', 'valloc': 'varchar(max)', 'vatcod': 'varchar(max)', 'vrfsts': 'varchar(max)', 'vrscod': 'varchar(max)', 'wwfseq': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['compny', 'mrqnum', 'seqnum']
    ),
    start=start,
    cron="0 2 * * *",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_md_rainbow_mrqlin_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_md_rainbow_mrqlin (_data_modified_utc)"]
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
		'Rainbow_MD' as _source_catalog,
		CAST(agrnum AS VARCHAR(MAX)) AS agrnum,
		CONVERT(varchar(max), appdat, 126) AS appdat,
		CAST(appsts AS VARCHAR(MAX)) AS appsts,
		CAST(appusr AS VARCHAR(MAX)) AS appusr,
		CAST(balseq AS VARCHAR(MAX)) AS balseq,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CONVERT(varchar(max), cmpdat, 126) AS cmpdat,
		CAST(cmpsts AS VARCHAR(MAX)) AS cmpsts,
		CAST(cmpusr AS VARCHAR(MAX)) AS cmpusr,
		CAST(cnvexc AS VARCHAR(MAX)) AS cnvexc,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(crtcod AS VARCHAR(MAX)) AS crtcod,
		CAST(curcod AS VARCHAR(MAX)) AS curcod,
		CAST(dfiseq AS VARCHAR(MAX)) AS dfiseq,
		CONVERT(varchar(max), dlgdat, 126) AS dlgdat,
		CAST(dlgidc AS VARCHAR(MAX)) AS dlgidc,
		CAST(dlgidt AS VARCHAR(MAX)) AS dlgidt,
		CAST(dlgsts AS VARCHAR(MAX)) AS dlgsts,
		CONVERT(varchar(max), dlvdat, 126) AS dlvdat,
		CAST(dlvusr AS VARCHAR(MAX)) AS dlvusr,
		CAST(dspcod AS VARCHAR(MAX)) AS dspcod,
		CONVERT(varchar(max), dspdat, 126) AS dspdat,
		CAST(dspnam AS VARCHAR(MAX)) AS dspnam,
		CAST(dsptyp AS VARCHAR(MAX)) AS dsptyp,
		CAST(dstper AS VARCHAR(MAX)) AS dstper,
		CAST(enttyp AS VARCHAR(MAX)) AS enttyp,
		CAST(excrat AS VARCHAR(MAX)) AS excrat,
		CAST(extitm AS VARCHAR(MAX)) AS extitm,
		CAST(extnam AS VARCHAR(MAX)) AS extnam,
		CAST(extseq AS VARCHAR(MAX)) AS extseq,
		CAST(faiseq AS VARCHAR(MAX)) AS faiseq,
		CONVERT(varchar(max), fivdat, 126) AS fivdat,
		CAST(fivsts AS VARCHAR(MAX)) AS fivsts,
		CAST(fivusr AS VARCHAR(MAX)) AS fivusr,
		CAST(hidprc AS VARCHAR(MAX)) AS hidprc,
		CAST(hndseq AS VARCHAR(MAX)) AS hndseq,
		CAST(icscat AS VARCHAR(MAX)) AS icscat,
		CAST(icsref AS VARCHAR(MAX)) AS icsref,
		CAST(idncod AS VARCHAR(MAX)) AS idncod,
		CAST(infsts AS VARCHAR(MAX)) AS infsts,
		CAST(lincod AS VARCHAR(MAX)) AS lincod,
		CAST(linnam AS VARCHAR(MAX)) AS linnam,
		CAST(lintyp AS VARCHAR(MAX)) AS lintyp,
		CAST(maploc AS VARCHAR(MAX)) AS maploc,
		CAST(mrqalt AS VARCHAR(MAX)) AS mrqalt,
		CAST(mrqnum AS VARCHAR(MAX)) AS mrqnum,
		CAST(mstseq AS VARCHAR(MAX)) AS mstseq,
		CAST(ofmcod AS VARCHAR(MAX)) AS ofmcod,
		CAST(ordbk1 AS VARCHAR(MAX)) AS ordbk1,
		CONVERT(varchar(max), orddat, 126) AS orddat,
		CONVERT(varchar(max), oreval, 126) AS oreval,
		CAST(otscod AS VARCHAR(MAX)) AS otscod,
		CAST(prccst AS VARCHAR(MAX)) AS prccst,
		CAST(rascat AS VARCHAR(MAX)) AS rascat,
		CAST(rasref AS VARCHAR(MAX)) AS rasref,
		CAST(ratsts AS VARCHAR(MAX)) AS ratsts,
		CAST(rcvcod AS VARCHAR(MAX)) AS rcvcod,
		CONVERT(varchar(max), rcvdat, 126) AS rcvdat,
		CAST(rcvnam AS VARCHAR(MAX)) AS rcvnam,
		CAST(rcvtyp AS VARCHAR(MAX)) AS rcvtyp,
		CAST(recusr AS VARCHAR(MAX)) AS recusr,
		CONVERT(varchar(max), reqdat, 126) AS reqdat,
		CAST(reqqty AS VARCHAR(MAX)) AS reqqty,
		CAST(reqrra AS VARCHAR(MAX)) AS reqrra,
		CAST(reqrty AS VARCHAR(MAX)) AS reqrty,
		CAST(reqsts AS VARCHAR(MAX)) AS reqsts,
		CAST(requnt AS VARCHAR(MAX)) AS requnt,
		CAST(rfqsts AS VARCHAR(MAX)) AS rfqsts,
		CAST(rplref AS VARCHAR(MAX)) AS rplref,
		CAST(rsptyp AS VARCHAR(MAX)) AS rsptyp,
		CONVERT(varchar(max), rtndat, 126) AS rtndat,
		CAST(seqnum AS VARCHAR(MAX)) AS seqnum,
		CAST(splmst AS VARCHAR(MAX)) AS splmst,
		CAST(srctyp AS VARCHAR(MAX)) AS srctyp,
		CAST(tadref AS VARCHAR(MAX)) AS tadref,
		CAST(tndnum AS VARCHAR(MAX)) AS tndnum,
		CAST(tndseq AS VARCHAR(MAX)) AS tndseq,
		CAST(txtext AS VARCHAR(MAX)) AS txtext,
		CAST(txtint AS VARCHAR(MAX)) AS txtint,
		CAST(valcur AS VARCHAR(MAX)) AS valcur,
		CAST(valloc AS VARCHAR(MAX)) AS valloc,
		CAST(vatcod AS VARCHAR(MAX)) AS vatcod,
		CAST(vrfsts AS VARCHAR(MAX)) AS vrfsts,
		CAST(vrscod AS VARCHAR(MAX)) AS vrscod,
		CAST(wwfseq AS VARCHAR(MAX)) AS wwfseq 
	FROM Rainbow_MD.rainbow.mrqlin
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    