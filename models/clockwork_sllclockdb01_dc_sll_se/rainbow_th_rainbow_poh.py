
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.clockwork import start

    
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'acrctr': 'varchar(max)', 'actcod': 'varchar(max)', 'acttim': 'varchar(max)', 'actusr': 'varchar(max)', 'agncod': 'varchar(max)', 'cbchld': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'chncod': 'varchar(max)', 'cnfdat': 'varchar(max)', 'cnfnum': 'varchar(max)', 'cnvexc': 'varchar(max)', 'compny': 'varchar(max)', 'coppor': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'csccod': 'varchar(max)', 'ctaghd': 'varchar(max)', 'ctragr': 'varchar(max)', 'curcod': 'varchar(max)', 'dathld': 'varchar(max)', 'disprc': 'varchar(max)', 'disval': 'varchar(max)', 'dlvdat': 'varchar(max)', 'docnbr': 'varchar(max)', 'dptcod': 'varchar(max)', 'dspdat': 'varchar(max)', 'dspnam': 'varchar(max)', 'dstper': 'varchar(max)', 'edists': 'varchar(max)', 'ettcod': 'varchar(max)', 'excdat': 'varchar(max)', 'excmsg': 'varchar(max)', 'excrat': 'varchar(max)', 'faxdat': 'varchar(max)', 'faxsts': 'varchar(max)', 'frtref': 'varchar(max)', 'fwacod': 'varchar(max)', 'fwdcod': 'varchar(max)', 'hldmin': 'varchar(max)', 'incmrp': 'varchar(max)', 'invapr': 'varchar(max)', 'lindpl': 'varchar(max)', 'linfdl': 'varchar(max)', 'linfin': 'varchar(max)', 'linpin': 'varchar(max)', 'llccod': 'varchar(max)', 'lngcod': 'varchar(max)', 'lsdldt': 'varchar(max)', 'lstdlv': 'varchar(max)', 'lststs': 'varchar(max)', 'mstfot': 'varchar(max)', 'mstfrt': 'varchar(max)', 'mstpyp': 'varchar(max)', 'mstqar': 'varchar(max)', 'msttra': 'varchar(max)', 'ntfdat': 'varchar(max)', 'ntfown': 'varchar(max)', 'ntftim': 'varchar(max)', 'numchg': 'varchar(max)', 'onhold': 'varchar(max)', 'ordcnf': 'varchar(max)', 'ordval': 'varchar(max)', 'orqdat': 'varchar(max)', 'otpcod': 'varchar(max)', 'otpgrp': 'varchar(max)', 'ourcod': 'varchar(max)', 'ourref': 'varchar(max)', 'owncod': 'varchar(max)', 'ownctc': 'varchar(max)', 'ownref': 'varchar(max)', 'owntyp': 'varchar(max)', 'paycod': 'varchar(max)', 'payfrt': 'varchar(max)', 'paynam': 'varchar(max)', 'paytyp': 'varchar(max)', 'pcccod': 'varchar(max)', 'poadat': 'varchar(max)', 'poahnd': 'varchar(max)', 'pornum': 'varchar(max)', 'prccod': 'varchar(max)', 'pricod': 'varchar(max)', 'prjcod': 'varchar(max)', 'rcvcod': 'varchar(max)', 'rcvdat': 'varchar(max)', 'rcvnam': 'varchar(max)', 'rcvtyp': 'varchar(max)', 'regdat': 'varchar(max)', 'reqnum': 'varchar(max)', 'requsr': 'varchar(max)', 'revnum': 'varchar(max)', 'sbihnd': 'varchar(max)', 'tdlcod': 'varchar(max)', 'tdlown': 'varchar(max)', 'totlin': 'varchar(max)', 'tpycod': 'varchar(max)', 'tpyown': 'varchar(max)', 'trfhst': 'varchar(max)', 'trfsta': 'varchar(max)', 'trnnum': 'varchar(max)', 'trnseq': 'varchar(max)', 'trntyp': 'varchar(max)', 'trsdat': 'varchar(max)', 'trssts': 'varchar(max)', 'txtdlv': 'varchar(max)', 'txtfot': 'varchar(max)', 'txtgen': 'varchar(max)', 'txthed': 'varchar(max)', 'usrhld': 'varchar(max)', 'vatcod': 'varchar(max)', 'vatuse': 'varchar(max)', 'wtrcod': 'varchar(max)', 'wtrown': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['compny', 'pornum']
    ),
    start=start,
    cron="0 2 * * *",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_th_rainbow_poh_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_th_rainbow_poh (_data_modified_utc)"]
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
		'Rainbow_TH' as _source_catalog,
		CAST(acrctr AS VARCHAR(MAX)) AS acrctr,
		CAST(actcod AS VARCHAR(MAX)) AS actcod,
		CONVERT(varchar(max), acttim, 126) AS acttim,
		CAST(actusr AS VARCHAR(MAX)) AS actusr,
		CAST(agncod AS VARCHAR(MAX)) AS agncod,
		CAST(cbchld AS VARCHAR(MAX)) AS cbchld,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(chncod AS VARCHAR(MAX)) AS chncod,
		CONVERT(varchar(max), cnfdat, 126) AS cnfdat,
		CAST(cnfnum AS VARCHAR(MAX)) AS cnfnum,
		CAST(cnvexc AS VARCHAR(MAX)) AS cnvexc,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(coppor AS VARCHAR(MAX)) AS coppor,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(csccod AS VARCHAR(MAX)) AS csccod,
		CAST(ctaghd AS VARCHAR(MAX)) AS ctaghd,
		CAST(ctragr AS VARCHAR(MAX)) AS ctragr,
		CAST(curcod AS VARCHAR(MAX)) AS curcod,
		CONVERT(varchar(max), dathld, 126) AS dathld,
		CAST(disprc AS VARCHAR(MAX)) AS disprc,
		CAST(disval AS VARCHAR(MAX)) AS disval,
		CONVERT(varchar(max), dlvdat, 126) AS dlvdat,
		CAST(docnbr AS VARCHAR(MAX)) AS docnbr,
		CAST(dptcod AS VARCHAR(MAX)) AS dptcod,
		CONVERT(varchar(max), dspdat, 126) AS dspdat,
		CAST(dspnam AS VARCHAR(MAX)) AS dspnam,
		CAST(dstper AS VARCHAR(MAX)) AS dstper,
		CAST(edists AS VARCHAR(MAX)) AS edists,
		CAST(ettcod AS VARCHAR(MAX)) AS ettcod,
		CONVERT(varchar(max), excdat, 126) AS excdat,
		CAST(excmsg AS VARCHAR(MAX)) AS excmsg,
		CAST(excrat AS VARCHAR(MAX)) AS excrat,
		CONVERT(varchar(max), faxdat, 126) AS faxdat,
		CAST(faxsts AS VARCHAR(MAX)) AS faxsts,
		CAST(frtref AS VARCHAR(MAX)) AS frtref,
		CAST(fwacod AS VARCHAR(MAX)) AS fwacod,
		CAST(fwdcod AS VARCHAR(MAX)) AS fwdcod,
		CAST(hldmin AS VARCHAR(MAX)) AS hldmin,
		CAST(incmrp AS VARCHAR(MAX)) AS incmrp,
		CAST(invapr AS VARCHAR(MAX)) AS invapr,
		CAST(lindpl AS VARCHAR(MAX)) AS lindpl,
		CAST(linfdl AS VARCHAR(MAX)) AS linfdl,
		CAST(linfin AS VARCHAR(MAX)) AS linfin,
		CAST(linpin AS VARCHAR(MAX)) AS linpin,
		CAST(llccod AS VARCHAR(MAX)) AS llccod,
		CAST(lngcod AS VARCHAR(MAX)) AS lngcod,
		CONVERT(varchar(max), lsdldt, 126) AS lsdldt,
		CONVERT(varchar(max), lstdlv, 126) AS lstdlv,
		CAST(lststs AS VARCHAR(MAX)) AS lststs,
		CAST(mstfot AS VARCHAR(MAX)) AS mstfot,
		CAST(mstfrt AS VARCHAR(MAX)) AS mstfrt,
		CAST(mstpyp AS VARCHAR(MAX)) AS mstpyp,
		CAST(mstqar AS VARCHAR(MAX)) AS mstqar,
		CAST(msttra AS VARCHAR(MAX)) AS msttra,
		CONVERT(varchar(max), ntfdat, 126) AS ntfdat,
		CAST(ntfown AS VARCHAR(MAX)) AS ntfown,
		CAST(ntftim AS VARCHAR(MAX)) AS ntftim,
		CAST(numchg AS VARCHAR(MAX)) AS numchg,
		CAST(onhold AS VARCHAR(MAX)) AS onhold,
		CAST(ordcnf AS VARCHAR(MAX)) AS ordcnf,
		CAST(ordval AS VARCHAR(MAX)) AS ordval,
		CONVERT(varchar(max), orqdat, 126) AS orqdat,
		CAST(otpcod AS VARCHAR(MAX)) AS otpcod,
		CAST(otpgrp AS VARCHAR(MAX)) AS otpgrp,
		CAST(ourcod AS VARCHAR(MAX)) AS ourcod,
		CAST(ourref AS VARCHAR(MAX)) AS ourref,
		CAST(owncod AS VARCHAR(MAX)) AS owncod,
		CAST(ownctc AS VARCHAR(MAX)) AS ownctc,
		CAST(ownref AS VARCHAR(MAX)) AS ownref,
		CAST(owntyp AS VARCHAR(MAX)) AS owntyp,
		CAST(paycod AS VARCHAR(MAX)) AS paycod,
		CAST(payfrt AS VARCHAR(MAX)) AS payfrt,
		CAST(paynam AS VARCHAR(MAX)) AS paynam,
		CAST(paytyp AS VARCHAR(MAX)) AS paytyp,
		CAST(pcccod AS VARCHAR(MAX)) AS pcccod,
		CONVERT(varchar(max), poadat, 126) AS poadat,
		CAST(poahnd AS VARCHAR(MAX)) AS poahnd,
		CAST(pornum AS VARCHAR(MAX)) AS pornum,
		CAST(prccod AS VARCHAR(MAX)) AS prccod,
		CAST(pricod AS VARCHAR(MAX)) AS pricod,
		CAST(prjcod AS VARCHAR(MAX)) AS prjcod,
		CAST(rcvcod AS VARCHAR(MAX)) AS rcvcod,
		CONVERT(varchar(max), rcvdat, 126) AS rcvdat,
		CAST(rcvnam AS VARCHAR(MAX)) AS rcvnam,
		CAST(rcvtyp AS VARCHAR(MAX)) AS rcvtyp,
		CONVERT(varchar(max), regdat, 126) AS regdat,
		CAST(reqnum AS VARCHAR(MAX)) AS reqnum,
		CAST(requsr AS VARCHAR(MAX)) AS requsr,
		CAST(revnum AS VARCHAR(MAX)) AS revnum,
		CAST(sbihnd AS VARCHAR(MAX)) AS sbihnd,
		CAST(tdlcod AS VARCHAR(MAX)) AS tdlcod,
		CAST(tdlown AS VARCHAR(MAX)) AS tdlown,
		CAST(totlin AS VARCHAR(MAX)) AS totlin,
		CAST(tpycod AS VARCHAR(MAX)) AS tpycod,
		CAST(tpyown AS VARCHAR(MAX)) AS tpyown,
		CAST(trfhst AS VARCHAR(MAX)) AS trfhst,
		CAST(trfsta AS VARCHAR(MAX)) AS trfsta,
		CAST(trnnum AS VARCHAR(MAX)) AS trnnum,
		CAST(trnseq AS VARCHAR(MAX)) AS trnseq,
		CAST(trntyp AS VARCHAR(MAX)) AS trntyp,
		CONVERT(varchar(max), trsdat, 126) AS trsdat,
		CAST(trssts AS VARCHAR(MAX)) AS trssts,
		CAST(txtdlv AS VARCHAR(MAX)) AS txtdlv,
		CAST(txtfot AS VARCHAR(MAX)) AS txtfot,
		CAST(txtgen AS VARCHAR(MAX)) AS txtgen,
		CAST(txthed AS VARCHAR(MAX)) AS txthed,
		CAST(usrhld AS VARCHAR(MAX)) AS usrhld,
		CAST(vatcod AS VARCHAR(MAX)) AS vatcod,
		CAST(vatuse AS VARCHAR(MAX)) AS vatuse,
		CAST(wtrcod AS VARCHAR(MAX)) AS wtrcod,
		CAST(wtrown AS VARCHAR(MAX)) AS wtrown 
	FROM Rainbow_TH.rainbow.poh
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    