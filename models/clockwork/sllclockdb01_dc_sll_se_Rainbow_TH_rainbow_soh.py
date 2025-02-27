
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified': 'date',
 '_source_catalog': 'varchar(max)',
 'actcod': 'varchar(max)',
 'actdat': 'varchar(max)',
 'acttim': 'varchar(max)',
 'actusr': 'varchar(max)',
 'addcst': 'varchar(max)',
 'addmrk': 'varchar(max)',
 'addsal': 'varchar(max)',
 'agncod': 'varchar(max)',
 'aodmid': 'varchar(max)',
 'cbchld': 'varchar(max)',
 'cdcusn': 'varchar(max)',
 'cddlvo': 'varchar(max)',
 'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'chncod': 'varchar(max)',
 'cnvexc': 'varchar(max)',
 'compny': 'varchar(max)',
 'copsor': 'varchar(max)',
 'covval': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'csccod': 'varchar(max)',
 'cstval': 'varchar(max)',
 'ctrhnd': 'varchar(max)',
 'curcod': 'varchar(max)',
 'curtyp': 'varchar(max)',
 'dathld': 'varchar(max)',
 'ddldat': 'varchar(max)',
 'defprc': 'varchar(max)',
 'disprc': 'varchar(max)',
 'disval': 'varchar(max)',
 'dlachg': 'varchar(max)',
 'dlvdat': 'varchar(max)',
 'dptcod': 'varchar(max)',
 'dspcod': 'varchar(max)',
 'dspdat': 'varchar(max)',
 'dspnam': 'varchar(max)',
 'dsptyp': 'varchar(max)',
 'edists': 'varchar(max)',
 'ettcod': 'varchar(max)',
 'euscod': 'varchar(max)',
 'eusnam': 'varchar(max)',
 'excdat': 'varchar(max)',
 'excrat': 'varchar(max)',
 'expdat': 'varchar(max)',
 'extord': 'varchar(max)',
 'faxdat': 'varchar(max)',
 'faxsts': 'varchar(max)',
 'fixdsc': 'varchar(max)',
 'frtcod': 'varchar(max)',
 'fwacod': 'varchar(max)',
 'fwdcod': 'varchar(max)',
 'gencod': 'varchar(max)',
 'grsam2': 'varchar(max)',
 'grsvdm': 'varchar(max)',
 'grswgk': 'varchar(max)',
 'gvrcod': 'varchar(max)',
 'hldmin': 'varchar(max)',
 'incmrp': 'varchar(max)',
 'invfee': 'varchar(max)',
 'ipdnum': 'varchar(max)',
 'llccod': 'varchar(max)',
 'lngcod': 'varchar(max)',
 'lsdldt': 'varchar(max)',
 'lststs': 'varchar(max)',
 'manddn': 'varchar(max)',
 'manddt': 'varchar(max)',
 'manddv': 'varchar(max)',
 'manfrt': 'varchar(max)',
 'markup': 'varchar(max)',
 'mstfot': 'varchar(max)',
 'mstfrt': 'varchar(max)',
 'mstpyp': 'varchar(max)',
 'msttra': 'varchar(max)',
 'ndldat': 'varchar(max)',
 'netam2': 'varchar(max)',
 'netvdm': 'varchar(max)',
 'netwgk': 'varchar(max)',
 'ntfdat': 'varchar(max)',
 'ntfown': 'varchar(max)',
 'ntftim': 'varchar(max)',
 'numchg': 'varchar(max)',
 'onhold': 'varchar(max)',
 'ordcnf': 'varchar(max)',
 'ordval': 'varchar(max)',
 'otscod': 'varchar(max)',
 'otsgrp': 'varchar(max)',
 'ourctc': 'varchar(max)',
 'ourref': 'varchar(max)',
 'owncod': 'varchar(max)',
 'ownctc': 'varchar(max)',
 'owndat': 'varchar(max)',
 'ownref': 'varchar(max)',
 'ownrf2': 'varchar(max)',
 'owntyp': 'varchar(max)',
 'paycod': 'varchar(max)',
 'paynam': 'varchar(max)',
 'paytyp': 'varchar(max)',
 'pctcod': 'varchar(max)',
 'prbblt': 'varchar(max)',
 'pricod': 'varchar(max)',
 'prjcod': 'varchar(max)',
 'prjprc': 'varchar(max)',
 'prttxd': 'varchar(max)',
 'prttxf': 'varchar(max)',
 'prttxg': 'varchar(max)',
 'prttxh': 'varchar(max)',
 'rcvdat': 'varchar(max)',
 'rcvfrm': 'varchar(max)',
 'rcvnam': 'varchar(max)',
 'rcvunt': 'varchar(max)',
 'regdat': 'varchar(max)',
 'rutcod': 'varchar(max)',
 'sapcod': 'varchar(max)',
 'sbccod': 'varchar(max)',
 'scccod': 'varchar(max)',
 'sdxcod': 'varchar(max)',
 'sifcod': 'varchar(max)',
 'sixcod': 'varchar(max)',
 'snginv': 'varchar(max)',
 'sngtrs': 'varchar(max)',
 'soahnd': 'varchar(max)',
 'sornum': 'varchar(max)',
 'splcod': 'varchar(max)',
 'sysddn': 'varchar(max)',
 'sysddt': 'varchar(max)',
 'sysddv': 'varchar(max)',
 'tdlcod': 'varchar(max)',
 'tdlown': 'varchar(max)',
 'tpycod': 'varchar(max)',
 'tpyown': 'varchar(max)',
 'trfhst': 'varchar(max)',
 'trfsta': 'varchar(max)',
 'trssts': 'varchar(max)',
 'txtdlv': 'varchar(max)',
 'txtfot': 'varchar(max)',
 'txtgen': 'varchar(max)',
 'txthed': 'varchar(max)',
 'txtinv': 'varchar(max)',
 'usemai': 'varchar(max)',
 'useref': 'varchar(max)',
 'usrhld': 'varchar(max)',
 'vatcod': 'varchar(max)',
 'vatuse': 'varchar(max)',
 'wtrcod': 'varchar(max)',
 'wtrown': 'varchar(max)',
 'xtccod': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified"
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
	SELECT TOP 1000 
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
            ) AS _data_modified,
		'Rainbow_TH' as _source_catalog,
		CAST(actcod AS VARCHAR(MAX)) AS actcod,
		CONVERT(varchar(max), actdat, 126) AS actdat,
		CONVERT(varchar(max), acttim, 126) AS acttim,
		CAST(actusr AS VARCHAR(MAX)) AS actusr,
		CAST(addcst AS VARCHAR(MAX)) AS addcst,
		CAST(addmrk AS VARCHAR(MAX)) AS addmrk,
		CAST(addsal AS VARCHAR(MAX)) AS addsal,
		CAST(agncod AS VARCHAR(MAX)) AS agncod,
		CAST(aodmid AS VARCHAR(MAX)) AS aodmid,
		CAST(cbchld AS VARCHAR(MAX)) AS cbchld,
		CAST(cdcusn AS VARCHAR(MAX)) AS cdcusn,
		CAST(cddlvo AS VARCHAR(MAX)) AS cddlvo,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(chncod AS VARCHAR(MAX)) AS chncod,
		CAST(cnvexc AS VARCHAR(MAX)) AS cnvexc,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(copsor AS VARCHAR(MAX)) AS copsor,
		CAST(covval AS VARCHAR(MAX)) AS covval,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(csccod AS VARCHAR(MAX)) AS csccod,
		CAST(cstval AS VARCHAR(MAX)) AS cstval,
		CAST(ctrhnd AS VARCHAR(MAX)) AS ctrhnd,
		CAST(curcod AS VARCHAR(MAX)) AS curcod,
		CAST(curtyp AS VARCHAR(MAX)) AS curtyp,
		CONVERT(varchar(max), dathld, 126) AS dathld,
		CONVERT(varchar(max), ddldat, 126) AS ddldat,
		CAST(defprc AS VARCHAR(MAX)) AS defprc,
		CAST(disprc AS VARCHAR(MAX)) AS disprc,
		CAST(disval AS VARCHAR(MAX)) AS disval,
		CAST(dlachg AS VARCHAR(MAX)) AS dlachg,
		CONVERT(varchar(max), dlvdat, 126) AS dlvdat,
		CAST(dptcod AS VARCHAR(MAX)) AS dptcod,
		CAST(dspcod AS VARCHAR(MAX)) AS dspcod,
		CONVERT(varchar(max), dspdat, 126) AS dspdat,
		CAST(dspnam AS VARCHAR(MAX)) AS dspnam,
		CAST(dsptyp AS VARCHAR(MAX)) AS dsptyp,
		CAST(edists AS VARCHAR(MAX)) AS edists,
		CAST(ettcod AS VARCHAR(MAX)) AS ettcod,
		CAST(euscod AS VARCHAR(MAX)) AS euscod,
		CAST(eusnam AS VARCHAR(MAX)) AS eusnam,
		CONVERT(varchar(max), excdat, 126) AS excdat,
		CAST(excrat AS VARCHAR(MAX)) AS excrat,
		CONVERT(varchar(max), expdat, 126) AS expdat,
		CAST(extord AS VARCHAR(MAX)) AS extord,
		CONVERT(varchar(max), faxdat, 126) AS faxdat,
		CAST(faxsts AS VARCHAR(MAX)) AS faxsts,
		CAST(fixdsc AS VARCHAR(MAX)) AS fixdsc,
		CAST(frtcod AS VARCHAR(MAX)) AS frtcod,
		CAST(fwacod AS VARCHAR(MAX)) AS fwacod,
		CAST(fwdcod AS VARCHAR(MAX)) AS fwdcod,
		CAST(gencod AS VARCHAR(MAX)) AS gencod,
		CAST(grsam2 AS VARCHAR(MAX)) AS grsam2,
		CAST(grsvdm AS VARCHAR(MAX)) AS grsvdm,
		CAST(grswgk AS VARCHAR(MAX)) AS grswgk,
		CAST(gvrcod AS VARCHAR(MAX)) AS gvrcod,
		CAST(hldmin AS VARCHAR(MAX)) AS hldmin,
		CAST(incmrp AS VARCHAR(MAX)) AS incmrp,
		CAST(invfee AS VARCHAR(MAX)) AS invfee,
		CAST(ipdnum AS VARCHAR(MAX)) AS ipdnum,
		CAST(llccod AS VARCHAR(MAX)) AS llccod,
		CAST(lngcod AS VARCHAR(MAX)) AS lngcod,
		CONVERT(varchar(max), lsdldt, 126) AS lsdldt,
		CAST(lststs AS VARCHAR(MAX)) AS lststs,
		CAST(manddn AS VARCHAR(MAX)) AS manddn,
		CAST(manddt AS VARCHAR(MAX)) AS manddt,
		CAST(manddv AS VARCHAR(MAX)) AS manddv,
		CAST(manfrt AS VARCHAR(MAX)) AS manfrt,
		CAST(markup AS VARCHAR(MAX)) AS markup,
		CAST(mstfot AS VARCHAR(MAX)) AS mstfot,
		CAST(mstfrt AS VARCHAR(MAX)) AS mstfrt,
		CAST(mstpyp AS VARCHAR(MAX)) AS mstpyp,
		CAST(msttra AS VARCHAR(MAX)) AS msttra,
		CONVERT(varchar(max), ndldat, 126) AS ndldat,
		CAST(netam2 AS VARCHAR(MAX)) AS netam2,
		CAST(netvdm AS VARCHAR(MAX)) AS netvdm,
		CAST(netwgk AS VARCHAR(MAX)) AS netwgk,
		CONVERT(varchar(max), ntfdat, 126) AS ntfdat,
		CAST(ntfown AS VARCHAR(MAX)) AS ntfown,
		CAST(ntftim AS VARCHAR(MAX)) AS ntftim,
		CAST(numchg AS VARCHAR(MAX)) AS numchg,
		CAST(onhold AS VARCHAR(MAX)) AS onhold,
		CAST(ordcnf AS VARCHAR(MAX)) AS ordcnf,
		CAST(ordval AS VARCHAR(MAX)) AS ordval,
		CAST(otscod AS VARCHAR(MAX)) AS otscod,
		CAST(otsgrp AS VARCHAR(MAX)) AS otsgrp,
		CAST(ourctc AS VARCHAR(MAX)) AS ourctc,
		CAST(ourref AS VARCHAR(MAX)) AS ourref,
		CAST(owncod AS VARCHAR(MAX)) AS owncod,
		CAST(ownctc AS VARCHAR(MAX)) AS ownctc,
		CONVERT(varchar(max), owndat, 126) AS owndat,
		CAST(ownref AS VARCHAR(MAX)) AS ownref,
		CAST(ownrf2 AS VARCHAR(MAX)) AS ownrf2,
		CAST(owntyp AS VARCHAR(MAX)) AS owntyp,
		CAST(paycod AS VARCHAR(MAX)) AS paycod,
		CAST(paynam AS VARCHAR(MAX)) AS paynam,
		CAST(paytyp AS VARCHAR(MAX)) AS paytyp,
		CAST(pctcod AS VARCHAR(MAX)) AS pctcod,
		CAST(prbblt AS VARCHAR(MAX)) AS prbblt,
		CAST(pricod AS VARCHAR(MAX)) AS pricod,
		CAST(prjcod AS VARCHAR(MAX)) AS prjcod,
		CAST(prjprc AS VARCHAR(MAX)) AS prjprc,
		CAST(prttxd AS VARCHAR(MAX)) AS prttxd,
		CAST(prttxf AS VARCHAR(MAX)) AS prttxf,
		CAST(prttxg AS VARCHAR(MAX)) AS prttxg,
		CAST(prttxh AS VARCHAR(MAX)) AS prttxh,
		CONVERT(varchar(max), rcvdat, 126) AS rcvdat,
		CAST(rcvfrm AS VARCHAR(MAX)) AS rcvfrm,
		CAST(rcvnam AS VARCHAR(MAX)) AS rcvnam,
		CAST(rcvunt AS VARCHAR(MAX)) AS rcvunt,
		CONVERT(varchar(max), regdat, 126) AS regdat,
		CAST(rutcod AS VARCHAR(MAX)) AS rutcod,
		CAST(sapcod AS VARCHAR(MAX)) AS sapcod,
		CAST(sbccod AS VARCHAR(MAX)) AS sbccod,
		CAST(scccod AS VARCHAR(MAX)) AS scccod,
		CAST(sdxcod AS VARCHAR(MAX)) AS sdxcod,
		CAST(sifcod AS VARCHAR(MAX)) AS sifcod,
		CAST(sixcod AS VARCHAR(MAX)) AS sixcod,
		CAST(snginv AS VARCHAR(MAX)) AS snginv,
		CAST(sngtrs AS VARCHAR(MAX)) AS sngtrs,
		CAST(soahnd AS VARCHAR(MAX)) AS soahnd,
		CAST(sornum AS VARCHAR(MAX)) AS sornum,
		CAST(splcod AS VARCHAR(MAX)) AS splcod,
		CAST(sysddn AS VARCHAR(MAX)) AS sysddn,
		CAST(sysddt AS VARCHAR(MAX)) AS sysddt,
		CAST(sysddv AS VARCHAR(MAX)) AS sysddv,
		CAST(tdlcod AS VARCHAR(MAX)) AS tdlcod,
		CAST(tdlown AS VARCHAR(MAX)) AS tdlown,
		CAST(tpycod AS VARCHAR(MAX)) AS tpycod,
		CAST(tpyown AS VARCHAR(MAX)) AS tpyown,
		CAST(trfhst AS VARCHAR(MAX)) AS trfhst,
		CAST(trfsta AS VARCHAR(MAX)) AS trfsta,
		CAST(trssts AS VARCHAR(MAX)) AS trssts,
		CAST(txtdlv AS VARCHAR(MAX)) AS txtdlv,
		CAST(txtfot AS VARCHAR(MAX)) AS txtfot,
		CAST(txtgen AS VARCHAR(MAX)) AS txtgen,
		CAST(txthed AS VARCHAR(MAX)) AS txthed,
		CAST(txtinv AS VARCHAR(MAX)) AS txtinv,
		CAST(usemai AS VARCHAR(MAX)) AS usemai,
		CAST(useref AS VARCHAR(MAX)) AS useref,
		CAST(usrhld AS VARCHAR(MAX)) AS usrhld,
		CAST(vatcod AS VARCHAR(MAX)) AS vatcod,
		CAST(vatuse AS VARCHAR(MAX)) AS vatuse,
		CAST(wtrcod AS VARCHAR(MAX)) AS wtrcod,
		CAST(wtrown AS VARCHAR(MAX)) AS wtrown,
		CAST(xtccod AS VARCHAR(MAX)) AS xtccod 
	FROM Rainbow_TH.rainbow.soh
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
        