
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified': 'date',
 '_source_catalog': 'varchar(max)',
 'accloa': 'varchar(max)',
 'accnsh': 'varchar(max)',
 'accprd': 'varchar(max)',
 'accset': 'varchar(max)',
 'acpdch': 'varchar(max)',
 'acprch': 'varchar(max)',
 'acsalh': 'varchar(max)',
 'actlot': 'varchar(max)',
 'adddsc': 'varchar(max)',
 'agncod': 'varchar(max)',
 'aitmnm': 'varchar(max)',
 'aprctr': 'varchar(max)',
 'asmmdl': 'varchar(max)',
 'astcod': 'varchar(max)',
 'ausctr': 'varchar(max)',
 'basunt': 'varchar(max)',
 'bilmat': 'varchar(max)',
 'bilrtl': 'varchar(max)',
 'brkdwn': 'varchar(max)',
 'budunt': 'varchar(max)',
 'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'clclot': 'varchar(max)',
 'cmplwt': 'varchar(max)',
 'cmplwv': 'varchar(max)',
 'cnsgnd': 'varchar(max)',
 'cnsref': 'varchar(max)',
 'cnsunt': 'varchar(max)',
 'cntori': 'varchar(max)',
 'compny': 'varchar(max)',
 'cpcopo': 'varchar(max)',
 'cpcoso': 'varchar(max)',
 'cpcowo': 'varchar(max)',
 'cprmod': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'criitm': 'varchar(max)',
 'csccod': 'varchar(max)',
 'cstcod': 'varchar(max)',
 'ctrgen': 'varchar(max)',
 'ctrlvl': 'varchar(max)',
 'cuscod': 'varchar(max)',
 'cvlcod': 'varchar(max)',
 'digcod': 'varchar(max)',
 'drwcod': 'varchar(max)',
 'dtlunt': 'varchar(max)',
 'dtscod': 'varchar(max)',
 'eanitm': 'varchar(max)',
 'eanprc': 'varchar(max)',
 'earcod': 'varchar(max)',
 'entfrm': 'varchar(max)',
 'envcls': 'varchar(max)',
 'eprcod': 'varchar(max)',
 'excexd': 'varchar(max)',
 'excivd': 'varchar(max)',
 'exctrd': 'varchar(max)',
 'extnum': 'varchar(max)',
 'exttyp': 'varchar(max)',
 'fcpcod': 'varchar(max)',
 'fctcod': 'varchar(max)',
 'fixtim': 'varchar(max)',
 'hidsrc': 'varchar(max)',
 'hidstc': 'varchar(max)',
 'icccod': 'varchar(max)',
 'ictcod': 'varchar(max)',
 'idgcod': 'varchar(max)',
 'infmod': 'varchar(max)',
 'itgdat': 'varchar(max)',
 'itgref': 'varchar(max)',
 'itmact': 'varchar(max)',
 'itmcod': 'varchar(max)',
 'itmlcs': 'varchar(max)',
 'itmnam': 'varchar(max)',
 'itycod': 'varchar(max)',
 'keywds': 'varchar(max)',
 'lcsdat': 'varchar(max)',
 'lcsusr': 'varchar(max)',
 'llcchd': 'varchar(max)',
 'llcchg': 'varchar(max)',
 'llcdes': 'varchar(max)',
 'llcode': 'varchar(max)',
 'logcod': 'varchar(max)',
 'logdsc': 'varchar(max)',
 'lotsiz': 'varchar(max)',
 'lsswrc': 'varchar(max)',
 'lwvtyp': 'varchar(max)',
 'macofh': 'varchar(max)',
 'macqty': 'varchar(max)',
 'mapofh': 'varchar(max)',
 'mapqty': 'varchar(max)',
 'martpc': 'varchar(max)',
 'masofh': 'varchar(max)',
 'masqty': 'varchar(max)',
 'matret': 'varchar(max)',
 'matuse': 'varchar(max)',
 'medgrp': 'varchar(max)',
 'micqty': 'varchar(max)',
 'migcod': 'varchar(max)',
 'mipqty': 'varchar(max)',
 'misqty': 'varchar(max)',
 'mstadd': 'varchar(max)',
 'mstadp': 'varchar(max)',
 'mstalt': 'varchar(max)',
 'mstbbs': 'varchar(max)',
 'mstbok': 'varchar(max)',
 'mstcoc': 'varchar(max)',
 'mstctf': 'varchar(max)',
 'mstdgc': 'varchar(max)',
 'mstflg': 'varchar(max)',
 'mstitk': 'varchar(max)',
 'mstlcr': 'varchar(max)',
 'mstlpd': 'varchar(max)',
 'mstmpi': 'varchar(max)',
 'mstmqd': 'varchar(max)',
 'mstmrk': 'varchar(max)',
 'mstrat': 'varchar(max)',
 'mstrpc': 'varchar(max)',
 'mstrwk': 'varchar(max)',
 'mstsvc': 'varchar(max)',
 'mstvar': 'varchar(max)',
 'mstvrc': 'varchar(max)',
 'otomrq': 'varchar(max)',
 'pcgcod': 'varchar(max)',
 'pckitc': 'varchar(max)',
 'pckits': 'varchar(max)',
 'pdcctr': 'varchar(max)',
 'pdcitc': 'varchar(max)',
 'pdcitn': 'varchar(max)',
 'pdcnam': 'varchar(max)',
 'pfmcod': 'varchar(max)',
 'pgrcod': 'varchar(max)',
 'picfil': 'varchar(max)',
 'pictyp': 'varchar(max)',
 'pinmod': 'varchar(max)',
 'plgcod': 'varchar(max)',
 'pmdcod': 'varchar(max)',
 'pmgcod': 'varchar(max)',
 'pplcod': 'varchar(max)',
 'pprctr': 'varchar(max)',
 'prbuac': 'varchar(max)',
 'prccod': 'varchar(max)',
 'prcctr': 'varchar(max)',
 'prcunt': 'varchar(max)',
 'prddmd': 'varchar(max)',
 'prdret': 'varchar(max)',
 'prdtim': 'varchar(max)',
 'prfcod': 'varchar(max)',
 'prncod': 'varchar(max)',
 'qamcod': 'varchar(max)',
 'qlmcod': 'varchar(max)',
 'qtyfml': 'varchar(max)',
 'recycl': 'varchar(max)',
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
 'salkit': 'varchar(max)',
 'salrsp': 'varchar(max)',
 'salunt': 'varchar(max)',
 'sapcod': 'varchar(max)',
 'scpunt': 'varchar(max)',
 'settim': 'varchar(max)',
 'sigcod': 'varchar(max)',
 'spctrt': 'varchar(max)',
 'sprkit': 'varchar(max)',
 'sprmod': 'varchar(max)',
 'srtnam': 'varchar(max)',
 'srtnum': 'varchar(max)',
 'stcrsp': 'varchar(max)',
 'sthctr': 'varchar(max)',
 'strpdh': 'varchar(max)',
 'strpdt': 'varchar(max)',
 'strpdv': 'varchar(max)',
 'strpqt': 'varchar(max)',
 'supcod': 'varchar(max)',
 'taxbas': 'varchar(max)',
 'taxcod': 'varchar(max)',
 'thelot': 'varchar(max)',
 'txgcod': 'varchar(max)',
 'txtenv': 'varchar(max)',
 'txtgen': 'varchar(max)',
 'txtitm': 'varchar(max)',
 'untinf': 'varchar(max)',
 'usebcm': 'varchar(max)',
 'vargrp': 'varchar(max)',
 'vcgcod': 'varchar(max)',
 'vernum': 'varchar(max)',
 'vidfil': 'varchar(max)',
 'vidtyp': 'varchar(max)',
 'whscod': 'varchar(max)',
 'worrsp': 'varchar(max)'},
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
		'Rainbow_DS' as _source_catalog,
		CAST(accloa AS VARCHAR(MAX)) AS accloa,
		CAST(accnsh AS VARCHAR(MAX)) AS accnsh,
		CAST(accprd AS VARCHAR(MAX)) AS accprd,
		CAST(accset AS VARCHAR(MAX)) AS accset,
		CAST(acpdch AS VARCHAR(MAX)) AS acpdch,
		CAST(acprch AS VARCHAR(MAX)) AS acprch,
		CAST(acsalh AS VARCHAR(MAX)) AS acsalh,
		CAST(actlot AS VARCHAR(MAX)) AS actlot,
		CAST(adddsc AS VARCHAR(MAX)) AS adddsc,
		CAST(agncod AS VARCHAR(MAX)) AS agncod,
		CAST(aitmnm AS VARCHAR(MAX)) AS aitmnm,
		CAST(aprctr AS VARCHAR(MAX)) AS aprctr,
		CAST(asmmdl AS VARCHAR(MAX)) AS asmmdl,
		CAST(astcod AS VARCHAR(MAX)) AS astcod,
		CAST(ausctr AS VARCHAR(MAX)) AS ausctr,
		CAST(basunt AS VARCHAR(MAX)) AS basunt,
		CAST(bilmat AS VARCHAR(MAX)) AS bilmat,
		CAST(bilrtl AS VARCHAR(MAX)) AS bilrtl,
		CAST(brkdwn AS VARCHAR(MAX)) AS brkdwn,
		CAST(budunt AS VARCHAR(MAX)) AS budunt,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(clclot AS VARCHAR(MAX)) AS clclot,
		CAST(cmplwt AS VARCHAR(MAX)) AS cmplwt,
		CAST(cmplwv AS VARCHAR(MAX)) AS cmplwv,
		CAST(cnsgnd AS VARCHAR(MAX)) AS cnsgnd,
		CAST(cnsref AS VARCHAR(MAX)) AS cnsref,
		CAST(cnsunt AS VARCHAR(MAX)) AS cnsunt,
		CAST(cntori AS VARCHAR(MAX)) AS cntori,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(cpcopo AS VARCHAR(MAX)) AS cpcopo,
		CAST(cpcoso AS VARCHAR(MAX)) AS cpcoso,
		CAST(cpcowo AS VARCHAR(MAX)) AS cpcowo,
		CAST(cprmod AS VARCHAR(MAX)) AS cprmod,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(criitm AS VARCHAR(MAX)) AS criitm,
		CAST(csccod AS VARCHAR(MAX)) AS csccod,
		CAST(cstcod AS VARCHAR(MAX)) AS cstcod,
		CAST(ctrgen AS VARCHAR(MAX)) AS ctrgen,
		CAST(ctrlvl AS VARCHAR(MAX)) AS ctrlvl,
		CAST(cuscod AS VARCHAR(MAX)) AS cuscod,
		CAST(cvlcod AS VARCHAR(MAX)) AS cvlcod,
		CAST(digcod AS VARCHAR(MAX)) AS digcod,
		CAST(drwcod AS VARCHAR(MAX)) AS drwcod,
		CAST(dtlunt AS VARCHAR(MAX)) AS dtlunt,
		CAST(dtscod AS VARCHAR(MAX)) AS dtscod,
		CAST(eanitm AS VARCHAR(MAX)) AS eanitm,
		CAST(eanprc AS VARCHAR(MAX)) AS eanprc,
		CAST(earcod AS VARCHAR(MAX)) AS earcod,
		CAST(entfrm AS VARCHAR(MAX)) AS entfrm,
		CAST(envcls AS VARCHAR(MAX)) AS envcls,
		CAST(eprcod AS VARCHAR(MAX)) AS eprcod,
		CAST(excexd AS VARCHAR(MAX)) AS excexd,
		CAST(excivd AS VARCHAR(MAX)) AS excivd,
		CAST(exctrd AS VARCHAR(MAX)) AS exctrd,
		CAST(extnum AS VARCHAR(MAX)) AS extnum,
		CAST(exttyp AS VARCHAR(MAX)) AS exttyp,
		CAST(fcpcod AS VARCHAR(MAX)) AS fcpcod,
		CAST(fctcod AS VARCHAR(MAX)) AS fctcod,
		CAST(fixtim AS VARCHAR(MAX)) AS fixtim,
		CAST(hidsrc AS VARCHAR(MAX)) AS hidsrc,
		CAST(hidstc AS VARCHAR(MAX)) AS hidstc,
		CAST(icccod AS VARCHAR(MAX)) AS icccod,
		CAST(ictcod AS VARCHAR(MAX)) AS ictcod,
		CAST(idgcod AS VARCHAR(MAX)) AS idgcod,
		CAST(infmod AS VARCHAR(MAX)) AS infmod,
		CONVERT(varchar(max), itgdat, 126) AS itgdat,
		CAST(itgref AS VARCHAR(MAX)) AS itgref,
		CAST(itmact AS VARCHAR(MAX)) AS itmact,
		CAST(itmcod AS VARCHAR(MAX)) AS itmcod,
		CAST(itmlcs AS VARCHAR(MAX)) AS itmlcs,
		CAST(itmnam AS VARCHAR(MAX)) AS itmnam,
		CAST(itycod AS VARCHAR(MAX)) AS itycod,
		CAST(keywds AS VARCHAR(MAX)) AS keywds,
		CONVERT(varchar(max), lcsdat, 126) AS lcsdat,
		CAST(lcsusr AS VARCHAR(MAX)) AS lcsusr,
		CAST(llcchd AS VARCHAR(MAX)) AS llcchd,
		CAST(llcchg AS VARCHAR(MAX)) AS llcchg,
		CAST(llcdes AS VARCHAR(MAX)) AS llcdes,
		CAST(llcode AS VARCHAR(MAX)) AS llcode,
		CAST(logcod AS VARCHAR(MAX)) AS logcod,
		CAST(logdsc AS VARCHAR(MAX)) AS logdsc,
		CAST(lotsiz AS VARCHAR(MAX)) AS lotsiz,
		CAST(lsswrc AS VARCHAR(MAX)) AS lsswrc,
		CAST(lwvtyp AS VARCHAR(MAX)) AS lwvtyp,
		CAST(macofh AS VARCHAR(MAX)) AS macofh,
		CAST(macqty AS VARCHAR(MAX)) AS macqty,
		CAST(mapofh AS VARCHAR(MAX)) AS mapofh,
		CAST(mapqty AS VARCHAR(MAX)) AS mapqty,
		CAST(martpc AS VARCHAR(MAX)) AS martpc,
		CAST(masofh AS VARCHAR(MAX)) AS masofh,
		CAST(masqty AS VARCHAR(MAX)) AS masqty,
		CAST(matret AS VARCHAR(MAX)) AS matret,
		CAST(matuse AS VARCHAR(MAX)) AS matuse,
		CAST(medgrp AS VARCHAR(MAX)) AS medgrp,
		CAST(micqty AS VARCHAR(MAX)) AS micqty,
		CAST(migcod AS VARCHAR(MAX)) AS migcod,
		CAST(mipqty AS VARCHAR(MAX)) AS mipqty,
		CAST(misqty AS VARCHAR(MAX)) AS misqty,
		CAST(mstadd AS VARCHAR(MAX)) AS mstadd,
		CAST(mstadp AS VARCHAR(MAX)) AS mstadp,
		CAST(mstalt AS VARCHAR(MAX)) AS mstalt,
		CAST(mstbbs AS VARCHAR(MAX)) AS mstbbs,
		CAST(mstbok AS VARCHAR(MAX)) AS mstbok,
		CAST(mstcoc AS VARCHAR(MAX)) AS mstcoc,
		CAST(mstctf AS VARCHAR(MAX)) AS mstctf,
		CAST(mstdgc AS VARCHAR(MAX)) AS mstdgc,
		CAST(mstflg AS VARCHAR(MAX)) AS mstflg,
		CAST(mstitk AS VARCHAR(MAX)) AS mstitk,
		CAST(mstlcr AS VARCHAR(MAX)) AS mstlcr,
		CAST(mstlpd AS VARCHAR(MAX)) AS mstlpd,
		CAST(mstmpi AS VARCHAR(MAX)) AS mstmpi,
		CAST(mstmqd AS VARCHAR(MAX)) AS mstmqd,
		CAST(mstmrk AS VARCHAR(MAX)) AS mstmrk,
		CAST(mstrat AS VARCHAR(MAX)) AS mstrat,
		CAST(mstrpc AS VARCHAR(MAX)) AS mstrpc,
		CAST(mstrwk AS VARCHAR(MAX)) AS mstrwk,
		CAST(mstsvc AS VARCHAR(MAX)) AS mstsvc,
		CAST(mstvar AS VARCHAR(MAX)) AS mstvar,
		CAST(mstvrc AS VARCHAR(MAX)) AS mstvrc,
		CAST(otomrq AS VARCHAR(MAX)) AS otomrq,
		CAST(pcgcod AS VARCHAR(MAX)) AS pcgcod,
		CAST(pckitc AS VARCHAR(MAX)) AS pckitc,
		CAST(pckits AS VARCHAR(MAX)) AS pckits,
		CAST(pdcctr AS VARCHAR(MAX)) AS pdcctr,
		CAST(pdcitc AS VARCHAR(MAX)) AS pdcitc,
		CAST(pdcitn AS VARCHAR(MAX)) AS pdcitn,
		CAST(pdcnam AS VARCHAR(MAX)) AS pdcnam,
		CAST(pfmcod AS VARCHAR(MAX)) AS pfmcod,
		CAST(pgrcod AS VARCHAR(MAX)) AS pgrcod,
		CAST(picfil AS VARCHAR(MAX)) AS picfil,
		CAST(pictyp AS VARCHAR(MAX)) AS pictyp,
		CAST(pinmod AS VARCHAR(MAX)) AS pinmod,
		CAST(plgcod AS VARCHAR(MAX)) AS plgcod,
		CAST(pmdcod AS VARCHAR(MAX)) AS pmdcod,
		CAST(pmgcod AS VARCHAR(MAX)) AS pmgcod,
		CAST(pplcod AS VARCHAR(MAX)) AS pplcod,
		CAST(pprctr AS VARCHAR(MAX)) AS pprctr,
		CAST(prbuac AS VARCHAR(MAX)) AS prbuac,
		CAST(prccod AS VARCHAR(MAX)) AS prccod,
		CAST(prcctr AS VARCHAR(MAX)) AS prcctr,
		CAST(prcunt AS VARCHAR(MAX)) AS prcunt,
		CAST(prddmd AS VARCHAR(MAX)) AS prddmd,
		CAST(prdret AS VARCHAR(MAX)) AS prdret,
		CAST(prdtim AS VARCHAR(MAX)) AS prdtim,
		CAST(prfcod AS VARCHAR(MAX)) AS prfcod,
		CAST(prncod AS VARCHAR(MAX)) AS prncod,
		CAST(qamcod AS VARCHAR(MAX)) AS qamcod,
		CAST(qlmcod AS VARCHAR(MAX)) AS qlmcod,
		CAST(qtyfml AS VARCHAR(MAX)) AS qtyfml,
		CAST(recycl AS VARCHAR(MAX)) AS recycl,
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
		CAST(salkit AS VARCHAR(MAX)) AS salkit,
		CAST(salrsp AS VARCHAR(MAX)) AS salrsp,
		CAST(salunt AS VARCHAR(MAX)) AS salunt,
		CAST(sapcod AS VARCHAR(MAX)) AS sapcod,
		CAST(scpunt AS VARCHAR(MAX)) AS scpunt,
		CAST(settim AS VARCHAR(MAX)) AS settim,
		CAST(sigcod AS VARCHAR(MAX)) AS sigcod,
		CAST(spctrt AS VARCHAR(MAX)) AS spctrt,
		CAST(sprkit AS VARCHAR(MAX)) AS sprkit,
		CAST(sprmod AS VARCHAR(MAX)) AS sprmod,
		CAST(srtnam AS VARCHAR(MAX)) AS srtnam,
		CAST(srtnum AS VARCHAR(MAX)) AS srtnum,
		CAST(stcrsp AS VARCHAR(MAX)) AS stcrsp,
		CAST(sthctr AS VARCHAR(MAX)) AS sthctr,
		CAST(strpdh AS VARCHAR(MAX)) AS strpdh,
		CAST(strpdt AS VARCHAR(MAX)) AS strpdt,
		CAST(strpdv AS VARCHAR(MAX)) AS strpdv,
		CAST(strpqt AS VARCHAR(MAX)) AS strpqt,
		CAST(supcod AS VARCHAR(MAX)) AS supcod,
		CAST(taxbas AS VARCHAR(MAX)) AS taxbas,
		CAST(taxcod AS VARCHAR(MAX)) AS taxcod,
		CAST(thelot AS VARCHAR(MAX)) AS thelot,
		CAST(txgcod AS VARCHAR(MAX)) AS txgcod,
		CAST(txtenv AS VARCHAR(MAX)) AS txtenv,
		CAST(txtgen AS VARCHAR(MAX)) AS txtgen,
		CAST(txtitm AS VARCHAR(MAX)) AS txtitm,
		CAST(untinf AS VARCHAR(MAX)) AS untinf,
		CAST(usebcm AS VARCHAR(MAX)) AS usebcm,
		CAST(vargrp AS VARCHAR(MAX)) AS vargrp,
		CAST(vcgcod AS VARCHAR(MAX)) AS vcgcod,
		CAST(vernum AS VARCHAR(MAX)) AS vernum,
		CAST(vidfil AS VARCHAR(MAX)) AS vidfil,
		CAST(vidtyp AS VARCHAR(MAX)) AS vidtyp,
		CAST(whscod AS VARCHAR(MAX)) AS whscod,
		CAST(worrsp AS VARCHAR(MAX)) AS worrsp 
	FROM Rainbow_DS.rainbow.itm
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
        