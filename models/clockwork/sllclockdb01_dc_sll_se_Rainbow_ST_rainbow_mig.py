
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'accnsh': 'char(1)',
 'acpdch': 'char(1)',
 'acprch': 'char(1)',
 'acsalh': 'char(1)',
 'admad': 'decimal',
 'admadd': 'decimal',
 'agncod': 'char(10)',
 'aodgid': 'char(10)',
 'aprctr': 'char(1)',
 'astcod': 'char(10)',
 'ausctr': 'char(1)',
 'basunt': 'char(6)',
 'bilrtl': 'char(1)',
 'brkdwn': 'char(1)',
 'budunt': 'char(6)',
 'chgdat': 'datetime',
 'chgusr': 'char(10)',
 'clclot': 'decimal',
 'cnsgnd': 'char(1)',
 'cnsref': 'char(1)',
 'cnsunt': 'char(6)',
 'cntori': 'char(3)',
 'compny': 'char(2)',
 'cprmod': 'char(1)',
 'credat': 'datetime',
 'creusr': 'char(10)',
 'criitm': 'char(1)',
 'csccod': 'char(10)',
 'cstcod': 'char(20)',
 'ctrgen': 'char(1)',
 'ctrlvl': 'int',
 'dtlunt': 'char(6)',
 'dtscod': 'char(10)',
 'earcod': 'char(10)',
 'envcls': 'char(20)',
 'excexd': 'char(1)',
 'excivd': 'char(1)',
 'exctrd': 'char(1)',
 'exttyp': 'char(1)',
 'fcpcod': 'char(10)',
 'fctcod': 'char(10)',
 'fixtim': 'decimal',
 'gs1ibc': 'char(1)',
 'hidsrc': 'char(1)',
 'ictcod': 'char(10)',
 'idgcod': 'char(10)',
 'infmod': 'char(10)',
 'itmact': 'char(1)',
 'itmlcs': 'char(1)',
 'itycod': 'char(10)',
 'logcod': 'char(10)',
 'lotsiz': 'decimal',
 'lwvtyp': 'char(1)',
 'macofh': 'char(1)',
 'macqty': 'decimal',
 'mapofh': 'char(1)',
 'mapqty': 'decimal',
 'masofh': 'char(1)',
 'masqty': 'decimal',
 'matuse': 'char(1)',
 'medgrp': 'char(14)',
 'micqty': 'decimal',
 'migcod': 'char(10)',
 'mignam': 'char(40)',
 'mipqty': 'decimal',
 'misqty': 'decimal',
 'mstbbs': 'char(1)',
 'otomrq': 'char(1)',
 'pcgcod': 'char(10)',
 'pdcctr': 'char(1)',
 'pfmcod': 'char(10)',
 'pgrcod': 'char(10)',
 'pinmod': 'char(10)',
 'plgcod': 'char(10)',
 'pmdcod': 'char(10)',
 'pmgcod': 'char(10)',
 'pplcod': 'char(10)',
 'pprctr': 'char(1)',
 'prbuac': 'char(10)',
 'prccod': 'char(10)',
 'prcctr': 'char(1)',
 'prcunt': 'char(6)',
 'prfcod': 'char(10)',
 'prncod': 'char(10)',
 'qamcod': 'char(10)',
 'qlmcod': 'char(10)',
 'qtyfml': 'char(14)',
 'recycl': 'int',
 'reinit': 'char(1)',
 'reqcbm': 'decimal',
 'reqcbr': 'decimal',
 'rrbbud': 'decimal',
 'rrbcns': 'decimal',
 'rrbdtl': 'decimal',
 'rrbprc': 'decimal',
 'rrbsal': 'decimal',
 'rtbbud': 'char(1)',
 'rtbcns': 'char(1)',
 'rtbdtl': 'char(1)',
 'rtbprd': 'char(1)',
 'rtbsal': 'char(1)',
 'saladd': 'decimal',
 'salrsp': 'char(1)',
 'salunt': 'char(6)',
 'sapcod': 'char(10)',
 'scpunt': 'char(1)',
 'spctrt': 'char(1)',
 'sprmod': 'char(1)',
 'spsale': 'char(1)',
 'srtnam': 'char(10)',
 'srtnum': 'char(10)',
 'stcrsp': 'char(1)',
 'sthctr': 'char(1)',
 'strpdh': 'char(1)',
 'strpdt': 'char(1)',
 'strpdv': 'decimal',
 'strpqt': 'decimal',
 'sublvl': 'char(1)',
 'supcod': 'char(10)',
 'taxbas': 'char(1)',
 'taxcod': 'decimal',
 'txgcod': 'char(10)',
 'txtdsc': 'varchar(max)',
 'updori': 'char(1)',
 'updscp': 'char(1)',
 'updspp': 'char(1)',
 'usebcm': 'char(1)',
 'vargrp': 'char(10)',
 'vatcns': 'char(4)',
 'vatpdc': 'char(4)',
 'vatprc': 'char(4)',
 'vatsal': 'char(4)',
 'whscod': 'char(10)',
 'worrsp': 'char(1)'},
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
 		CAST(accnsh AS VARCHAR(MAX)) AS accnsh,
		CAST(acpdch AS VARCHAR(MAX)) AS acpdch,
		CAST(acprch AS VARCHAR(MAX)) AS acprch,
		CAST(acsalh AS VARCHAR(MAX)) AS acsalh,
		CAST(admad AS VARCHAR(MAX)) AS admad,
		CAST(admadd AS VARCHAR(MAX)) AS admadd,
		CAST(agncod AS VARCHAR(MAX)) AS agncod,
		CAST(aodgid AS VARCHAR(MAX)) AS aodgid,
		CAST(aprctr AS VARCHAR(MAX)) AS aprctr,
		CAST(astcod AS VARCHAR(MAX)) AS astcod,
		CAST(ausctr AS VARCHAR(MAX)) AS ausctr,
		CAST(basunt AS VARCHAR(MAX)) AS basunt,
		CAST(bilrtl AS VARCHAR(MAX)) AS bilrtl,
		CAST(brkdwn AS VARCHAR(MAX)) AS brkdwn,
		CAST(budunt AS VARCHAR(MAX)) AS budunt,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(clclot AS VARCHAR(MAX)) AS clclot,
		CAST(cnsgnd AS VARCHAR(MAX)) AS cnsgnd,
		CAST(cnsref AS VARCHAR(MAX)) AS cnsref,
		CAST(cnsunt AS VARCHAR(MAX)) AS cnsunt,
		CAST(cntori AS VARCHAR(MAX)) AS cntori,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(cprmod AS VARCHAR(MAX)) AS cprmod,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(criitm AS VARCHAR(MAX)) AS criitm,
		CAST(csccod AS VARCHAR(MAX)) AS csccod,
		CAST(cstcod AS VARCHAR(MAX)) AS cstcod,
		CAST(ctrgen AS VARCHAR(MAX)) AS ctrgen,
		CAST(ctrlvl AS VARCHAR(MAX)) AS ctrlvl,
		CAST(dtlunt AS VARCHAR(MAX)) AS dtlunt,
		CAST(dtscod AS VARCHAR(MAX)) AS dtscod,
		CAST(earcod AS VARCHAR(MAX)) AS earcod,
		CAST(envcls AS VARCHAR(MAX)) AS envcls,
		CAST(excexd AS VARCHAR(MAX)) AS excexd,
		CAST(excivd AS VARCHAR(MAX)) AS excivd,
		CAST(exctrd AS VARCHAR(MAX)) AS exctrd,
		CAST(exttyp AS VARCHAR(MAX)) AS exttyp,
		CAST(fcpcod AS VARCHAR(MAX)) AS fcpcod,
		CAST(fctcod AS VARCHAR(MAX)) AS fctcod,
		CAST(fixtim AS VARCHAR(MAX)) AS fixtim,
		CAST(gs1ibc AS VARCHAR(MAX)) AS gs1ibc,
		CAST(hidsrc AS VARCHAR(MAX)) AS hidsrc,
		CAST(ictcod AS VARCHAR(MAX)) AS ictcod,
		CAST(idgcod AS VARCHAR(MAX)) AS idgcod,
		CAST(infmod AS VARCHAR(MAX)) AS infmod,
		CAST(itmact AS VARCHAR(MAX)) AS itmact,
		CAST(itmlcs AS VARCHAR(MAX)) AS itmlcs,
		CAST(itycod AS VARCHAR(MAX)) AS itycod,
		CAST(logcod AS VARCHAR(MAX)) AS logcod,
		CAST(lotsiz AS VARCHAR(MAX)) AS lotsiz,
		CAST(lwvtyp AS VARCHAR(MAX)) AS lwvtyp,
		CAST(macofh AS VARCHAR(MAX)) AS macofh,
		CAST(macqty AS VARCHAR(MAX)) AS macqty,
		CAST(mapofh AS VARCHAR(MAX)) AS mapofh,
		CAST(mapqty AS VARCHAR(MAX)) AS mapqty,
		CAST(masofh AS VARCHAR(MAX)) AS masofh,
		CAST(masqty AS VARCHAR(MAX)) AS masqty,
		CAST(matuse AS VARCHAR(MAX)) AS matuse,
		CAST(medgrp AS VARCHAR(MAX)) AS medgrp,
		CAST(micqty AS VARCHAR(MAX)) AS micqty,
		CAST(migcod AS VARCHAR(MAX)) AS migcod,
		CAST(mignam AS VARCHAR(MAX)) AS mignam,
		CAST(mipqty AS VARCHAR(MAX)) AS mipqty,
		CAST(misqty AS VARCHAR(MAX)) AS misqty,
		CAST(mstbbs AS VARCHAR(MAX)) AS mstbbs,
		CAST(otomrq AS VARCHAR(MAX)) AS otomrq,
		CAST(pcgcod AS VARCHAR(MAX)) AS pcgcod,
		CAST(pdcctr AS VARCHAR(MAX)) AS pdcctr,
		CAST(pfmcod AS VARCHAR(MAX)) AS pfmcod,
		CAST(pgrcod AS VARCHAR(MAX)) AS pgrcod,
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
		CAST(prfcod AS VARCHAR(MAX)) AS prfcod,
		CAST(prncod AS VARCHAR(MAX)) AS prncod,
		CAST(qamcod AS VARCHAR(MAX)) AS qamcod,
		CAST(qlmcod AS VARCHAR(MAX)) AS qlmcod,
		CAST(qtyfml AS VARCHAR(MAX)) AS qtyfml,
		CAST(recycl AS VARCHAR(MAX)) AS recycl,
		CAST(reinit AS VARCHAR(MAX)) AS reinit,
		CAST(reqcbm AS VARCHAR(MAX)) AS reqcbm,
		CAST(reqcbr AS VARCHAR(MAX)) AS reqcbr,
		CAST(rrbbud AS VARCHAR(MAX)) AS rrbbud,
		CAST(rrbcns AS VARCHAR(MAX)) AS rrbcns,
		CAST(rrbdtl AS VARCHAR(MAX)) AS rrbdtl,
		CAST(rrbprc AS VARCHAR(MAX)) AS rrbprc,
		CAST(rrbsal AS VARCHAR(MAX)) AS rrbsal,
		CAST(rtbbud AS VARCHAR(MAX)) AS rtbbud,
		CAST(rtbcns AS VARCHAR(MAX)) AS rtbcns,
		CAST(rtbdtl AS VARCHAR(MAX)) AS rtbdtl,
		CAST(rtbprd AS VARCHAR(MAX)) AS rtbprd,
		CAST(rtbsal AS VARCHAR(MAX)) AS rtbsal,
		CAST(saladd AS VARCHAR(MAX)) AS saladd,
		CAST(salrsp AS VARCHAR(MAX)) AS salrsp,
		CAST(salunt AS VARCHAR(MAX)) AS salunt,
		CAST(sapcod AS VARCHAR(MAX)) AS sapcod,
		CAST(scpunt AS VARCHAR(MAX)) AS scpunt,
		CAST(spctrt AS VARCHAR(MAX)) AS spctrt,
		CAST(sprmod AS VARCHAR(MAX)) AS sprmod,
		CAST(spsale AS VARCHAR(MAX)) AS spsale,
		CAST(srtnam AS VARCHAR(MAX)) AS srtnam,
		CAST(srtnum AS VARCHAR(MAX)) AS srtnum,
		CAST(stcrsp AS VARCHAR(MAX)) AS stcrsp,
		CAST(sthctr AS VARCHAR(MAX)) AS sthctr,
		CAST(strpdh AS VARCHAR(MAX)) AS strpdh,
		CAST(strpdt AS VARCHAR(MAX)) AS strpdt,
		CAST(strpdv AS VARCHAR(MAX)) AS strpdv,
		CAST(strpqt AS VARCHAR(MAX)) AS strpqt,
		CAST(sublvl AS VARCHAR(MAX)) AS sublvl,
		CAST(supcod AS VARCHAR(MAX)) AS supcod,
		CAST(taxbas AS VARCHAR(MAX)) AS taxbas,
		CAST(taxcod AS VARCHAR(MAX)) AS taxcod,
		CAST(txgcod AS VARCHAR(MAX)) AS txgcod,
		CAST(txtdsc AS VARCHAR(MAX)) AS txtdsc,
		CAST(updori AS VARCHAR(MAX)) AS updori,
		CAST(updscp AS VARCHAR(MAX)) AS updscp,
		CAST(updspp AS VARCHAR(MAX)) AS updspp,
		CAST(usebcm AS VARCHAR(MAX)) AS usebcm,
		CAST(vargrp AS VARCHAR(MAX)) AS vargrp,
		CAST(vatcns AS VARCHAR(MAX)) AS vatcns,
		CAST(vatpdc AS VARCHAR(MAX)) AS vatpdc,
		CAST(vatprc AS VARCHAR(MAX)) AS vatprc,
		CAST(vatsal AS VARCHAR(MAX)) AS vatsal,
		CAST(whscod AS VARCHAR(MAX)) AS whscod,
		CAST(worrsp AS VARCHAR(MAX)) AS worrsp 
	FROM Rainbow_ST.rainbow.mig
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
