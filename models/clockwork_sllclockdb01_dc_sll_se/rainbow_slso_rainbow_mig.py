
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'accnsh': 'varchar(max)', 'acpdch': 'varchar(max)', 'acprch': 'varchar(max)', 'acsalh': 'varchar(max)', 'admad': 'varchar(max)', 'admadd': 'varchar(max)', 'agncod': 'varchar(max)', 'aodgid': 'varchar(max)', 'aprctr': 'varchar(max)', 'astcod': 'varchar(max)', 'ausctr': 'varchar(max)', 'basunt': 'varchar(max)', 'bilrtl': 'varchar(max)', 'brkdwn': 'varchar(max)', 'budunt': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'clclot': 'varchar(max)', 'cnsgnd': 'varchar(max)', 'cnsref': 'varchar(max)', 'cnsunt': 'varchar(max)', 'cntori': 'varchar(max)', 'compny': 'varchar(max)', 'cprmod': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'criitm': 'varchar(max)', 'csccod': 'varchar(max)', 'cstcod': 'varchar(max)', 'ctrgen': 'varchar(max)', 'ctrlvl': 'varchar(max)', 'dtlunt': 'varchar(max)', 'dtscod': 'varchar(max)', 'earcod': 'varchar(max)', 'envcls': 'varchar(max)', 'excexd': 'varchar(max)', 'excivd': 'varchar(max)', 'exctrd': 'varchar(max)', 'exttyp': 'varchar(max)', 'fcpcod': 'varchar(max)', 'fctcod': 'varchar(max)', 'fixtim': 'varchar(max)', 'gs1ibc': 'varchar(max)', 'hidsrc': 'varchar(max)', 'ictcod': 'varchar(max)', 'idgcod': 'varchar(max)', 'infmod': 'varchar(max)', 'itmact': 'varchar(max)', 'itmlcs': 'varchar(max)', 'itycod': 'varchar(max)', 'logcod': 'varchar(max)', 'lotsiz': 'varchar(max)', 'lwvtyp': 'varchar(max)', 'macofh': 'varchar(max)', 'macqty': 'varchar(max)', 'mapofh': 'varchar(max)', 'mapqty': 'varchar(max)', 'masofh': 'varchar(max)', 'masqty': 'varchar(max)', 'matuse': 'varchar(max)', 'medgrp': 'varchar(max)', 'micqty': 'varchar(max)', 'migcod': 'varchar(max)', 'mignam': 'varchar(max)', 'mipqty': 'varchar(max)', 'misqty': 'varchar(max)', 'mstbbs': 'varchar(max)', 'otomrq': 'varchar(max)', 'pcgcod': 'varchar(max)', 'pdcctr': 'varchar(max)', 'pfmcod': 'varchar(max)', 'pgrcod': 'varchar(max)', 'pinmod': 'varchar(max)', 'plgcod': 'varchar(max)', 'pmdcod': 'varchar(max)', 'pmgcod': 'varchar(max)', 'pplcod': 'varchar(max)', 'pprctr': 'varchar(max)', 'prbuac': 'varchar(max)', 'prccod': 'varchar(max)', 'prcctr': 'varchar(max)', 'prcunt': 'varchar(max)', 'prfcod': 'varchar(max)', 'prncod': 'varchar(max)', 'qamcod': 'varchar(max)', 'qlmcod': 'varchar(max)', 'qtyfml': 'varchar(max)', 'recycl': 'varchar(max)', 'reinit': 'varchar(max)', 'reqcbm': 'varchar(max)', 'reqcbr': 'varchar(max)', 'rrbbud': 'varchar(max)', 'rrbcns': 'varchar(max)', 'rrbdtl': 'varchar(max)', 'rrbprc': 'varchar(max)', 'rrbsal': 'varchar(max)', 'rtbbud': 'varchar(max)', 'rtbcns': 'varchar(max)', 'rtbdtl': 'varchar(max)', 'rtbprd': 'varchar(max)', 'rtbsal': 'varchar(max)', 'saladd': 'varchar(max)', 'salrsp': 'varchar(max)', 'salunt': 'varchar(max)', 'sapcod': 'varchar(max)', 'scpunt': 'varchar(max)', 'spctrt': 'varchar(max)', 'sprmod': 'varchar(max)', 'spsale': 'varchar(max)', 'srtnam': 'varchar(max)', 'srtnum': 'varchar(max)', 'stcrsp': 'varchar(max)', 'sthctr': 'varchar(max)', 'strpdh': 'varchar(max)', 'strpdt': 'varchar(max)', 'strpdv': 'varchar(max)', 'strpqt': 'varchar(max)', 'sublvl': 'varchar(max)', 'supcod': 'varchar(max)', 'taxbas': 'varchar(max)', 'taxcod': 'varchar(max)', 'txgcod': 'varchar(max)', 'txtdsc': 'varchar(max)', 'updori': 'varchar(max)', 'updscp': 'varchar(max)', 'updspp': 'varchar(max)', 'usebcm': 'varchar(max)', 'vargrp': 'varchar(max)', 'vatcns': 'varchar(max)', 'vatpdc': 'varchar(max)', 'vatprc': 'varchar(max)', 'vatsal': 'varchar(max)', 'whscod': 'varchar(max)', 'worrsp': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        batch_size=5000,
        time_column="_data_modified_utc"
    ),
    cron="@daily",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_slso_rainbow_mig_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_slso_rainbow_mig (_data_modified_utc)"]
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
		'Rainbow_SLSO' as _source_catalog,
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
	FROM Rainbow_SLSO.rainbow.mig
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    