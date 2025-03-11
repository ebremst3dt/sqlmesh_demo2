
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date',
 '_metadata_modified_utc': 'datetime2',
 '_source_catalog': 'varchar(max)',
 'abcexd': 'varchar(max)',
 'abcexu': 'varchar(max)',
 'abcfvd': 'varchar(max)',
 'abcfvu': 'varchar(max)',
 'abcpvd': 'varchar(max)',
 'abcpvu': 'varchar(max)',
 'abcsld': 'varchar(max)',
 'abcslu': 'varchar(max)',
 'acmcod': 'varchar(max)',
 'adrctr': 'varchar(max)',
 'agncod': 'varchar(max)',
 'agporp': 'varchar(max)',
 'appcls': 'varchar(max)',
 'appfda': 'varchar(max)',
 'appuda': 'varchar(max)',
 'asupnm': 'varchar(max)',
 'auorrq': 'varchar(max)',
 'bankac': 'varchar(max)',
 'barcod': 'varchar(max)',
 'bnsccm': 'varchar(max)',
 'cabcex': 'varchar(max)',
 'cabcfv': 'varchar(max)',
 'cabcpv': 'varchar(max)',
 'cabcsl': 'varchar(max)',
 'ccdlvs': 'varchar(max)',
 'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'chncod': 'varchar(max)',
 'chnfrm': 'varchar(max)',
 'chnunt': 'varchar(max)',
 'clicod': 'varchar(max)',
 'cnfpfc': 'varchar(max)',
 'cnsrr2': 'varchar(max)',
 'cnssep': 'varchar(max)',
 'cntcod': 'varchar(max)',
 'compny': 'varchar(max)',
 'crbcod': 'varchar(max)',
 'crbplm': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'csldld': 'varchar(max)',
 'csldlu': 'varchar(max)',
 'csldlv': 'varchar(max)',
 'csvclq': 'varchar(max)',
 'csvlqd': 'varchar(max)',
 'csvlqu': 'varchar(max)',
 'ctcprs': 'varchar(max)',
 'ctrdis': 'varchar(max)',
 'ctrpri': 'varchar(max)',
 'curcod': 'varchar(max)',
 'curtyp': 'varchar(max)',
 'dcscod': 'varchar(max)',
 'defprc': 'varchar(max)',
 'dicing': 'varchar(max)',
 'dladr1': 'varchar(max)',
 'dladr2': 'varchar(max)',
 'dladr3': 'varchar(max)',
 'dladr4': 'varchar(max)',
 'dlgcod': 'varchar(max)',
 'dlgown': 'varchar(max)',
 'dlvstp': 'varchar(max)',
 'dstcod': 'varchar(max)',
 'ecland': 'varchar(max)',
 'ecregn': 'varchar(max)',
 'enblda': 'varchar(max)',
 'enblva': 'varchar(max)',
 'entfrm': 'varchar(max)',
 'envcls': 'varchar(max)',
 'envfda': 'varchar(max)',
 'envuda': 'varchar(max)',
 'extcod': 'varchar(max)',
 'extprf': 'varchar(max)',
 'exwldt': 'varchar(max)',
 'faxswt': 'varchar(max)',
 'fcoddl': 'varchar(max)',
 'fixdsc': 'varchar(max)',
 'fwacod': 'varchar(max)',
 'fwdcod': 'varchar(max)',
 'genpfc': 'varchar(max)',
 'gentxt': 'varchar(max)',
 'hidsrc': 'varchar(max)',
 'hndldt': 'varchar(max)',
 'ibancd': 'varchar(max)',
 'intext': 'varchar(max)',
 'invapr': 'varchar(max)',
 'invfrq': 'varchar(max)',
 'lintyp': 'varchar(max)',
 'lngcod': 'varchar(max)',
 'maadr1': 'varchar(max)',
 'maadr2': 'varchar(max)',
 'maadr3': 'varchar(max)',
 'maadr4': 'varchar(max)',
 'markup': 'varchar(max)',
 'mstbbs': 'varchar(max)',
 'mstmrk': 'varchar(max)',
 'mstupe': 'varchar(max)',
 'ntffco': 'varchar(max)',
 'ntfown': 'varchar(max)',
 'ourcod': 'varchar(max)',
 'owncod': 'varchar(max)',
 'owntyp': 'varchar(max)',
 'pabcex': 'varchar(max)',
 'pabcfv': 'varchar(max)',
 'pabcpv': 'varchar(max)',
 'pabcsl': 'varchar(max)',
 'pdpcod': 'varchar(max)',
 'phnext': 'varchar(max)',
 'phnfwd': 'varchar(max)',
 'phnprc': 'varchar(max)',
 'phnsvc': 'varchar(max)',
 'phnswt': 'varchar(max)',
 'phnwhs': 'varchar(max)',
 'postac': 'varchar(max)',
 'prccod': 'varchar(max)',
 'prfcod': 'varchar(max)',
 'pricod': 'varchar(max)',
 'psvcld': 'varchar(max)',
 'psvclq': 'varchar(max)',
 'ptdlvm': 'varchar(max)',
 'regcod': 'varchar(max)',
 'regnum': 'varchar(max)',
 'sbiacp': 'varchar(max)',
 'sbihrz': 'varchar(max)',
 'sbiuse': 'varchar(max)',
 'sctcod': 'varchar(max)',
 'sdxcod': 'varchar(max)',
 'sgrcod': 'varchar(max)',
 'sifcod': 'varchar(max)',
 'sixcod': 'varchar(max)',
 'splcod': 'varchar(max)',
 'splord': 'varchar(max)',
 'srtnam': 'varchar(max)',
 'srtnum': 'varchar(max)',
 'stdldt': 'varchar(max)',
 'stycod': 'varchar(max)',
 'supact': 'varchar(max)',
 'supcod': 'varchar(max)',
 'suplcy': 'varchar(max)',
 'supnam': 'varchar(max)',
 'swiftc': 'varchar(max)',
 'tlxswt': 'varchar(max)',
 'tpycod': 'varchar(max)',
 'tpyown': 'varchar(max)',
 'updeco': 'varchar(max)',
 'vatcod': 'varchar(max)',
 'vatnum': 'varchar(max)',
 'viadr1': 'varchar(max)',
 'viadr2': 'varchar(max)',
 'viadr3': 'varchar(max)',
 'viadr4': 'varchar(max)',
 'whscod': 'varchar(max)',
 'xtccod': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        batch_size=1,
        time_column="_data_modified_utc"
    ),
    cron="@daily",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_Rainbow_SLSO_rainbow_sup_data_modified_utc ON clockwork.sllclockdb01_dc_sll_se_Rainbow_SLSO_rainbow_sup (_data_modified_utc)"]
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
		) AS DATE ) as data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'Rainbow_SLSO' as _source_catalog,
		CONVERT(varchar(max), abcexd, 126) AS abcexd,
		CAST(abcexu AS VARCHAR(MAX)) AS abcexu,
		CONVERT(varchar(max), abcfvd, 126) AS abcfvd,
		CAST(abcfvu AS VARCHAR(MAX)) AS abcfvu,
		CONVERT(varchar(max), abcpvd, 126) AS abcpvd,
		CAST(abcpvu AS VARCHAR(MAX)) AS abcpvu,
		CONVERT(varchar(max), abcsld, 126) AS abcsld,
		CAST(abcslu AS VARCHAR(MAX)) AS abcslu,
		CAST(acmcod AS VARCHAR(MAX)) AS acmcod,
		CAST(adrctr AS VARCHAR(MAX)) AS adrctr,
		CAST(agncod AS VARCHAR(MAX)) AS agncod,
		CAST(agporp AS VARCHAR(MAX)) AS agporp,
		CAST(appcls AS VARCHAR(MAX)) AS appcls,
		CONVERT(varchar(max), appfda, 126) AS appfda,
		CONVERT(varchar(max), appuda, 126) AS appuda,
		CAST(asupnm AS VARCHAR(MAX)) AS asupnm,
		CAST(auorrq AS VARCHAR(MAX)) AS auorrq,
		CAST(bankac AS VARCHAR(MAX)) AS bankac,
		CAST(barcod AS VARCHAR(MAX)) AS barcod,
		CAST(bnsccm AS VARCHAR(MAX)) AS bnsccm,
		CAST(cabcex AS VARCHAR(MAX)) AS cabcex,
		CAST(cabcfv AS VARCHAR(MAX)) AS cabcfv,
		CAST(cabcpv AS VARCHAR(MAX)) AS cabcpv,
		CAST(cabcsl AS VARCHAR(MAX)) AS cabcsl,
		CAST(ccdlvs AS VARCHAR(MAX)) AS ccdlvs,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(chncod AS VARCHAR(MAX)) AS chncod,
		CONVERT(varchar(max), chnfrm, 126) AS chnfrm,
		CONVERT(varchar(max), chnunt, 126) AS chnunt,
		CAST(clicod AS VARCHAR(MAX)) AS clicod,
		CAST(cnfpfc AS VARCHAR(MAX)) AS cnfpfc,
		CAST(cnsrr2 AS VARCHAR(MAX)) AS cnsrr2,
		CAST(cnssep AS VARCHAR(MAX)) AS cnssep,
		CAST(cntcod AS VARCHAR(MAX)) AS cntcod,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(crbcod AS VARCHAR(MAX)) AS crbcod,
		CAST(crbplm AS VARCHAR(MAX)) AS crbplm,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CONVERT(varchar(max), csldld, 126) AS csldld,
		CAST(csldlu AS VARCHAR(MAX)) AS csldlu,
		CAST(csldlv AS VARCHAR(MAX)) AS csldlv,
		CAST(csvclq AS VARCHAR(MAX)) AS csvclq,
		CONVERT(varchar(max), csvlqd, 126) AS csvlqd,
		CAST(csvlqu AS VARCHAR(MAX)) AS csvlqu,
		CAST(ctcprs AS VARCHAR(MAX)) AS ctcprs,
		CAST(ctrdis AS VARCHAR(MAX)) AS ctrdis,
		CAST(ctrpri AS VARCHAR(MAX)) AS ctrpri,
		CAST(curcod AS VARCHAR(MAX)) AS curcod,
		CAST(curtyp AS VARCHAR(MAX)) AS curtyp,
		CAST(dcscod AS VARCHAR(MAX)) AS dcscod,
		CAST(defprc AS VARCHAR(MAX)) AS defprc,
		CAST(dicing AS VARCHAR(MAX)) AS dicing,
		CAST(dladr1 AS VARCHAR(MAX)) AS dladr1,
		CAST(dladr2 AS VARCHAR(MAX)) AS dladr2,
		CAST(dladr3 AS VARCHAR(MAX)) AS dladr3,
		CAST(dladr4 AS VARCHAR(MAX)) AS dladr4,
		CAST(dlgcod AS VARCHAR(MAX)) AS dlgcod,
		CAST(dlgown AS VARCHAR(MAX)) AS dlgown,
		CAST(dlvstp AS VARCHAR(MAX)) AS dlvstp,
		CAST(dstcod AS VARCHAR(MAX)) AS dstcod,
		CAST(ecland AS VARCHAR(MAX)) AS ecland,
		CAST(ecregn AS VARCHAR(MAX)) AS ecregn,
		CAST(enblda AS VARCHAR(MAX)) AS enblda,
		CAST(enblva AS VARCHAR(MAX)) AS enblva,
		CAST(entfrm AS VARCHAR(MAX)) AS entfrm,
		CAST(envcls AS VARCHAR(MAX)) AS envcls,
		CONVERT(varchar(max), envfda, 126) AS envfda,
		CONVERT(varchar(max), envuda, 126) AS envuda,
		CAST(extcod AS VARCHAR(MAX)) AS extcod,
		CAST(extprf AS VARCHAR(MAX)) AS extprf,
		CAST(exwldt AS VARCHAR(MAX)) AS exwldt,
		CAST(faxswt AS VARCHAR(MAX)) AS faxswt,
		CAST(fcoddl AS VARCHAR(MAX)) AS fcoddl,
		CAST(fixdsc AS VARCHAR(MAX)) AS fixdsc,
		CAST(fwacod AS VARCHAR(MAX)) AS fwacod,
		CAST(fwdcod AS VARCHAR(MAX)) AS fwdcod,
		CAST(genpfc AS VARCHAR(MAX)) AS genpfc,
		CAST(gentxt AS VARCHAR(MAX)) AS gentxt,
		CAST(hidsrc AS VARCHAR(MAX)) AS hidsrc,
		CAST(hndldt AS VARCHAR(MAX)) AS hndldt,
		CAST(ibancd AS VARCHAR(MAX)) AS ibancd,
		CAST(intext AS VARCHAR(MAX)) AS intext,
		CAST(invapr AS VARCHAR(MAX)) AS invapr,
		CAST(invfrq AS VARCHAR(MAX)) AS invfrq,
		CAST(lintyp AS VARCHAR(MAX)) AS lintyp,
		CAST(lngcod AS VARCHAR(MAX)) AS lngcod,
		CAST(maadr1 AS VARCHAR(MAX)) AS maadr1,
		CAST(maadr2 AS VARCHAR(MAX)) AS maadr2,
		CAST(maadr3 AS VARCHAR(MAX)) AS maadr3,
		CAST(maadr4 AS VARCHAR(MAX)) AS maadr4,
		CAST(markup AS VARCHAR(MAX)) AS markup,
		CAST(mstbbs AS VARCHAR(MAX)) AS mstbbs,
		CAST(mstmrk AS VARCHAR(MAX)) AS mstmrk,
		CAST(mstupe AS VARCHAR(MAX)) AS mstupe,
		CAST(ntffco AS VARCHAR(MAX)) AS ntffco,
		CAST(ntfown AS VARCHAR(MAX)) AS ntfown,
		CAST(ourcod AS VARCHAR(MAX)) AS ourcod,
		CAST(owncod AS VARCHAR(MAX)) AS owncod,
		CAST(owntyp AS VARCHAR(MAX)) AS owntyp,
		CAST(pabcex AS VARCHAR(MAX)) AS pabcex,
		CAST(pabcfv AS VARCHAR(MAX)) AS pabcfv,
		CAST(pabcpv AS VARCHAR(MAX)) AS pabcpv,
		CAST(pabcsl AS VARCHAR(MAX)) AS pabcsl,
		CAST(pdpcod AS VARCHAR(MAX)) AS pdpcod,
		CAST(phnext AS VARCHAR(MAX)) AS phnext,
		CAST(phnfwd AS VARCHAR(MAX)) AS phnfwd,
		CAST(phnprc AS VARCHAR(MAX)) AS phnprc,
		CAST(phnsvc AS VARCHAR(MAX)) AS phnsvc,
		CAST(phnswt AS VARCHAR(MAX)) AS phnswt,
		CAST(phnwhs AS VARCHAR(MAX)) AS phnwhs,
		CAST(postac AS VARCHAR(MAX)) AS postac,
		CAST(prccod AS VARCHAR(MAX)) AS prccod,
		CAST(prfcod AS VARCHAR(MAX)) AS prfcod,
		CAST(pricod AS VARCHAR(MAX)) AS pricod,
		CAST(psvcld AS VARCHAR(MAX)) AS psvcld,
		CAST(psvclq AS VARCHAR(MAX)) AS psvclq,
		CAST(ptdlvm AS VARCHAR(MAX)) AS ptdlvm,
		CAST(regcod AS VARCHAR(MAX)) AS regcod,
		CAST(regnum AS VARCHAR(MAX)) AS regnum,
		CAST(sbiacp AS VARCHAR(MAX)) AS sbiacp,
		CAST(sbihrz AS VARCHAR(MAX)) AS sbihrz,
		CAST(sbiuse AS VARCHAR(MAX)) AS sbiuse,
		CAST(sctcod AS VARCHAR(MAX)) AS sctcod,
		CAST(sdxcod AS VARCHAR(MAX)) AS sdxcod,
		CAST(sgrcod AS VARCHAR(MAX)) AS sgrcod,
		CAST(sifcod AS VARCHAR(MAX)) AS sifcod,
		CAST(sixcod AS VARCHAR(MAX)) AS sixcod,
		CAST(splcod AS VARCHAR(MAX)) AS splcod,
		CAST(splord AS VARCHAR(MAX)) AS splord,
		CAST(srtnam AS VARCHAR(MAX)) AS srtnam,
		CAST(srtnum AS VARCHAR(MAX)) AS srtnum,
		CAST(stdldt AS VARCHAR(MAX)) AS stdldt,
		CAST(stycod AS VARCHAR(MAX)) AS stycod,
		CAST(supact AS VARCHAR(MAX)) AS supact,
		CAST(supcod AS VARCHAR(MAX)) AS supcod,
		CAST(suplcy AS VARCHAR(MAX)) AS suplcy,
		CAST(supnam AS VARCHAR(MAX)) AS supnam,
		CAST(swiftc AS VARCHAR(MAX)) AS swiftc,
		CAST(tlxswt AS VARCHAR(MAX)) AS tlxswt,
		CAST(tpycod AS VARCHAR(MAX)) AS tpycod,
		CAST(tpyown AS VARCHAR(MAX)) AS tpyown,
		CAST(updeco AS VARCHAR(MAX)) AS updeco,
		CAST(vatcod AS VARCHAR(MAX)) AS vatcod,
		CAST(vatnum AS VARCHAR(MAX)) AS vatnum,
		CAST(viadr1 AS VARCHAR(MAX)) AS viadr1,
		CAST(viadr2 AS VARCHAR(MAX)) AS viadr2,
		CAST(viadr3 AS VARCHAR(MAX)) AS viadr3,
		CAST(viadr4 AS VARCHAR(MAX)) AS viadr4,
		CAST(whscod AS VARCHAR(MAX)) AS whscod,
		CAST(xtccod AS VARCHAR(MAX)) AS xtccod 
	FROM Rainbow_SLSO.rainbow.sup
     )y
        WHERE _data_modified_utc between '{start}' and '{end}'
        
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
        