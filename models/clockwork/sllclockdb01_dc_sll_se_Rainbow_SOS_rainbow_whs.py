
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
 'actcod': 'varchar(max)',
 'adrctr': 'varchar(max)',
 'aphdlc': 'varchar(max)',
 'appcls': 'varchar(max)',
 'appfda': 'varchar(max)',
 'appuda': 'varchar(max)',
 'apwhsc': 'varchar(max)',
 'auidar': 'varchar(max)',
 'autalc': 'varchar(max)',
 'autpck': 'varchar(max)',
 'awhsnm': 'varchar(max)',
 'bankac': 'varchar(max)',
 'blltyp': 'varchar(max)',
 'ccdlvs': 'varchar(max)',
 'cdcval': 'varchar(max)',
 'chgdat': 'varchar(max)',
 'chgusr': 'varchar(max)',
 'clicod': 'varchar(max)',
 'cnsreg': 'varchar(max)',
 'cntcod': 'varchar(max)',
 'compny': 'varchar(max)',
 'cpccby': 'varchar(max)',
 'crbcod': 'varchar(max)',
 'crbplm': 'varchar(max)',
 'credat': 'varchar(max)',
 'creusr': 'varchar(max)',
 'crpwhs': 'varchar(max)',
 'csccod': 'varchar(max)',
 'csldlu': 'varchar(max)',
 'csldlv': 'varchar(max)',
 'csttou': 'varchar(max)',
 'csttov': 'varchar(max)',
 'ctcprs': 'varchar(max)',
 'curcod': 'varchar(max)',
 'curtyp': 'varchar(max)',
 'dbdpdc': 'varchar(max)',
 'dbdprc': 'varchar(max)',
 'dcscod': 'varchar(max)',
 'defacs': 'varchar(max)',
 'defprc': 'varchar(max)',
 'dladr1': 'varchar(max)',
 'dladr2': 'varchar(max)',
 'dladr3': 'varchar(max)',
 'dladr4': 'varchar(max)',
 'dlgcod': 'varchar(max)',
 'dmdwhs': 'varchar(max)',
 'dptcod': 'varchar(max)',
 'dstcod': 'varchar(max)',
 'ecland': 'varchar(max)',
 'eclins': 'varchar(max)',
 'ecregn': 'varchar(max)',
 'enblma': 'varchar(max)',
 'enblva': 'varchar(max)',
 'ettcod': 'varchar(max)',
 'exwldt': 'varchar(max)',
 'faxswt': 'varchar(max)',
 'ficinv': 'varchar(max)',
 'fixarr': 'varchar(max)',
 'fixcrd': 'varchar(max)',
 'fixdsc': 'varchar(max)',
 'fixdsm': 'varchar(max)',
 'fixdsp': 'varchar(max)',
 'fixeoo': 'varchar(max)',
 'fixeor': 'varchar(max)',
 'fixiin': 'varchar(max)',
 'fixior': 'varchar(max)',
 'fixotv': 'varchar(max)',
 'fixpal': 'varchar(max)',
 'fixpil': 'varchar(max)',
 'fixpll': 'varchar(max)',
 'fixprc': 'varchar(max)',
 'fixprq': 'varchar(max)',
 'fixrcp': 'varchar(max)',
 'fixreq': 'varchar(max)',
 'fixrpl': 'varchar(max)',
 'fixsor': 'varchar(max)',
 'fixstt': 'varchar(max)',
 'fwacod': 'varchar(max)',
 'fwdcod': 'varchar(max)',
 'fwstop': 'varchar(max)',
 'gtwcod': 'varchar(max)',
 'gwlcod': 'varchar(max)',
 'gwltyp': 'varchar(max)',
 'hidsrc': 'varchar(max)',
 'hndcst': 'varchar(max)',
 'hndldt': 'varchar(max)',
 'hndtim': 'varchar(max)',
 'hsthrz': 'varchar(max)',
 'inscby': 'varchar(max)',
 'inssts': 'varchar(max)',
 'intext': 'varchar(max)',
 'intloc': 'varchar(max)',
 'invfrq': 'varchar(max)',
 'irtcod': 'varchar(max)',
 'isacby': 'varchar(max)',
 'isactr': 'varchar(max)',
 'isamsg': 'varchar(max)',
 'isazch': 'varchar(max)',
 'isssc1': 'varchar(max)',
 'jobque': 'varchar(max)',
 'lczlbl': 'varchar(max)',
 'lczpcf': 'varchar(max)',
 'linstp': 'varchar(max)',
 'lintyp': 'varchar(max)',
 'llccod': 'varchar(max)',
 'lngcod': 'varchar(max)',
 'locism': 'varchar(max)',
 'maadr1': 'varchar(max)',
 'maadr2': 'varchar(max)',
 'maadr3': 'varchar(max)',
 'maadr4': 'varchar(max)',
 'manreq': 'varchar(max)',
 'markup': 'varchar(max)',
 'mrappw': 'varchar(max)',
 'mrapus': 'varchar(max)',
 'mrenit': 'varchar(max)',
 'mrensu': 'varchar(max)',
 'mstbbs': 'varchar(max)',
 'mstlcm': 'varchar(max)',
 'mstmrk': 'varchar(max)',
 'ortcod': 'varchar(max)',
 'otccod': 'varchar(max)',
 'otrcdb': 'varchar(max)',
 'otrcod': 'varchar(max)',
 'otrcrt': 'varchar(max)',
 'otropn': 'varchar(max)',
 'otrrtn': 'varchar(max)',
 'otsstt': 'varchar(max)',
 'otvcod': 'varchar(max)',
 'owncod': 'varchar(max)',
 'owntyp': 'varchar(max)',
 'pckalc': 'varchar(max)',
 'pckhrz': 'varchar(max)',
 'pcksor': 'varchar(max)',
 'pckwor': 'varchar(max)',
 'phnext': 'varchar(max)',
 'phnswt': 'varchar(max)',
 'picfil': 'varchar(max)',
 'postac': 'varchar(max)',
 'prccod': 'varchar(max)',
 'prczon': 'varchar(max)',
 'prdstc': 'varchar(max)',
 'prfcod': 'varchar(max)',
 'pricod': 'varchar(max)',
 'prpfwa': 'varchar(max)',
 'prprcv': 'varchar(max)',
 'prprel': 'varchar(max)',
 'prpses': 'varchar(max)',
 'prtisa': 'varchar(max)',
 'prtism': 'varchar(max)',
 'prtist': 'varchar(max)',
 'prtisx': 'varchar(max)',
 'pstcod': 'varchar(max)',
 'psttov': 'varchar(max)',
 'psvcld': 'varchar(max)',
 'ptdlvm': 'varchar(max)',
 'qlmcod': 'varchar(max)',
 'regcod': 'varchar(max)',
 'regnum': 'varchar(max)',
 'rspwhs': 'varchar(max)',
 'sclins': 'varchar(max)',
 'scocod': 'varchar(max)',
 'scpwhs': 'varchar(max)',
 'sdxcod': 'varchar(max)',
 'sifcod': 'varchar(max)',
 'sixcod': 'varchar(max)',
 'sordas': 'varchar(max)',
 'sordcp': 'varchar(max)',
 'sorddl': 'varchar(max)',
 'sordpk': 'varchar(max)',
 'sordpl': 'varchar(max)',
 'splcod': 'varchar(max)',
 'splpck': 'varchar(max)',
 'splpll': 'varchar(max)',
 'srtnam': 'varchar(max)',
 'srtnum': 'varchar(max)',
 'srtpck': 'varchar(max)',
 'srtpt1': 'varchar(max)',
 'stdldt': 'varchar(max)',
 'stltyp': 'varchar(max)',
 'svccod': 'varchar(max)',
 'tlxswt': 'varchar(max)',
 'tpycod': 'varchar(max)',
 'tpyown': 'varchar(max)',
 'usdmac': 'varchar(max)',
 'usecdc': 'varchar(max)',
 'usfcst': 'varchar(max)',
 'usfifo': 'varchar(max)',
 'usmulo': 'varchar(max)',
 'uspdst': 'varchar(max)',
 'ustrlo': 'varchar(max)',
 'valfrm': 'varchar(max)',
 'valunt': 'varchar(max)',
 'vatcod': 'varchar(max)',
 'vatnum': 'varchar(max)',
 'vcrgrp': 'varchar(max)',
 'viadr1': 'varchar(max)',
 'viadr2': 'varchar(max)',
 'viadr3': 'varchar(max)',
 'viadr4': 'varchar(max)',
 'wctcod': 'varchar(max)',
 'wgrcod': 'varchar(max)',
 'whsact': 'varchar(max)',
 'whscod': 'varchar(max)',
 'whscst': 'varchar(max)',
 'whsfrm': 'varchar(max)',
 'whslcy': 'varchar(max)',
 'whsnam': 'varchar(max)',
 'whstyp': 'varchar(max)',
 'wttdma': 'varchar(max)',
 'wttdmr': 'varchar(max)',
 'wttiss': 'varchar(max)',
 'wttmrc': 'varchar(max)',
 'wttwdw': 'varchar(max)',
 'wtycod': 'varchar(max)',
 'xtccod': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        batch_size=1,
        time_column="_data_modified_utc"
    ),
    cron="@daily",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_Rainbow_SOS_rainbow_whs_data_modified_utc ON clockwork.sllclockdb01_dc_sll_se_Rainbow_SOS_rainbow_whs (_data_modified_utc)"]
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
		'Rainbow_SOS' as _source_catalog,
		CAST(actcod AS VARCHAR(MAX)) AS actcod,
		CAST(adrctr AS VARCHAR(MAX)) AS adrctr,
		CAST(aphdlc AS VARCHAR(MAX)) AS aphdlc,
		CAST(appcls AS VARCHAR(MAX)) AS appcls,
		CONVERT(varchar(max), appfda, 126) AS appfda,
		CONVERT(varchar(max), appuda, 126) AS appuda,
		CAST(apwhsc AS VARCHAR(MAX)) AS apwhsc,
		CAST(auidar AS VARCHAR(MAX)) AS auidar,
		CAST(autalc AS VARCHAR(MAX)) AS autalc,
		CAST(autpck AS VARCHAR(MAX)) AS autpck,
		CAST(awhsnm AS VARCHAR(MAX)) AS awhsnm,
		CAST(bankac AS VARCHAR(MAX)) AS bankac,
		CAST(blltyp AS VARCHAR(MAX)) AS blltyp,
		CAST(ccdlvs AS VARCHAR(MAX)) AS ccdlvs,
		CAST(cdcval AS VARCHAR(MAX)) AS cdcval,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(clicod AS VARCHAR(MAX)) AS clicod,
		CAST(cnsreg AS VARCHAR(MAX)) AS cnsreg,
		CAST(cntcod AS VARCHAR(MAX)) AS cntcod,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(cpccby AS VARCHAR(MAX)) AS cpccby,
		CAST(crbcod AS VARCHAR(MAX)) AS crbcod,
		CAST(crbplm AS VARCHAR(MAX)) AS crbplm,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(crpwhs AS VARCHAR(MAX)) AS crpwhs,
		CAST(csccod AS VARCHAR(MAX)) AS csccod,
		CAST(csldlu AS VARCHAR(MAX)) AS csldlu,
		CAST(csldlv AS VARCHAR(MAX)) AS csldlv,
		CAST(csttou AS VARCHAR(MAX)) AS csttou,
		CAST(csttov AS VARCHAR(MAX)) AS csttov,
		CAST(ctcprs AS VARCHAR(MAX)) AS ctcprs,
		CAST(curcod AS VARCHAR(MAX)) AS curcod,
		CAST(curtyp AS VARCHAR(MAX)) AS curtyp,
		CAST(dbdpdc AS VARCHAR(MAX)) AS dbdpdc,
		CAST(dbdprc AS VARCHAR(MAX)) AS dbdprc,
		CAST(dcscod AS VARCHAR(MAX)) AS dcscod,
		CAST(defacs AS VARCHAR(MAX)) AS defacs,
		CAST(defprc AS VARCHAR(MAX)) AS defprc,
		CAST(dladr1 AS VARCHAR(MAX)) AS dladr1,
		CAST(dladr2 AS VARCHAR(MAX)) AS dladr2,
		CAST(dladr3 AS VARCHAR(MAX)) AS dladr3,
		CAST(dladr4 AS VARCHAR(MAX)) AS dladr4,
		CAST(dlgcod AS VARCHAR(MAX)) AS dlgcod,
		CAST(dmdwhs AS VARCHAR(MAX)) AS dmdwhs,
		CAST(dptcod AS VARCHAR(MAX)) AS dptcod,
		CAST(dstcod AS VARCHAR(MAX)) AS dstcod,
		CAST(ecland AS VARCHAR(MAX)) AS ecland,
		CONVERT(varchar(max), eclins, 126) AS eclins,
		CAST(ecregn AS VARCHAR(MAX)) AS ecregn,
		CAST(enblma AS VARCHAR(MAX)) AS enblma,
		CAST(enblva AS VARCHAR(MAX)) AS enblva,
		CAST(ettcod AS VARCHAR(MAX)) AS ettcod,
		CAST(exwldt AS VARCHAR(MAX)) AS exwldt,
		CAST(faxswt AS VARCHAR(MAX)) AS faxswt,
		CAST(ficinv AS VARCHAR(MAX)) AS ficinv,
		CAST(fixarr AS VARCHAR(MAX)) AS fixarr,
		CAST(fixcrd AS VARCHAR(MAX)) AS fixcrd,
		CAST(fixdsc AS VARCHAR(MAX)) AS fixdsc,
		CAST(fixdsm AS VARCHAR(MAX)) AS fixdsm,
		CAST(fixdsp AS VARCHAR(MAX)) AS fixdsp,
		CAST(fixeoo AS VARCHAR(MAX)) AS fixeoo,
		CAST(fixeor AS VARCHAR(MAX)) AS fixeor,
		CAST(fixiin AS VARCHAR(MAX)) AS fixiin,
		CAST(fixior AS VARCHAR(MAX)) AS fixior,
		CAST(fixotv AS VARCHAR(MAX)) AS fixotv,
		CAST(fixpal AS VARCHAR(MAX)) AS fixpal,
		CAST(fixpil AS VARCHAR(MAX)) AS fixpil,
		CAST(fixpll AS VARCHAR(MAX)) AS fixpll,
		CAST(fixprc AS VARCHAR(MAX)) AS fixprc,
		CAST(fixprq AS VARCHAR(MAX)) AS fixprq,
		CAST(fixrcp AS VARCHAR(MAX)) AS fixrcp,
		CAST(fixreq AS VARCHAR(MAX)) AS fixreq,
		CAST(fixrpl AS VARCHAR(MAX)) AS fixrpl,
		CAST(fixsor AS VARCHAR(MAX)) AS fixsor,
		CAST(fixstt AS VARCHAR(MAX)) AS fixstt,
		CAST(fwacod AS VARCHAR(MAX)) AS fwacod,
		CAST(fwdcod AS VARCHAR(MAX)) AS fwdcod,
		CAST(fwstop AS VARCHAR(MAX)) AS fwstop,
		CAST(gtwcod AS VARCHAR(MAX)) AS gtwcod,
		CAST(gwlcod AS VARCHAR(MAX)) AS gwlcod,
		CAST(gwltyp AS VARCHAR(MAX)) AS gwltyp,
		CAST(hidsrc AS VARCHAR(MAX)) AS hidsrc,
		CAST(hndcst AS VARCHAR(MAX)) AS hndcst,
		CAST(hndldt AS VARCHAR(MAX)) AS hndldt,
		CAST(hndtim AS VARCHAR(MAX)) AS hndtim,
		CAST(hsthrz AS VARCHAR(MAX)) AS hsthrz,
		CAST(inscby AS VARCHAR(MAX)) AS inscby,
		CAST(inssts AS VARCHAR(MAX)) AS inssts,
		CAST(intext AS VARCHAR(MAX)) AS intext,
		CAST(intloc AS VARCHAR(MAX)) AS intloc,
		CAST(invfrq AS VARCHAR(MAX)) AS invfrq,
		CAST(irtcod AS VARCHAR(MAX)) AS irtcod,
		CAST(isacby AS VARCHAR(MAX)) AS isacby,
		CAST(isactr AS VARCHAR(MAX)) AS isactr,
		CAST(isamsg AS VARCHAR(MAX)) AS isamsg,
		CAST(isazch AS VARCHAR(MAX)) AS isazch,
		CAST(isssc1 AS VARCHAR(MAX)) AS isssc1,
		CAST(jobque AS VARCHAR(MAX)) AS jobque,
		CAST(lczlbl AS VARCHAR(MAX)) AS lczlbl,
		CAST(lczpcf AS VARCHAR(MAX)) AS lczpcf,
		CAST(linstp AS VARCHAR(MAX)) AS linstp,
		CAST(lintyp AS VARCHAR(MAX)) AS lintyp,
		CAST(llccod AS VARCHAR(MAX)) AS llccod,
		CAST(lngcod AS VARCHAR(MAX)) AS lngcod,
		CAST(locism AS VARCHAR(MAX)) AS locism,
		CAST(maadr1 AS VARCHAR(MAX)) AS maadr1,
		CAST(maadr2 AS VARCHAR(MAX)) AS maadr2,
		CAST(maadr3 AS VARCHAR(MAX)) AS maadr3,
		CAST(maadr4 AS VARCHAR(MAX)) AS maadr4,
		CAST(manreq AS VARCHAR(MAX)) AS manreq,
		CAST(markup AS VARCHAR(MAX)) AS markup,
		CAST(mrappw AS VARCHAR(MAX)) AS mrappw,
		CAST(mrapus AS VARCHAR(MAX)) AS mrapus,
		CAST(mrenit AS VARCHAR(MAX)) AS mrenit,
		CAST(mrensu AS VARCHAR(MAX)) AS mrensu,
		CAST(mstbbs AS VARCHAR(MAX)) AS mstbbs,
		CAST(mstlcm AS VARCHAR(MAX)) AS mstlcm,
		CAST(mstmrk AS VARCHAR(MAX)) AS mstmrk,
		CAST(ortcod AS VARCHAR(MAX)) AS ortcod,
		CAST(otccod AS VARCHAR(MAX)) AS otccod,
		CAST(otrcdb AS VARCHAR(MAX)) AS otrcdb,
		CAST(otrcod AS VARCHAR(MAX)) AS otrcod,
		CAST(otrcrt AS VARCHAR(MAX)) AS otrcrt,
		CAST(otropn AS VARCHAR(MAX)) AS otropn,
		CAST(otrrtn AS VARCHAR(MAX)) AS otrrtn,
		CAST(otsstt AS VARCHAR(MAX)) AS otsstt,
		CAST(otvcod AS VARCHAR(MAX)) AS otvcod,
		CAST(owncod AS VARCHAR(MAX)) AS owncod,
		CAST(owntyp AS VARCHAR(MAX)) AS owntyp,
		CAST(pckalc AS VARCHAR(MAX)) AS pckalc,
		CAST(pckhrz AS VARCHAR(MAX)) AS pckhrz,
		CAST(pcksor AS VARCHAR(MAX)) AS pcksor,
		CAST(pckwor AS VARCHAR(MAX)) AS pckwor,
		CAST(phnext AS VARCHAR(MAX)) AS phnext,
		CAST(phnswt AS VARCHAR(MAX)) AS phnswt,
		CAST(picfil AS VARCHAR(MAX)) AS picfil,
		CAST(postac AS VARCHAR(MAX)) AS postac,
		CAST(prccod AS VARCHAR(MAX)) AS prccod,
		CAST(prczon AS VARCHAR(MAX)) AS prczon,
		CAST(prdstc AS VARCHAR(MAX)) AS prdstc,
		CAST(prfcod AS VARCHAR(MAX)) AS prfcod,
		CAST(pricod AS VARCHAR(MAX)) AS pricod,
		CAST(prpfwa AS VARCHAR(MAX)) AS prpfwa,
		CAST(prprcv AS VARCHAR(MAX)) AS prprcv,
		CAST(prprel AS VARCHAR(MAX)) AS prprel,
		CAST(prpses AS VARCHAR(MAX)) AS prpses,
		CAST(prtisa AS VARCHAR(MAX)) AS prtisa,
		CAST(prtism AS VARCHAR(MAX)) AS prtism,
		CAST(prtist AS VARCHAR(MAX)) AS prtist,
		CAST(prtisx AS VARCHAR(MAX)) AS prtisx,
		CAST(pstcod AS VARCHAR(MAX)) AS pstcod,
		CAST(psttov AS VARCHAR(MAX)) AS psttov,
		CAST(psvcld AS VARCHAR(MAX)) AS psvcld,
		CAST(ptdlvm AS VARCHAR(MAX)) AS ptdlvm,
		CAST(qlmcod AS VARCHAR(MAX)) AS qlmcod,
		CAST(regcod AS VARCHAR(MAX)) AS regcod,
		CAST(regnum AS VARCHAR(MAX)) AS regnum,
		CAST(rspwhs AS VARCHAR(MAX)) AS rspwhs,
		CONVERT(varchar(max), sclins, 126) AS sclins,
		CAST(scocod AS VARCHAR(MAX)) AS scocod,
		CAST(scpwhs AS VARCHAR(MAX)) AS scpwhs,
		CAST(sdxcod AS VARCHAR(MAX)) AS sdxcod,
		CAST(sifcod AS VARCHAR(MAX)) AS sifcod,
		CAST(sixcod AS VARCHAR(MAX)) AS sixcod,
		CAST(sordas AS VARCHAR(MAX)) AS sordas,
		CAST(sordcp AS VARCHAR(MAX)) AS sordcp,
		CAST(sorddl AS VARCHAR(MAX)) AS sorddl,
		CAST(sordpk AS VARCHAR(MAX)) AS sordpk,
		CAST(sordpl AS VARCHAR(MAX)) AS sordpl,
		CAST(splcod AS VARCHAR(MAX)) AS splcod,
		CAST(splpck AS VARCHAR(MAX)) AS splpck,
		CAST(splpll AS VARCHAR(MAX)) AS splpll,
		CAST(srtnam AS VARCHAR(MAX)) AS srtnam,
		CAST(srtnum AS VARCHAR(MAX)) AS srtnum,
		CAST(srtpck AS VARCHAR(MAX)) AS srtpck,
		CAST(srtpt1 AS VARCHAR(MAX)) AS srtpt1,
		CAST(stdldt AS VARCHAR(MAX)) AS stdldt,
		CAST(stltyp AS VARCHAR(MAX)) AS stltyp,
		CAST(svccod AS VARCHAR(MAX)) AS svccod,
		CAST(tlxswt AS VARCHAR(MAX)) AS tlxswt,
		CAST(tpycod AS VARCHAR(MAX)) AS tpycod,
		CAST(tpyown AS VARCHAR(MAX)) AS tpyown,
		CAST(usdmac AS VARCHAR(MAX)) AS usdmac,
		CAST(usecdc AS VARCHAR(MAX)) AS usecdc,
		CAST(usfcst AS VARCHAR(MAX)) AS usfcst,
		CAST(usfifo AS VARCHAR(MAX)) AS usfifo,
		CAST(usmulo AS VARCHAR(MAX)) AS usmulo,
		CAST(uspdst AS VARCHAR(MAX)) AS uspdst,
		CAST(ustrlo AS VARCHAR(MAX)) AS ustrlo,
		CONVERT(varchar(max), valfrm, 126) AS valfrm,
		CONVERT(varchar(max), valunt, 126) AS valunt,
		CAST(vatcod AS VARCHAR(MAX)) AS vatcod,
		CAST(vatnum AS VARCHAR(MAX)) AS vatnum,
		CAST(vcrgrp AS VARCHAR(MAX)) AS vcrgrp,
		CAST(viadr1 AS VARCHAR(MAX)) AS viadr1,
		CAST(viadr2 AS VARCHAR(MAX)) AS viadr2,
		CAST(viadr3 AS VARCHAR(MAX)) AS viadr3,
		CAST(viadr4 AS VARCHAR(MAX)) AS viadr4,
		CAST(wctcod AS VARCHAR(MAX)) AS wctcod,
		CAST(wgrcod AS VARCHAR(MAX)) AS wgrcod,
		CAST(whsact AS VARCHAR(MAX)) AS whsact,
		CAST(whscod AS VARCHAR(MAX)) AS whscod,
		CAST(whscst AS VARCHAR(MAX)) AS whscst,
		CAST(whsfrm AS VARCHAR(MAX)) AS whsfrm,
		CAST(whslcy AS VARCHAR(MAX)) AS whslcy,
		CAST(whsnam AS VARCHAR(MAX)) AS whsnam,
		CAST(whstyp AS VARCHAR(MAX)) AS whstyp,
		CAST(wttdma AS VARCHAR(MAX)) AS wttdma,
		CAST(wttdmr AS VARCHAR(MAX)) AS wttdmr,
		CAST(wttiss AS VARCHAR(MAX)) AS wttiss,
		CAST(wttmrc AS VARCHAR(MAX)) AS wttmrc,
		CAST(wttwdw AS VARCHAR(MAX)) AS wttwdw,
		CAST(wtycod AS VARCHAR(MAX)) AS wtycod,
		CAST(xtccod AS VARCHAR(MAX)) AS xtccod
	FROM Rainbow_SOS.rainbow.whs
     )y
        WHERE _data_modified_utc between '{start}' and '{end}'

	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
