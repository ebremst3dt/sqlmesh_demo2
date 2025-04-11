
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.clockwork import start

    
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'annset': 'varchar(max)', 'aprmst': 'varchar(max)', 'bomlev': 'varchar(max)', 'cadpgm': 'varchar(max)', 'cadrcv': 'varchar(max)', 'cadsnd': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'clifac': 'varchar(max)', 'cmntrm': 'varchar(max)', 'cntcod': 'varchar(max)', 'coltab': 'varchar(max)', 'compny': 'varchar(max)', 'crctrl': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'ctcprs': 'varchar(max)', 'curcod': 'varchar(max)', 'cwpsrc': 'varchar(max)', 'digarr': 'varchar(max)', 'digdsm': 'varchar(max)', 'digeor': 'varchar(max)', 'digfag': 'varchar(max)', 'diggwe': 'varchar(max)', 'digido': 'varchar(max)', 'digiiv': 'varchar(max)', 'digior': 'varchar(max)', 'digjrn': 'varchar(max)', 'digoiv': 'varchar(max)', 'digpil': 'varchar(max)', 'digpll': 'varchar(max)', 'digpor': 'varchar(max)', 'digprq': 'varchar(max)', 'digqac': 'varchar(max)', 'digreq': 'varchar(max)', 'digrpl': 'varchar(max)', 'digrqp': 'varchar(max)', 'digshp': 'varchar(max)', 'digsor': 'varchar(max)', 'digstf': 'varchar(max)', 'digstt': 'varchar(max)', 'digwor': 'varchar(max)', 'dladr1': 'varchar(max)', 'dladr2': 'varchar(max)', 'dladr3': 'varchar(max)', 'dladr4': 'varchar(max)', 'dmawhs': 'varchar(max)', 'docstr': 'varchar(max)', 'dqcwhs': 'varchar(max)', 'drwdir': 'varchar(max)', 'drwpgm': 'varchar(max)', 'ecland': 'varchar(max)', 'ecregn': 'varchar(max)', 'emledt': 'varchar(max)', 'euscep': 'varchar(max)', 'euspyl': 'varchar(max)', 'expdir': 'varchar(max)', 'faxswt': 'varchar(max)', 'fctcod': 'varchar(max)', 'fixdsm': 'varchar(max)', 'fixfac': 'varchar(max)', 'fixfas': 'varchar(max)', 'fixgwe': 'varchar(max)', 'fixido': 'varchar(max)', 'fixiiv': 'varchar(max)', 'fixior': 'varchar(max)', 'fixjrn': 'varchar(max)', 'fixoiv': 'varchar(max)', 'fixpil': 'varchar(max)', 'fixpll': 'varchar(max)', 'fixprq': 'varchar(max)', 'fixqac': 'varchar(max)', 'fixreq': 'varchar(max)', 'fixrpl': 'varchar(max)', 'fixrqp': 'varchar(max)', 'fixshp': 'varchar(max)', 'fixsor': 'varchar(max)', 'fixstf': 'varchar(max)', 'fixwor': 'varchar(max)', 'flemnu': 'varchar(max)', 'fnttab': 'varchar(max)', 'frmnam': 'varchar(max)', 'ftbexc': 'varchar(max)', 'ftcexc': 'varchar(max)', 'ftfexc': 'varchar(max)', 'fyoffs': 'varchar(max)', 'gtwcod': 'varchar(max)', 'higbnd': 'varchar(max)', 'icscat': 'varchar(max)', 'icwhit': 'varchar(max)', 'icworg': 'varchar(max)', 'iicgen': 'varchar(max)', 'iivdoi': 'varchar(max)', 'ilglvl': 'varchar(max)', 'impdir': 'varchar(max)', 'infmks': 'varchar(max)', 'jobque': 'varchar(max)', 'lngcod': 'varchar(max)', 'lowbnd': 'varchar(max)', 'lrnco1': 'varchar(max)', 'lrnco2': 'varchar(max)', 'lrnco3': 'varchar(max)', 'lrncur': 'varchar(max)', 'maadr1': 'varchar(max)', 'maadr2': 'varchar(max)', 'maadr3': 'varchar(max)', 'maadr4': 'varchar(max)', 'matadj': 'varchar(max)', 'mcptyp': 'varchar(max)', 'merppr': 'varchar(max)', 'mkrgrp': 'varchar(max)', 'mltrpc': 'varchar(max)', 'mplcod': 'varchar(max)', 'mplper': 'varchar(max)', 'mstslv': 'varchar(max)', 'muntyp': 'varchar(max)', 'ordmst': 'varchar(max)', 'ordtrc': 'varchar(max)', 'otccod': 'varchar(max)', 'otmcod': 'varchar(max)', 'otpcod': 'varchar(max)', 'otscod': 'varchar(max)', 'otsrcv': 'varchar(max)', 'pbacty': 'varchar(max)', 'phnfwd': 'varchar(max)', 'phnord': 'varchar(max)', 'phnprc': 'varchar(max)', 'phnsvc': 'varchar(max)', 'phnswt': 'varchar(max)', 'phnwhs': 'varchar(max)', 'picdir': 'varchar(max)', 'pordat': 'varchar(max)', 'pospor': 'varchar(max)', 'possor': 'varchar(max)', 'poswhs': 'varchar(max)', 'poswor': 'varchar(max)', 'prcbdg': 'varchar(max)', 'purinv': 'varchar(max)', 'ralscl': 'varchar(max)', 'raltyp': 'varchar(max)', 'regnum': 'varchar(max)', 'rqapop': 'varchar(max)', 'rqscop': 'varchar(max)', 'savbsk': 'varchar(max)', 'scrdir': 'varchar(max)', 'serarr': 'varchar(max)', 'serdsm': 'varchar(max)', 'sereor': 'varchar(max)', 'serfag': 'varchar(max)', 'sergwe': 'varchar(max)', 'serido': 'varchar(max)', 'seriiv': 'varchar(max)', 'serior': 'varchar(max)', 'serjrn': 'varchar(max)', 'seroiv': 'varchar(max)', 'serpil': 'varchar(max)', 'serpll': 'varchar(max)', 'serpor': 'varchar(max)', 'serprq': 'varchar(max)', 'serqac': 'varchar(max)', 'serreq': 'varchar(max)', 'serrpl': 'varchar(max)', 'serrqp': 'varchar(max)', 'sershp': 'varchar(max)', 'sersor': 'varchar(max)', 'serstf': 'varchar(max)', 'serstt': 'varchar(max)', 'serwor': 'varchar(max)', 'shwcap': 'varchar(max)', 'sordat': 'varchar(max)', 'srcsup': 'varchar(max)', 'suntyp': 'varchar(max)', 'svcdpt': 'varchar(max)', 'sw1sup': 'varchar(max)', 'tlxswt': 'varchar(max)', 'tsklvl': 'varchar(max)', 'txtdir': 'varchar(max)', 'umdlvl': 'varchar(max)', 'usegui': 'varchar(max)', 'usemks': 'varchar(max)', 'usetoo': 'varchar(max)', 'vatnum': 'varchar(max)', 'viadr1': 'varchar(max)', 'viadr2': 'varchar(max)', 'viadr3': 'varchar(max)', 'viadr4': 'varchar(max)', 'whsdat': 'varchar(max)', 'whssrc': 'varchar(max)', 'wordat': 'varchar(max)', 'wrthit': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['compny']
    ),
    start=start,
    cron="@daily",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_st_rainbow_par_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_st_rainbow_par (_data_modified_utc)"]
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
		'Rainbow_ST' as _source_catalog,
		CONVERT(varchar(max), annset, 126) AS annset,
		CAST(aprmst AS VARCHAR(MAX)) AS aprmst,
		CAST(bomlev AS VARCHAR(MAX)) AS bomlev,
		CAST(cadpgm AS VARCHAR(MAX)) AS cadpgm,
		CAST(cadrcv AS VARCHAR(MAX)) AS cadrcv,
		CAST(cadsnd AS VARCHAR(MAX)) AS cadsnd,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(clifac AS VARCHAR(MAX)) AS clifac,
		CAST(cmntrm AS VARCHAR(MAX)) AS cmntrm,
		CAST(cntcod AS VARCHAR(MAX)) AS cntcod,
		CAST(coltab AS VARCHAR(MAX)) AS coltab,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(crctrl AS VARCHAR(MAX)) AS crctrl,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(ctcprs AS VARCHAR(MAX)) AS ctcprs,
		CAST(curcod AS VARCHAR(MAX)) AS curcod,
		CAST(cwpsrc AS VARCHAR(MAX)) AS cwpsrc,
		CAST(digarr AS VARCHAR(MAX)) AS digarr,
		CAST(digdsm AS VARCHAR(MAX)) AS digdsm,
		CAST(digeor AS VARCHAR(MAX)) AS digeor,
		CAST(digfag AS VARCHAR(MAX)) AS digfag,
		CAST(diggwe AS VARCHAR(MAX)) AS diggwe,
		CAST(digido AS VARCHAR(MAX)) AS digido,
		CAST(digiiv AS VARCHAR(MAX)) AS digiiv,
		CAST(digior AS VARCHAR(MAX)) AS digior,
		CAST(digjrn AS VARCHAR(MAX)) AS digjrn,
		CAST(digoiv AS VARCHAR(MAX)) AS digoiv,
		CAST(digpil AS VARCHAR(MAX)) AS digpil,
		CAST(digpll AS VARCHAR(MAX)) AS digpll,
		CAST(digpor AS VARCHAR(MAX)) AS digpor,
		CAST(digprq AS VARCHAR(MAX)) AS digprq,
		CAST(digqac AS VARCHAR(MAX)) AS digqac,
		CAST(digreq AS VARCHAR(MAX)) AS digreq,
		CAST(digrpl AS VARCHAR(MAX)) AS digrpl,
		CAST(digrqp AS VARCHAR(MAX)) AS digrqp,
		CAST(digshp AS VARCHAR(MAX)) AS digshp,
		CAST(digsor AS VARCHAR(MAX)) AS digsor,
		CAST(digstf AS VARCHAR(MAX)) AS digstf,
		CAST(digstt AS VARCHAR(MAX)) AS digstt,
		CAST(digwor AS VARCHAR(MAX)) AS digwor,
		CAST(dladr1 AS VARCHAR(MAX)) AS dladr1,
		CAST(dladr2 AS VARCHAR(MAX)) AS dladr2,
		CAST(dladr3 AS VARCHAR(MAX)) AS dladr3,
		CAST(dladr4 AS VARCHAR(MAX)) AS dladr4,
		CAST(dmawhs AS VARCHAR(MAX)) AS dmawhs,
		CAST(docstr AS VARCHAR(MAX)) AS docstr,
		CAST(dqcwhs AS VARCHAR(MAX)) AS dqcwhs,
		CAST(drwdir AS VARCHAR(MAX)) AS drwdir,
		CAST(drwpgm AS VARCHAR(MAX)) AS drwpgm,
		CAST(ecland AS VARCHAR(MAX)) AS ecland,
		CAST(ecregn AS VARCHAR(MAX)) AS ecregn,
		CAST(emledt AS VARCHAR(MAX)) AS emledt,
		CAST(euscep AS VARCHAR(MAX)) AS euscep,
		CAST(euspyl AS VARCHAR(MAX)) AS euspyl,
		CAST(expdir AS VARCHAR(MAX)) AS expdir,
		CAST(faxswt AS VARCHAR(MAX)) AS faxswt,
		CAST(fctcod AS VARCHAR(MAX)) AS fctcod,
		CAST(fixdsm AS VARCHAR(MAX)) AS fixdsm,
		CAST(fixfac AS VARCHAR(MAX)) AS fixfac,
		CAST(fixfas AS VARCHAR(MAX)) AS fixfas,
		CAST(fixgwe AS VARCHAR(MAX)) AS fixgwe,
		CAST(fixido AS VARCHAR(MAX)) AS fixido,
		CAST(fixiiv AS VARCHAR(MAX)) AS fixiiv,
		CAST(fixior AS VARCHAR(MAX)) AS fixior,
		CAST(fixjrn AS VARCHAR(MAX)) AS fixjrn,
		CAST(fixoiv AS VARCHAR(MAX)) AS fixoiv,
		CAST(fixpil AS VARCHAR(MAX)) AS fixpil,
		CAST(fixpll AS VARCHAR(MAX)) AS fixpll,
		CAST(fixprq AS VARCHAR(MAX)) AS fixprq,
		CAST(fixqac AS VARCHAR(MAX)) AS fixqac,
		CAST(fixreq AS VARCHAR(MAX)) AS fixreq,
		CAST(fixrpl AS VARCHAR(MAX)) AS fixrpl,
		CAST(fixrqp AS VARCHAR(MAX)) AS fixrqp,
		CAST(fixshp AS VARCHAR(MAX)) AS fixshp,
		CAST(fixsor AS VARCHAR(MAX)) AS fixsor,
		CAST(fixstf AS VARCHAR(MAX)) AS fixstf,
		CAST(fixwor AS VARCHAR(MAX)) AS fixwor,
		CAST(flemnu AS VARCHAR(MAX)) AS flemnu,
		CAST(fnttab AS VARCHAR(MAX)) AS fnttab,
		CAST(frmnam AS VARCHAR(MAX)) AS frmnam,
		CAST(ftbexc AS VARCHAR(MAX)) AS ftbexc,
		CAST(ftcexc AS VARCHAR(MAX)) AS ftcexc,
		CAST(ftfexc AS VARCHAR(MAX)) AS ftfexc,
		CAST(fyoffs AS VARCHAR(MAX)) AS fyoffs,
		CAST(gtwcod AS VARCHAR(MAX)) AS gtwcod,
		CAST(higbnd AS VARCHAR(MAX)) AS higbnd,
		CAST(icscat AS VARCHAR(MAX)) AS icscat,
		CAST(icwhit AS VARCHAR(MAX)) AS icwhit,
		CAST(icworg AS VARCHAR(MAX)) AS icworg,
		CAST(iicgen AS VARCHAR(MAX)) AS iicgen,
		CAST(iivdoi AS VARCHAR(MAX)) AS iivdoi,
		CAST(ilglvl AS VARCHAR(MAX)) AS ilglvl,
		CAST(impdir AS VARCHAR(MAX)) AS impdir,
		CAST(infmks AS VARCHAR(MAX)) AS infmks,
		CAST(jobque AS VARCHAR(MAX)) AS jobque,
		CAST(lngcod AS VARCHAR(MAX)) AS lngcod,
		CAST(lowbnd AS VARCHAR(MAX)) AS lowbnd,
		CAST(lrnco1 AS VARCHAR(MAX)) AS lrnco1,
		CAST(lrnco2 AS VARCHAR(MAX)) AS lrnco2,
		CAST(lrnco3 AS VARCHAR(MAX)) AS lrnco3,
		CAST(lrncur AS VARCHAR(MAX)) AS lrncur,
		CAST(maadr1 AS VARCHAR(MAX)) AS maadr1,
		CAST(maadr2 AS VARCHAR(MAX)) AS maadr2,
		CAST(maadr3 AS VARCHAR(MAX)) AS maadr3,
		CAST(maadr4 AS VARCHAR(MAX)) AS maadr4,
		CAST(matadj AS VARCHAR(MAX)) AS matadj,
		CAST(mcptyp AS VARCHAR(MAX)) AS mcptyp,
		CAST(merppr AS VARCHAR(MAX)) AS merppr,
		CAST(mkrgrp AS VARCHAR(MAX)) AS mkrgrp,
		CAST(mltrpc AS VARCHAR(MAX)) AS mltrpc,
		CAST(mplcod AS VARCHAR(MAX)) AS mplcod,
		CAST(mplper AS VARCHAR(MAX)) AS mplper,
		CAST(mstslv AS VARCHAR(MAX)) AS mstslv,
		CAST(muntyp AS VARCHAR(MAX)) AS muntyp,
		CAST(ordmst AS VARCHAR(MAX)) AS ordmst,
		CAST(ordtrc AS VARCHAR(MAX)) AS ordtrc,
		CAST(otccod AS VARCHAR(MAX)) AS otccod,
		CAST(otmcod AS VARCHAR(MAX)) AS otmcod,
		CAST(otpcod AS VARCHAR(MAX)) AS otpcod,
		CAST(otscod AS VARCHAR(MAX)) AS otscod,
		CAST(otsrcv AS VARCHAR(MAX)) AS otsrcv,
		CAST(pbacty AS VARCHAR(MAX)) AS pbacty,
		CAST(phnfwd AS VARCHAR(MAX)) AS phnfwd,
		CAST(phnord AS VARCHAR(MAX)) AS phnord,
		CAST(phnprc AS VARCHAR(MAX)) AS phnprc,
		CAST(phnsvc AS VARCHAR(MAX)) AS phnsvc,
		CAST(phnswt AS VARCHAR(MAX)) AS phnswt,
		CAST(phnwhs AS VARCHAR(MAX)) AS phnwhs,
		CAST(picdir AS VARCHAR(MAX)) AS picdir,
		CONVERT(varchar(max), pordat, 126) AS pordat,
		CAST(pospor AS VARCHAR(MAX)) AS pospor,
		CAST(possor AS VARCHAR(MAX)) AS possor,
		CAST(poswhs AS VARCHAR(MAX)) AS poswhs,
		CAST(poswor AS VARCHAR(MAX)) AS poswor,
		CAST(prcbdg AS VARCHAR(MAX)) AS prcbdg,
		CAST(purinv AS VARCHAR(MAX)) AS purinv,
		CAST(ralscl AS VARCHAR(MAX)) AS ralscl,
		CAST(raltyp AS VARCHAR(MAX)) AS raltyp,
		CAST(regnum AS VARCHAR(MAX)) AS regnum,
		CAST(rqapop AS VARCHAR(MAX)) AS rqapop,
		CAST(rqscop AS VARCHAR(MAX)) AS rqscop,
		CAST(savbsk AS VARCHAR(MAX)) AS savbsk,
		CAST(scrdir AS VARCHAR(MAX)) AS scrdir,
		CAST(serarr AS VARCHAR(MAX)) AS serarr,
		CAST(serdsm AS VARCHAR(MAX)) AS serdsm,
		CAST(sereor AS VARCHAR(MAX)) AS sereor,
		CAST(serfag AS VARCHAR(MAX)) AS serfag,
		CAST(sergwe AS VARCHAR(MAX)) AS sergwe,
		CAST(serido AS VARCHAR(MAX)) AS serido,
		CAST(seriiv AS VARCHAR(MAX)) AS seriiv,
		CAST(serior AS VARCHAR(MAX)) AS serior,
		CAST(serjrn AS VARCHAR(MAX)) AS serjrn,
		CAST(seroiv AS VARCHAR(MAX)) AS seroiv,
		CAST(serpil AS VARCHAR(MAX)) AS serpil,
		CAST(serpll AS VARCHAR(MAX)) AS serpll,
		CAST(serpor AS VARCHAR(MAX)) AS serpor,
		CAST(serprq AS VARCHAR(MAX)) AS serprq,
		CAST(serqac AS VARCHAR(MAX)) AS serqac,
		CAST(serreq AS VARCHAR(MAX)) AS serreq,
		CAST(serrpl AS VARCHAR(MAX)) AS serrpl,
		CAST(serrqp AS VARCHAR(MAX)) AS serrqp,
		CAST(sershp AS VARCHAR(MAX)) AS sershp,
		CAST(sersor AS VARCHAR(MAX)) AS sersor,
		CAST(serstf AS VARCHAR(MAX)) AS serstf,
		CAST(serstt AS VARCHAR(MAX)) AS serstt,
		CAST(serwor AS VARCHAR(MAX)) AS serwor,
		CAST(shwcap AS VARCHAR(MAX)) AS shwcap,
		CONVERT(varchar(max), sordat, 126) AS sordat,
		CAST(srcsup AS VARCHAR(MAX)) AS srcsup,
		CAST(suntyp AS VARCHAR(MAX)) AS suntyp,
		CAST(svcdpt AS VARCHAR(MAX)) AS svcdpt,
		CAST(sw1sup AS VARCHAR(MAX)) AS sw1sup,
		CAST(tlxswt AS VARCHAR(MAX)) AS tlxswt,
		CAST(tsklvl AS VARCHAR(MAX)) AS tsklvl,
		CAST(txtdir AS VARCHAR(MAX)) AS txtdir,
		CAST(umdlvl AS VARCHAR(MAX)) AS umdlvl,
		CAST(usegui AS VARCHAR(MAX)) AS usegui,
		CAST(usemks AS VARCHAR(MAX)) AS usemks,
		CAST(usetoo AS VARCHAR(MAX)) AS usetoo,
		CAST(vatnum AS VARCHAR(MAX)) AS vatnum,
		CAST(viadr1 AS VARCHAR(MAX)) AS viadr1,
		CAST(viadr2 AS VARCHAR(MAX)) AS viadr2,
		CAST(viadr3 AS VARCHAR(MAX)) AS viadr3,
		CAST(viadr4 AS VARCHAR(MAX)) AS viadr4,
		CONVERT(varchar(max), whsdat, 126) AS whsdat,
		CAST(whssrc AS VARCHAR(MAX)) AS whssrc,
		CONVERT(varchar(max), wordat, 126) AS wordat,
		CAST(wrthit AS VARCHAR(MAX)) AS wrthit 
	FROM Rainbow_ST.rainbow.par
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    