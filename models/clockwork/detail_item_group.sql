MODEL (
  kind FULL
);

SELECT *, 'DS' as source
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_DS_rainbow_dig
UNION ALL
SELECT *, 'KS' as source
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_KS_rainbow_dig
UNION ALL
SELECT *, 'MD' as source
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_MD_rainbow_dig
UNION ALL
SELECT *, 'SLSO' as source
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_SLSO_rainbow_dig
UNION ALL
SELECT *, 'SOS' as source
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_SOS_rainbow_dig
UNION ALL
SELECT *, 'ST' as source
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_ST_rainbow_dig
UNION ALL
SELECT *, 'TH' as source
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_TH_rainbow_dig