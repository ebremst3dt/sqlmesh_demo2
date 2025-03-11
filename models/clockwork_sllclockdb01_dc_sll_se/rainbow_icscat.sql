
MODEL (
    kind VIEW
);

@UNION(
    'all',
    sllclockdb01_dc_sll_se.Rainbow_DS_rainbow_icscat,
	sllclockdb01_dc_sll_se.Rainbow_KS_rainbow_icscat,
	sllclockdb01_dc_sll_se.Rainbow_MD_rainbow_icscat,
	sllclockdb01_dc_sll_se.Rainbow_SLSO_rainbow_icscat,
	sllclockdb01_dc_sll_se.Rainbow_SOS_rainbow_icscat,
	sllclockdb01_dc_sll_se.Rainbow_ST_rainbow_icscat,
	sllclockdb01_dc_sll_se.Rainbow_TH_rainbow_icscat
)
        