
MODEL (
    kind VIEW
);

@UNION(
    'all',
    clockwork.sllclockdb01_dc_sll_se_Rainbow_DS_rainbow_mig,
	clockwork.sllclockdb01_dc_sll_se_Rainbow_KS_rainbow_mig,
	clockwork.sllclockdb01_dc_sll_se_Rainbow_MD_rainbow_mig,
	clockwork.sllclockdb01_dc_sll_se_Rainbow_SLSO_rainbow_mig,
	clockwork.sllclockdb01_dc_sll_se_Rainbow_SOS_rainbow_mig,
	clockwork.sllclockdb01_dc_sll_se_Rainbow_ST_rainbow_mig,
	clockwork.sllclockdb01_dc_sll_se_Rainbow_TH_rainbow_mig
)
        