
MODEL (
    kind VIEW
);

@UNION(
    'all',
    clockwork_sllclockdb01_dc_sll_se.Rainbow_DS_rainbow_obj,
	clockwork_sllclockdb01_dc_sll_se.Rainbow_KS_rainbow_obj,
	clockwork_sllclockdb01_dc_sll_se.Rainbow_MD_rainbow_obj,
	clockwork_sllclockdb01_dc_sll_se.Rainbow_SLSO_rainbow_obj,
	clockwork_sllclockdb01_dc_sll_se.Rainbow_SOS_rainbow_obj,
	clockwork_sllclockdb01_dc_sll_se.Rainbow_ST_rainbow_obj,
	clockwork_sllclockdb01_dc_sll_se.Rainbow_TH_rainbow_obj
)
        