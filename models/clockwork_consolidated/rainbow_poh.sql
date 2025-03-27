
MODEL (
    kind VIEW
);

@UNION(
    'all',
    clockwork_sllclockdb01_dc_sll_se.rainbow_ds_rainbow_poh,
	clockwork_sllclockdb01_dc_sll_se.rainbow_ks_rainbow_poh,
	clockwork_sllclockdb01_dc_sll_se.rainbow_md_rainbow_poh,
	clockwork_sllclockdb01_dc_sll_se.rainbow_slso_rainbow_poh,
	clockwork_sllclockdb01_dc_sll_se.rainbow_sos_rainbow_poh,
	clockwork_sllclockdb01_dc_sll_se.rainbow_st_rainbow_poh,
	clockwork_sllclockdb01_dc_sll_se.rainbow_th_rainbow_poh
)
        