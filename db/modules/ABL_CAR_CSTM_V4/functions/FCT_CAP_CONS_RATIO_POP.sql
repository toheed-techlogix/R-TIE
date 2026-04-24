-- =====================================================================
-- Task: FCT_CAP_CONS_RATIO_POP
-- Schema: OFSERM
-- Wrapped: 2026-04-23
-- Source: OFSAA execution log / metadata extraction
-- =====================================================================
CREATE OR REPLACE FUNCTION OFSERM.FCT_CAP_CONS_RATIO_POP(
    P_V_BATCH_ID         VARCHAR2,
    P_V_MIS_DATE         VARCHAR2,
    P_V_RUN_ID           VARCHAR2,
    P_V_PROCESS_ID       VARCHAR2,
    P_V_RUN_EXECUTION_ID VARCHAR2,
    P_N_RUN_SKEY         VARCHAR2,
    P_V_TASK_ID          VARCHAR2
) RETURN VARCHAR2 AUTHID CURRENT_USER AS

    ld_mis_date   DATE          := TO_DATE(P_V_MIS_DATE, 'YYYYMMDD');
    ln_mis_date   NUMBER        := TO_NUMBER(P_V_MIS_DATE);
    ln_run_skey   NUMBER(5)     := TO_NUMBER(SUBSTR(P_N_RUN_SKEY, 8, LENGTH(P_N_RUN_SKEY)));
    lv_run_id     VARCHAR2(64)  := SUBSTR(P_V_RUN_ID, 8, LENGTH(P_V_RUN_ID));

BEGIN

    insert /*+APPEND*/ into FCT_CAPITAL_CONSERVATION_RATIO(N_MIS_DATE_SKEY,N_RUN_SKEY,N_LOWER_LIMIT,N_UPPER_LIMIT,N_ENTITY_SKEY,N_MIN_CAP_CONS_RATIO,N_QUARTILE,V_CAPITAL_ADEQUACY_REGULATOR)SELECT  /*+PARALLEL (4)*/ to_char(DIM_DATES.N_DATE_SKEY),to_char(DIM_RUN.N_RUN_SKEY),CASE fsi_benchmark_cap_cons_ratio.n_quartile WHEN (MIN(fsi_benchmark_cap_cons_ratio.n_quartile) over()) THEN (CASE WHEN COALESCE(fct_standard_acct_head_cet.n_std_acct_head_amt, 0) < 0.045 THEN 0.045 ELSE COALESCE(fct_standard_acct_head_cet.n_std_acct_head_amt, 0) END) ELSE(0.000001 + ((fct_standard_acct_head_trb.n_std_acct_head_amt / ((MAX(fsi_benchmark_cap_cons_ratio.n_quartile) over()) - 1)) * (fsi_benchmark_cap_cons_ratio.n_quartile - 1)) + (CASE WHEN COALESCE(fct_standard_acct_head_cet.n_std_acct_head_amt, 0) < 0.045 THEN 0.045 ELSE COALESCE(fct_standard_acct_head_cet.n_std_acct_head_amt, 0) END)) END,CASE fsi_benchmark_cap_cons_ratio.n_quartile WHEN (MAX(fsi_benchmark_cap_cons_ratio.n_quartile) over()) THEN 100.00 ELSE ((fct_standard_acct_head_trb.n_std_acct_head_amt / ((MAX(fsi_benchmark_cap_cons_ratio.n_quartile) over()) - 1)) * fsi_benchmark_cap_cons_ratio.n_quartile) + (CASE WHEN COALESCE(fct_standard_acct_head_cet.n_std_acct_head_amt, 0) < 0.045 THEN 0.045 ELSE COALESCE(fct_standard_acct_head_cet.n_std_acct_head_amt, 0) END) END,to_char(FCT_ENTITY_INFO.N_ENTITY_SKEY),to_char(FSI_BENCHMARK_CAP_CONS_RATIO.N_MIN_CAP_CONS_RATIO),to_char(FSI_BENCHMARK_CAP_CONS_RATIO.N_QUARTILE),FSI_BENCHMARK_CAP_CONS_RATIO.V_CAPITAL_ADEQUACY_REGULATOR FROM fsi_benchmark_cap_cons_ratio LEFT JOIN dim_run ON dim_run.n_run_skey = '870' LEFT JOIN dim_dates ON dim_dates.d_calendar_date = to_date('20260331','YYYYMMDD') LEFT JOIN dim_standard_acct_head dim_cet ON dim_cet.v_std_acct_head_id = 'CAP820' AND to_date('20260331','YYYYMMDD') BETWEEN dim_cet.D_RECORD_START_DATE AND dim_cet.D_RECORD_END_DATE LEFT JOIN fct_standard_acct_head fct_standard_acct_head_cet ON fct_standard_acct_head_cet.n_std_acct_head_skey = dim_cet.n_std_acct_head_skey AND fct_standard_acct_head_cet.n_run_skey = dim_run.n_run_skey AND fct_standard_acct_head_cet.n_mis_date_skey = dim_dates.n_date_skey LEFT JOIN dim_standard_acct_head dim_trb ON dim_trb.v_std_acct_head_id = 'CAP829' AND to_date('20260331','YYYYMMDD') BETWEEN dim_trb.D_RECORD_START_DATE AND dim_trb.D_RECORD_END_DATE LEFT JOIN fct_standard_acct_head fct_standard_acct_head_trb ON fct_standard_acct_head_trb.n_std_acct_head_skey = dim_trb.n_std_acct_head_skey AND fct_standard_acct_head_trb.n_run_skey = dim_run.n_run_skey AND fct_standard_acct_head_trb.n_mis_date_skey = dim_dates.n_date_skey INNER JOIN fct_entity_info ON dim_run.n_run_skey = fct_entity_info.n_run_skey AND dim_dates.n_date_skey = fct_entity_info.n_mis_date_skey AND fct_standard_acct_head_cet.n_entity = fct_entity_info.n_entity_skey AND fct_standard_acct_head_trb.n_entity = fct_entity_info.n_entity_skey AND ( CASE ( SELECT dim_basel_consl_option_type.v_basel_consl_optn_type_code FROM dim_basel_consl_option_type WHERE dim_basel_consl_option_type.n_basel_consl_optn_type_skey = ( SELECT MAX(f.n_basel_consl_optn_type_skey) FROM fct_entity_info f WHERE f.n_run_skey = fct_entity_info.n_run_skey ) ) WHEN 'CONSL' THEN fct_entity_info.f_cap_consl_parent_entity_ind ELSE fct_entity_info.f_cap_consl_entity_ind END ) = 'Y' WHERE 1=1 AND DIM_DATES.D_CALENDAR_DATE = to_date('20260331','YYYYMMDD') AND DIM_RUN.N_RUN_SKEY = '870' AND FSI_BENCHMARK_CAP_CONS_RATIO.N_RECORD_SKEY > 0 AND DIM_DATES.D_CALENDAR_DATE BETWEEN FSI_BENCHMARK_CAP_CONS_RATIO.D_START_DATE AND FSI_BENCHMARK_CAP_CONS_RATIO.D_END_DATE LOG ERRORS INTO FCT_CAPITAL_CONSERVATION_RATI$ ('1776772087725_52113759_20260331_1_Job_2f96232c02714c3db135d8b58bc23d90') REJECT LIMIT 0;

    COMMIT;
    RETURN 'OK';

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RETURN 'FAIL: ' || SQLERRM;
END;
/
