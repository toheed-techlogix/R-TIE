-- =====================================================================
-- Task: CS_Minimum_Capital_Conservation_Ratio
-- Schema: OFSERM
-- Wrapped: 2026-04-23
-- Source: OFSAA execution log / metadata extraction
-- =====================================================================
CREATE OR REPLACE FUNCTION OFSERM.CS_Minimum_Capital_Conservation_Ratio(
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

    MERGE  INTO FCT_STANDARD_ACCT_HEAD TT USING (   SELECT  /*+ PARALLEL(4) */ FCT_STANDARD_ACCT_HEAD.N_RUN_SKEY,FCT_STANDARD_ACCT_HEAD.N_STD_ACCT_HEAD_SKEY,FCT_STANDARD_ACCT_HEAD.N_MIS_DATE_SKEY,FCT_STANDARD_ACCT_HEAD.N_ENTITY,FCT_STANDARD_ACCT_HEAD.N_CAP_COMP_GROUP_SKEY,FCT_STANDARD_ACCT_HEAD.N_GAAP_SKEY,FCT_STANDARD_ACCT_HEAD.N_FORECAST_DATE_SKEY, MIN(FCT_STANDARD_ACCT_HEAD.N_STD_ACCT_HEAD_AMT) AS  T_1347582247765_0,   MIN(CASE  WHEN  ( ((DIM_STANDARD_ACCT_HEAD.V_STD_ACCT_HEAD_ID  = 'CAP836')) ) THEN 10 ELSE 11 END)  AS COND_1347582247765_10 ,  (COALESCE(MAX(CASE WHEN 1 = 1 THEN FCT_CAPITAL_CONSERVATION_RATIO.n_min_cap_cons_ratio ELSE NULL END ),1)) AS EXP_1347582247765_10,MIN(FCT_STANDARD_ACCT_HEAD.N_STD_ACCT_HEAD_AMT) AS EXP_1347582247765_11  FROM  fct_standard_acct_head INNER JOIN DIM_DATES ON fct_standard_acct_head.n_mis_date_skey = DIM_DATES.n_date_skey INNER JOIN dim_standard_acct_head ON fct_standard_acct_head.n_std_acct_head_skey = dim_standard_acct_head.n_std_acct_head_skey INNER JOIN fct_standard_acct_head capital_accounting ON fct_standard_acct_head.n_run_skey = capital_accounting.n_run_skey AND FCT_STANDARD_ACCT_HEAD.n_mis_date_skey = CAPITAL_ACCOUNTING.n_mis_date_skey AND fct_standard_acct_head.n_gaap_skey = capital_accounting.n_gaap_skey INNER JOIN dim_standard_acct_head dim_capital_accounting ON capital_accounting.n_std_acct_head_skey = DIM_CAPITAL_ACCOUNTING.n_std_acct_head_skey LEFT OUTER JOIN fct_capital_conservation_ratio ON fct_capital_conservation_ratio.n_run_skey = capital_accounting.n_run_skey AND fct_capital_conservation_ratio.n_mis_date_skey = CAPITAL_ACCOUNTING.n_mis_date_skey AND capital_accounting.n_std_acct_head_amt BETWEEN fct_capital_conservation_ratio.n_lower_limit AND fct_capital_conservation_ratio.n_upper_limit  WHERE (1=1)  AND (DIM_DATES.d_calendar_date=TO_DATE('20260331','yyyymmdd') AND fct_standard_acct_head.n_run_skey = '870' and dim_capital_accounting.v_std_acct_head_id = 'CAP837')  AND ( (((DIM_STANDARD_ACCT_HEAD.V_STD_ACCT_HEAD_ID  = 'CAP836')))  )  GROUP BY FCT_STANDARD_ACCT_HEAD.N_RUN_SKEY,FCT_STANDARD_ACCT_HEAD.N_STD_ACCT_HEAD_SKEY,FCT_STANDARD_ACCT_HEAD.N_MIS_DATE_SKEY,FCT_STANDARD_ACCT_HEAD.N_ENTITY,FCT_STANDARD_ACCT_HEAD.N_CAP_COMP_GROUP_SKEY,FCT_STANDARD_ACCT_HEAD.N_GAAP_SKEY,FCT_STANDARD_ACCT_HEAD.N_FORECAST_DATE_SKEY ) SS ON ( TT.N_RUN_SKEY= SS.N_RUN_SKEY AND TT.N_STD_ACCT_HEAD_SKEY= SS.N_STD_ACCT_HEAD_SKEY AND TT.N_MIS_DATE_SKEY= SS.N_MIS_DATE_SKEY AND TT.N_ENTITY= SS.N_ENTITY AND TT.N_CAP_COMP_GROUP_SKEY= SS.N_CAP_COMP_GROUP_SKEY AND TT.N_GAAP_SKEY= SS.N_GAAP_SKEY AND TT.N_FORECAST_DATE_SKEY= SS.N_FORECAST_DATE_SKEY) WHEN MATCHED THEN UPDATE SET  TT.N_STD_ACCT_HEAD_AMT=  CASE  WHEN  COND_1347582247765_10=10 THEN EXP_1347582247765_10 ELSE EXP_1347582247765_11 END;

    COMMIT;
    RETURN 'OK';

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RETURN 'FAIL: ' || SQLERRM;
END;
/
