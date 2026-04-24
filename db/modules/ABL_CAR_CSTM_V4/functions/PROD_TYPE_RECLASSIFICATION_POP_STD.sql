-- =====================================================================
-- Task: PROD_TYPE_RECLASSIFICATION_POP_STD
-- Schema: OFSERM
-- Wrapped: 2026-04-23
-- Source: OFSAA execution log / metadata extraction
-- =====================================================================
CREATE OR REPLACE FUNCTION OFSERM.PROD_TYPE_RECLASSIFICATION_POP_STD(
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

    insert /*+APPEND*/ into FSI_REG_PROD_TYPE_RECLASS(N_MIS_DATE_SKEY,N_PROD_SKEY,N_RUN_SKEY,V_APPROACH)SELECT  /*+PARALLEL (4)*/ to_char(DIM_DATES.N_DATE_SKEY),to_char(DIM_PRODUCT.N_PROD_SKEY),to_char(DIM_RUN.N_RUN_SKEY),'STD' FROM DIM_PRODUCT inner join DIM_DATES on 1=1 inner join DIM_RUN on 1=1 WHERE 1=1 AND DIM_DATES.d_calendar_date BETWEEN coalesce(DIM_PRODUCT.d_record_start_date, DIM_DATES.d_calendar_date) AND coalesce(DIM_PRODUCT.d_record_end_date, DIM_DATES.d_calendar_date) and dim_dates.d_calendar_date=to_date('20260331','YYYYMMDD') and dim_run.n_run_skey='870' LOG ERRORS INTO FSI_REG_PROD_TYPE_RECLASS$ ('1776767914307_95929133_20260331_1_Job_41ada0663c7a40f5b23b921cf5fe2fe9') REJECT LIMIT 50;

    COMMIT;
    RETURN 'OK';

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RETURN 'FAIL: ' || SQLERRM;
END;
/
