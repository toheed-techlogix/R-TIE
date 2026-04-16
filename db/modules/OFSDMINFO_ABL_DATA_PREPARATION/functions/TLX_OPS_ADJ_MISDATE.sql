CREATE OR REPLACE FUNCTION OFSMDM.TLX_OPS_ADJ_MISDATE (
   P_V_BATCH_ID    VARCHAR2,
   P_V_MIS_DATE    VARCHAR2)
   RETURN NUMBER
AS
   CQD                    DATE := TO_DATE (P_V_MIS_DATE, 'YYYYMMDD');
   LD_V_GL_CODE           VARCHAR2 (20 CHAR);
   LD_N_EOP_BAL           NUMBER (22, 3);
   LD_V_CCY_CODE          VARCHAR2 (3 CHAR);
   LD_V_ACCOUNT_NUMBER    VARCHAR2 (50 CHAR);
   LD_TBL                 VARCHAR2 (100);
   LD_EXCHANGE_RATE       NUMBER (15, 9);
   LD_V_BRANCH_CODE       VARCHAR2 (20);
   CNUMBER                NUMBER;
   CQD_NUM                NUMBER;
   LN_TOTAL_DEDUCT        NUMBER (22, 3);
   LN_DEDUCITON_RATIO_1   NUMBER (22, 3);
   LN_DEDUCITON_RATIO_2   NUMBER (22, 3);
   CBA_DEDUCTION          NUMBER (22, 3);
   TOT1                   NUMBER (22, 3);
   TOT2                   NUMBER (22, 3);
   -- ld_curr_misdate       Date := EXTRACT(YEAR FROM TO_DATE(CQD, 'DD-MON-RR'));
   --  ld_2nd_prev_misadte   Date := EXTRACT(YEAR FROM CQD - 2);
   LD_T_MIS1              DATE;
   LD_T_MIS2              DATE;
   LN_COUNTER             NUMBER;
   LN_COUNTER_1           NUMBER;
   LD_CURR_MISDATE        DATE;
   --VARIABLES
   LN_SQLCODE             VARCHAR (200);
   LV_CURDATE             DATE;
   LV_PROG_NAME           VARCHAR2 (64) := 'TLX_OPS_ADJ_MISDATE';
   LV_STAGE               NUMBER := 0;
   LV_MESSAGE_E           VARCHAR2 (2000) := 'TLX_OPS_ADJ_MISDATE';
--VARIABLES
BEGIN
   DELETE FROM STG_OPS_ADJ_MISDATE_TLX OAMT;


   DELETE FROM STG_OPS_RISK_DATA SORD
         WHERE SORD.FIC_MIS_DATE = CQD;

   COMMIT;

   LV_STAGE := 1;

   IF TO_NUMBER (EXTRACT (MONTH FROM TO_DATE (CQD, 'DD-MON-RRRR'))) = 12 --CQD_NUM = 12 --
   THEN
      DBMS_OUTPUT.PUT_LINE ('line#: 40');
      LN_COUNTER := 1;

      WHILE LN_COUNTER <= 2
      LOOP
         INSERT INTO STG_OPS_ADJ_MISDATE_TLX OPT (
                        FIC_MIS_DATE,
                        D_FINANCIAL_YEAR,
                        V_LV_CODE,
                        V_LOB_CODE,
                        V_GAAP_CODE,
                        V_DATA_PROCESSING_TYPE,
                        N_NET_INTEREST_INCOME,
                        N_NET_NON_INT_INCOME,
                        N_OPERATING_EXPENSES,
                        N_INSURANCE_IRREGULAR_LOSS,
                        N_INSURANCE_IRREGULAR_GAIN,
                        N_PROVISION_AMOUNT,
                        N_LOANS_ADVANCES_AMT,
                        N_BETA_FACTOR,
                        N_WRITE_OFF_AMOUNT,
                        N_REV_PROV_AMT,
                        N_DISPOSAL_PROP_INCOME_AMT,
                        N_LEGAL_SETTLE_INCOME_AMT,
                        N_SECURITY_SALE_GAIN_LOSS_HTM,
                        N_SECURITY_SALE_GAIN_LOSS_AFS,
                        N_INCOME_INSURANCE_CLAIM,
                        N_REV_WRITE_OFF_AMT,
                        N_ALPHA_PERCENT,
                        N_FEE_INCOME,
                        N_IAH_SHARE_INCOME,
                        N_IJARAH_DEPRECIATION,
                        N_NET_INC_FINANCE_ACTIVITIES,
                        N_NET_INC_INVEST_ACTIVITIES,
                        N_EXTRAORDINARY_INCOME,
                        N_GAIN_LOSS_POS_NET_INCOME,
                        N_ANNUAL_GROSS_INCOME,
                        V_COUNTRY_CODE,
                        V_BRANCH_CODE,
                        V_RUN_IDENTIFIER_CODE,
                        V_CCY_CODE)
            SELECT FIC_MIS_DATE,
                   D_FINANCIAL_YEAR,
                   V_LV_CODE,
                   V_LOB_CODE,
                   V_GAAP_CODE,
                   V_DATA_PROCESSING_TYPE,
                   N_NET_INTEREST_INCOME,
                   N_NET_NON_INT_INCOME,
                   N_OPERATING_EXPENSES,
                   N_INSURANCE_IRREGULAR_LOSS,
                   N_INSURANCE_IRREGULAR_GAIN,
                   N_PROVISION_AMOUNT,
                   N_LOANS_ADVANCES_AMT,
                   N_BETA_FACTOR,
                   N_WRITE_OFF_AMOUNT,
                   N_REV_PROV_AMT,
                   N_DISPOSAL_PROP_INCOME_AMT,
                   N_LEGAL_SETTLE_INCOME_AMT,
                   N_SECURITY_SALE_GAIN_LOSS_HTM,
                   N_SECURITY_SALE_GAIN_LOSS_AFS,
                   N_INCOME_INSURANCE_CLAIM,
                   N_REV_WRITE_OFF_AMT,
                   N_ALPHA_PERCENT,
                   N_FEE_INCOME,
                   N_IAH_SHARE_INCOME,
                   N_IJARAH_DEPRECIATION,
                   N_NET_INC_FINANCE_ACTIVITIES,
                   N_NET_INC_INVEST_ACTIVITIES,
                   N_EXTRAORDINARY_INCOME,
                   N_GAIN_LOSS_POS_NET_INCOME,
                   N_ANNUAL_GROSS_INCOME,
                   V_COUNTRY_CODE,
                   V_BRANCH_CODE,
                   V_RUN_IDENTIFIER_CODE,
                   V_CCY_CODE
              FROM STG_OPS_RISK_DATA OPS
             WHERE     EXTRACT (
                          YEAR FROM TO_DATE (OPS.D_FINANCIAL_YEAR,
                                             'DD-MON-RR')) =
                            EXTRACT (YEAR FROM TO_DATE (CQD, 'DD-MON-RR'))
                          - LN_COUNTER
                   AND OPS.FIC_MIS_DATE = ADD_MONTHS (CQD, -12)
                   AND TO_NUMBER (
                          EXTRACT (
                             MONTH FROM TO_DATE (OPS.FIC_MIS_DATE,
                                                 'DD-MON-RR'))) NOT IN
                          (3, 6, 9); -- AND CQD > to_date('12312016','MMDDYYYY');

         DBMS_OUTPUT.PUT_LINE ('LOOP_1 SUCCESS LN_COUNTER: ' || LN_COUNTER);
         LN_COUNTER := LN_COUNTER + 1;
         
         
      /*  update  STG_OPS_ADJ_MISDATE_TLX
        SET D_PREVIOUS_DATE := ADD_MONTHS(CQD , -12 * LN_COUNTER)
        WHERE
        TO_DATE(ops.D_FINANCIAL_YEAR, 'DD-MON-RR')) = EXTRACT(YEAR FROM TO_DATE(CQD, 'DD-MON-RR')) - LN_COUNTER;
        commit;     */
        
        
      END LOOP;

      -- LV_STAGE:=2;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE (
         'Month: ' || EXTRACT (YEAR FROM TO_DATE (CQD, 'DD-MON-RR')));
      DBMS_OUTPUT.PUT_LINE ('Stage 2');
   ELSIF TO_NUMBER (EXTRACT (MONTH FROM TO_DATE (CQD, 'DD-MON-RR'))) <> 12
   THEN
      DBMS_OUTPUT.PUT_LINE ('line#: 41');
      LN_COUNTER_1 := 1;

      WHILE LN_COUNTER_1 <= 3
      LOOP
         DBMS_OUTPUT.PUT_LINE ('inner while loop');
         LV_STAGE := 2;
         DBMS_OUTPUT.PUT_LINE ('LV_STAGE 2');

         INSERT INTO STG_OPS_ADJ_MISDATE_TLX OPT (
                        FIC_MIS_DATE,
                        D_FINANCIAL_YEAR,
                        V_LV_CODE,
                        V_LOB_CODE,
                        V_GAAP_CODE,
                        V_DATA_PROCESSING_TYPE,
                        N_NET_INTEREST_INCOME,
                        N_NET_NON_INT_INCOME,
                        N_OPERATING_EXPENSES,
                        N_INSURANCE_IRREGULAR_LOSS,
                        N_INSURANCE_IRREGULAR_GAIN,
                        N_PROVISION_AMOUNT,
                        N_LOANS_ADVANCES_AMT,
                        N_BETA_FACTOR,
                        N_WRITE_OFF_AMOUNT,
                        N_REV_PROV_AMT,
                        N_DISPOSAL_PROP_INCOME_AMT,
                        N_LEGAL_SETTLE_INCOME_AMT,
                        N_SECURITY_SALE_GAIN_LOSS_HTM,
                        N_SECURITY_SALE_GAIN_LOSS_AFS,
                        N_INCOME_INSURANCE_CLAIM,
                        N_REV_WRITE_OFF_AMT,
                        N_ALPHA_PERCENT,
                        N_FEE_INCOME,
                        N_IAH_SHARE_INCOME,
                        N_IJARAH_DEPRECIATION,
                        N_NET_INC_FINANCE_ACTIVITIES,
                        N_NET_INC_INVEST_ACTIVITIES,
                        N_EXTRAORDINARY_INCOME,
                        N_GAIN_LOSS_POS_NET_INCOME,
                        N_ANNUAL_GROSS_INCOME,
                        V_COUNTRY_CODE,
                        V_BRANCH_CODE,
                        V_RUN_IDENTIFIER_CODE,
                        V_CCY_CODE)
            SELECT FIC_MIS_DATE,
                   D_FINANCIAL_YEAR,
                   V_LV_CODE,
                   V_LOB_CODE,
                   V_GAAP_CODE,
                   V_DATA_PROCESSING_TYPE,
                   N_NET_INTEREST_INCOME,
                   N_NET_NON_INT_INCOME,
                   N_OPERATING_EXPENSES,
                   N_INSURANCE_IRREGULAR_LOSS,
                   N_INSURANCE_IRREGULAR_GAIN,
                   N_PROVISION_AMOUNT,
                   N_LOANS_ADVANCES_AMT,
                   N_BETA_FACTOR,
                   N_WRITE_OFF_AMOUNT,
                   N_REV_PROV_AMT,
                   N_DISPOSAL_PROP_INCOME_AMT,
                   N_LEGAL_SETTLE_INCOME_AMT,
                   N_SECURITY_SALE_GAIN_LOSS_HTM,
                   N_SECURITY_SALE_GAIN_LOSS_AFS,
                   N_INCOME_INSURANCE_CLAIM,
                   N_REV_WRITE_OFF_AMT,
                   N_ALPHA_PERCENT,
                   N_FEE_INCOME,
                   N_IAH_SHARE_INCOME,
                   N_IJARAH_DEPRECIATION,
                   N_NET_INC_FINANCE_ACTIVITIES,
                   N_NET_INC_INVEST_ACTIVITIES,
                   N_EXTRAORDINARY_INCOME,
                   N_GAIN_LOSS_POS_NET_INCOME,
                   N_ANNUAL_GROSS_INCOME,
                   V_COUNTRY_CODE,
                   V_BRANCH_CODE,
                   V_RUN_IDENTIFIER_CODE,
                   V_CCY_CODE
              FROM STG_OPS_RISK_DATA OPS
             WHERE     EXTRACT (
                          YEAR FROM TO_DATE (OPS.D_FINANCIAL_YEAR,
                                             'DD-MON-RR')) =
                            EXTRACT (YEAR FROM TO_DATE (CQD, 'DD-MON-RR'))
                          - LN_COUNTER_1
                   AND CQD > TO_DATE ('12312016', 'MMDDYYYY')
                   AND OPS.FIC_MIS_DATE =
                          (SELECT MAX (FIC_MIS_DATE)
                             FROM STG_OPS_RISK_DATA
                            WHERE EXTRACT (
                                     YEAR FROM FIC_MIS_DATE) =
                                     EXTRACT (
                                        YEAR FROM ADD_MONTHS (
                                                     TO_DATE (CQD,
                                                              'DD-MON-RR'),
                                                     -12))); --AND OPS.D_FINANCIAL_YEAR > '31-dec-2015' ;

         DBMS_OUTPUT.PUT_LINE ('LOOP_2 SUCCESS LN_COUNTER: ' || LN_COUNTER_1);
         LN_COUNTER_1 := LN_COUNTER_1 + 1;
      END LOOP;

      LV_STAGE := 3;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE (
         'Month: ' || EXTRACT (YEAR FROM TO_DATE (CQD, 'DD-MON-RR')));
   END IF;

   DBMS_OUTPUT.PUT_LINE ('End conditions');
   LV_STAGE := 4;

   UPDATE STG_OPS_ADJ_MISDATE_TLX OPT
      SET OPT.FIC_MIS_DATE = CQD;

   COMMIT;
   DBMS_OUTPUT.PUT_LINE ('UPDATE SUCCESS');

   DBMS_OUTPUT.PUT_LINE ('line#: 72');
   LV_STAGE := 5;

   INSERT INTO STG_OPS_RISK_DATA (FIC_MIS_DATE,
                                  D_FINANCIAL_YEAR,
                                  V_LV_CODE,
                                  V_LOB_CODE,
                                  V_GAAP_CODE,
                                  V_DATA_PROCESSING_TYPE,
                                  N_NET_INTEREST_INCOME,
                                  N_NET_NON_INT_INCOME,
                                  N_OPERATING_EXPENSES,
                                  N_INSURANCE_IRREGULAR_LOSS,
                                  N_INSURANCE_IRREGULAR_GAIN,
                                  N_PROVISION_AMOUNT,
                                  N_LOANS_ADVANCES_AMT,
                                  N_BETA_FACTOR,
                                  N_WRITE_OFF_AMOUNT,
                                  N_REV_PROV_AMT,
                                  N_DISPOSAL_PROP_INCOME_AMT,
                                  N_LEGAL_SETTLE_INCOME_AMT,
                                  N_SECURITY_SALE_GAIN_LOSS_HTM,
                                  N_SECURITY_SALE_GAIN_LOSS_AFS,
                                  N_INCOME_INSURANCE_CLAIM,
                                  N_REV_WRITE_OFF_AMT,
                                  N_ALPHA_PERCENT,
                                  N_FEE_INCOME,
                                  N_IAH_SHARE_INCOME,
                                  N_IJARAH_DEPRECIATION,
                                  N_NET_INC_FINANCE_ACTIVITIES,
                                  N_NET_INC_INVEST_ACTIVITIES,
                                  N_EXTRAORDINARY_INCOME,
                                  N_GAIN_LOSS_POS_NET_INCOME,
                                  N_ANNUAL_GROSS_INCOME,
                                  V_COUNTRY_CODE,
                                  V_BRANCH_CODE,
                                  V_RUN_IDENTIFIER_CODE,
                                  V_CCY_CODE)
      SELECT FIC_MIS_DATE,
             D_FINANCIAL_YEAR,
             V_LV_CODE,
             V_LOB_CODE,
             V_GAAP_CODE,
             V_DATA_PROCESSING_TYPE,
             N_NET_INTEREST_INCOME,
             N_NET_NON_INT_INCOME,
             N_OPERATING_EXPENSES,
             N_INSURANCE_IRREGULAR_LOSS,
             N_INSURANCE_IRREGULAR_GAIN,
             N_PROVISION_AMOUNT,
             N_LOANS_ADVANCES_AMT,
             N_BETA_FACTOR,
             N_WRITE_OFF_AMOUNT,
             N_REV_PROV_AMT,
             N_DISPOSAL_PROP_INCOME_AMT,
             N_LEGAL_SETTLE_INCOME_AMT,
             N_SECURITY_SALE_GAIN_LOSS_HTM,
             N_SECURITY_SALE_GAIN_LOSS_AFS,
             N_INCOME_INSURANCE_CLAIM,
             N_REV_WRITE_OFF_AMT,
             N_ALPHA_PERCENT,
             N_FEE_INCOME,
             N_IAH_SHARE_INCOME,
             N_IJARAH_DEPRECIATION,
             N_NET_INC_FINANCE_ACTIVITIES,
             N_NET_INC_INVEST_ACTIVITIES,
             N_EXTRAORDINARY_INCOME,
             N_GAIN_LOSS_POS_NET_INCOME,
             N_ANNUAL_GROSS_INCOME,
             V_COUNTRY_CODE,
             V_BRANCH_CODE,
             V_RUN_IDENTIFIER_CODE,
             V_CCY_CODE
        FROM STG_OPS_ADJ_MISDATE_TLX OPS
       WHERE OPS.FIC_MIS_DATE = CQD;

   DBMS_OUTPUT.PUT_LINE ('INSERT SUCCESS');
   LV_STAGE := 6;
   COMMIT;

   CNUMBER := 1;
   RETURN CNUMBER;
   COMMIT;
EXCEPTION
   WHEN OTHERS
   THEN
      /*RAISE_APPLICATION_ERROR (
         -20001,
         'An error was encountered - ' || SQLCODE || ' -ERROR- ' || SQLERRM); */
      CNUMBER := 0;

      -- 4-April-2025 Rizwan
      LN_SQLCODE := TO_CHAR (SQLCODE);
      --LN_SQLCODE := TO_CHAR(v_sql);
      LV_CURDATE := SYSDATE;
      LV_MESSAGE_E := CQD || '   ' || SQLERRM;

      --LV_MESSAGE_E := SQLERRM || '    v_sql: ' || v_sql;



      INSERT INTO OFSDWH_ERROR_LOG (V_MAIN_PROG_NAME,
                                    V_STAGE_CODE,
                                    V_ERROR_DESC,
                                    D_DATETIME_STAMP)
           VALUES (LV_PROG_NAME,
                   LV_STAGE,
                   LV_MESSAGE_E,
                   SYSDATE);

      COMMIT;
      DBMS_OUTPUT.PUT_LINE ('Error!');

      RETURN CNUMBER;
END;
/
