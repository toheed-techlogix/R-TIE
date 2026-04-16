CREATE OR REPLACE FUNCTION OFSMDM.POPULATE_GL_FROMGLBAL (
   BatchId                IN VARCHAR2,
   MisDate                IN VARCHAR2
)
   RETURN NUMBER
IS
   result        NUMBER;
   ld_mis_date   DATE := TO_DATE (MisDate, 'YYYYMMDD');
BEGIN
   INSERT INTO STG_GL_DATA B (b.V_GL_CODE,
                              b.FIC_MIS_DATE,
                              b.V_DATA_ORIGIN,
                              b.D_DWNLD_DATE,
                              b.V_LV_CODE,
                              b.V_BRANCH_CODE,
                              b.V_SCENARIO_CODE,
                              b.V_CCY_CODE,
                              b.V_PROD_CODE,
                              b.V_FINANCIAL_ELEMENT_CODE,
                              b.V_COMMON_COA_CODE,
                              b.V_GL_TYPE,
                              b.N_AMOUNT_LCY,
                              b.N_AMOUNT_ACY,
                              b.N_AMOUNT_YTD_LCY,
                              b.N_AMOUNT_YTD_ACY,
                              b.N_AMOUNT_MTD_LCY,
                              b.N_AMOUNT_MTD_ACY,
                              b.N_MOVEMENT_MTD_LCY,
                              b.N_MOVEMENT_MTD_ACY,
                              b.N_MOVEMENT_YTD_LCY,
                              b.N_MOVEMENT_YTD_ACY,
                              b.F_CONSOLIDATION_FLAG,
                              b.V_GAAP_CODE,
                              b.N_MINORITY_INTEREST_AMT,
                              b.V_BUSINESS_UNIT_CODE,
                              b.V_CLASS_CODE,
                              b.V_ORG_UNIT_CODE,
                              b.N_LIQUIDATION_DAYS)
      SELECT   V_OF_GL_CODE,
               ld_mis_date FIC_MIS_DATE,
               'MAN-GL' V_DATA_ORIGIN,
               NULL D_DWNLD_DATE,
               V_LV_CODE,
               'PK0035001' V_BRANCH_CODE,
               '0' V_SCENARIO_CODE,
               V_CCY_CODE,
               'TEST' V_PROD_CODE,
               '0' V_FINANCIAL_ELEMENT_CODE,
               'TEMP' V_COMMON_COA_CODE,
               NULL V_GL_TYPE,
               sum(N_GL_BALANCE),
               NULL N_AMOUNT_ACY,
               NULL N_AMOUNT_YTD_LCY,
               NULL N_AMOUNT_YTD_ACY,
               NULL N_AMOUNT_MTD_LCY,
               NULL N_AMOUNT_MTD_ACY,
               NULL N_MOVEMENT_MTD_LCY,
               NULL N_MOVEMENT_MTD_ACY,
               NULL N_MOVEMENT_YTD_LCY,
               NULL N_MOVEMENT_YTD_ACY,
               'F' F_CONSOLIDATION_FLAG,
               'PKGAAP' V_GAAP_CODE,
               NULL N_MINORITY_INTEREST_AMT,
               NULL V_BUSINESS_UNIT_CODE,
               NULL V_CLASS_CODE,
               '0' V_ORG_UNIT_CODE,
               NULL N_LIQUIDATION_DAYS
        FROM   stg_gl_balances
        where FIC_MIS_DATE = ld_mis_date
        group by V_OF_GL_CODE,
                FIC_MIS_DATE,
               V_LV_CODE,
               V_CCY_CODE;



   COMMIT;

   RETURN 1;
EXCEPTION
   WHEN OTHERS
   THEN
      raise_application_error (
         -20001,
         'An error was encountered - ' || SQLCODE || ' -ERROR- ' || SQLERRM
      );
      --RESULT=0;
      RETURN 0;
END POPULATE_GL_FROMGLBAL;
/
