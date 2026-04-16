CREATE OR REPLACE FUNCTION OFSMDM.FN_LOAD_OPS_RISK_DATA (
   P_V_BATCH_ID    VARCHAR2,
   P_V_MIS_DATE    VARCHAR2)
   RETURN NUMBER
AS
   CQD                        DATE := TO_DATE (P_V_MIS_DATE, 'YYYYMMDD');
   --ld_v_gl_code          VARCHAR2 (20 CHAR);
   --- ld_n_eop_bal          NUMBER (22, 3);
   --   ld_v_ccy_code         VARCHAR2 (3 CHAR);
   LD_V_ACCOUNT_NUMBER        VARCHAR2 (50 CHAR);
   LD_TBL                     VARCHAR2 (100);
   LD_EXCHANGE_RATE           NUMBER (15, 9);
   LD_V_BRANCH_CODE           VARCHAR2 (20);
   CNUMBER                    NUMBER;
   LN_TOTAL_DEDUCT            NUMBER (22, 3);
   LN_DEDUCITON_RATIO_1       NUMBER (22, 3);
   LN_DEDUCITON_RATIO_2       NUMBER (22, 3);
   CBA_DEDUCTION              NUMBER (22, 3);
   TOT1                       NUMBER (22, 3);
   TOT2                       NUMBER (22, 3);
   LN_DEDUCT_RATIO_ABLIBG_1   NUMBER (22, 3);
   LN_DEDUCT_RATIO_ABLIBG_2   NUMBER (22, 3);
   LN_TOTAL_DEDUCT_ABLIBG     NUMBER (22, 3);
   LN_SUB_TOTAL_ABLIBG        NUMBER (22, 3);
   LN_SQLCODE                 VARCHAR (200);
   LV_CURDATE                 DATE;
   LV_PROG_NAME               VARCHAR2 (64) := 'FN_LOAD_OPS_RISK_DATA';
   LV_STAGE                   NUMBER := 0;
   LV_MESSAGE_E               VARCHAR2 (2000) := 'FN_LOAD_OPS_RISK_DATA';
BEGIN
   DBMS_OUTPUT.PUT_LINE ('Hello');

   IF TO_NUMBER (EXTRACT (MONTH FROM TO_DATE (CQD, 'DD-MON-RR'))) = 12
   THEN
      --    INSERT INTO STG_OPS_RISK_DATA ORD (ORD.FIC_MIS_DATE,
      --                                      ORD.N_ALPHA_PERCENT,
      --                                      ORD.V_GAAP_CODE,
      --                                      ORD.D_FINANCIAL_YEAR,
      --                                      ORD.V_LOB_CODE,
      --                                      ORD.V_LV_CODE,
      --                                      ORD.V_CCY_CODE,
      --                                      ORD.V_DATA_PROCESSING_TYPE,
      --                                      ORD.N_ANNUAL_GROSS_INCOME)
      --        SELECT   --gl.V_GL_CODE,
      --                 CQD FIC_MIS_DATE,
      --                 0.15,
      --                 GL.V_GAAP_CODE,
      --                 GL.FIC_MIS_DATE D_FINANCIAL_YEAR,
      --                 M.V_LOB_CODE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 CASE WHEN GL.V_LV_CODE = 'ABL' THEN 'A' ELSE 'C' END,
      --                 SUM (GL.N_AMOUNT_LCY) AMT_LCY
      --          FROM      STG_GL_LOB_MAPPING M
      --                 INNER JOIN
      --                    STG_GL_DATA GL
      --                 ON GL.V_GL_CODE = M.V_GL_CODE
      --         WHERE  GL.FIC_MIS_DATE = CQD
      --                 AND M.V_LOB_CODE <> 'IRR'
      --      --  AND GL.V_LV_CODE = 'ABL'
      --      GROUP BY   GL.FIC_MIS_DATE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 GL.V_GAAP_CODE,
      --                 M.V_GL_TYPE,
      --                 M.V_LOB_CODE
      --      UNION
      --
      --      SELECT   --gl.V_GL_CODE,
      --                 CQD FIC_MIS_DATE,
      --                 0.15,
      --                 GL.V_GAAP_CODE,
      --                 GL.FIC_MIS_DATE D_FINANCIAL_YEAR,
      --                 M.V_LOB_CODE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 CASE WHEN GL.V_LV_CODE = 'ABL' THEN 'C' ELSE 'C' END,
      --                 SUM (GL.N_AMOUNT_LCY) AMT_LCY
      --          FROM      STG_GL_LOB_MAPPING M
      --                 INNER JOIN
      --                    STG_GL_DATA GL
      --                 ON GL.V_GL_CODE = M.V_GL_CODE
      --         WHERE  GL.FIC_MIS_DATE = CQD
      --                 AND M.V_LOB_CODE <> 'IRR'
      --      --  AND GL.V_LV_CODE = 'ABL'
      --      GROUP BY   GL.FIC_MIS_DATE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 GL.V_GAAP_CODE,
      --                 M.V_GL_TYPE,
      --                 M.V_LOB_CODE
      --
      --      UNION
      --        SELECT   CQD FIC_MIS_DATE,
      --                 0.15,
      --                 GL.V_GAAP_CODE,
      --                 GL.FIC_MIS_DATE D_FINANCIAL_YEAR,
      --                 'ABLOR' V_LOB_CODE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 CASE WHEN GL.V_LV_CODE = 'ABL' THEN 'A' ELSE 'C' END,
      --                 SUM (GL.N_AMOUNT_LCY) AMT_LCY
      --          FROM      STG_GL_LOB_MAPPING M
      --                 INNER JOIN
      --                    STG_GL_DATA GL
      --                 ON GL.V_GL_CODE = M.V_GL_CODE
      --         WHERE   GL.FIC_MIS_DATE = CQD
      --                 AND M.V_LOB_CODE <> 'IRR'
      --      --AND GL.V_LV_CODE = 'ABL'
      --      GROUP BY   GL.FIC_MIS_DATE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 GL.V_GAAP_CODE
      --
      --       -- ABLOR data for consolidation
      --
      --        UNION
      --        SELECT   CQD FIC_MIS_DATE,
      --                 0.15,
      --                 GL.V_GAAP_CODE,
      --                 GL.FIC_MIS_DATE D_FINANCIAL_YEAR,
      --                 'ABLOR' V_LOB_CODE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 CASE WHEN GL.V_LV_CODE = 'ABL' THEN 'C' ELSE 'C' END,
      --                 SUM (GL.N_AMOUNT_LCY) AMT_LCY
      --          FROM      STG_GL_LOB_MAPPING M
      --                 INNER JOIN
      --                    STG_GL_DATA GL
      --                 ON GL.V_GL_CODE = M.V_GL_CODE
      --         WHERE   GL.FIC_MIS_DATE = CQD
      --                 AND M.V_LOB_CODE <> 'IRR'
      --      --AND GL.V_LV_CODE = 'ABL'
      --      GROUP BY   GL.FIC_MIS_DATE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 GL.V_GAAP_CODE
      --
      --        /* Populate STG_OPS_RISK_DATA By inserting records from STG_GL_DATA for ABLIBG branches*/
      --      UNION
      --
      --      SELECT
      --                 CQD FIC_MIS_DATE,
      --                 0.15,
      --                 GD.V_GAAP_CODE,
      --                 GD.FIC_MIS_DATE D_FINANCIAL_YEAR,
      --                 V_LOB_CODE,
      --                 'ABLIBG',
      --                 GD.V_CCY_CODE,
      --                 CASE WHEN GD.V_LV_CODE = 'ABL' THEN 'A' ELSE 'C' END,
      --                 SUM(GD.N_AMOUNT_LCY) AMT_LCY
      --     FROM         OFSMDM.STG_GL_DATA GD
      --            INNER JOIN
      --               OFSERM.VW_JURISDICTION_BR_MAP VW
      --            ON GD.V_BRANCH_CODE = VW.V_BRANCH_CODE
      --         INNER JOIN
      --            OFSMDM.STG_GL_LOB_MAPPING GLM
      --         ON GD.V_GL_CODE = GLM.V_GL_CODE
      -- WHERE   GD.FIC_MIS_DATE = CQD AND GLM.V_LOB_CODE NOT IN  ('DBS','IRR')
      -- GROUP BY
      --      GLM.V_LOB_CODE,
      --      GD.V_LV_CODE,
      --      GD.V_GAAP_CODE,
      --      GD.FIC_MIS_DATE,
      --       GD.V_CCY_CODE
      --
      --      /* ABLOR LOB FOR ABLIBG*/
      --
      --      UNION
      --        SELECT   CQD FIC_MIS_DATE,
      --                 0.15,
      --                 GL.V_GAAP_CODE,
      --                 GL.FIC_MIS_DATE D_FINANCIAL_YEAR,
      --                 'ABLOR' V_LOB_CODE,
      --                 'ABLIBG',
      --                 GL.V_CCY_CODE,
      --                 CASE WHEN GL.V_LV_CODE = 'ABL' THEN 'A' ELSE 'C' END,
      --                 SUM (GL.N_AMOUNT_LCY) AMT_LCY
      --          FROM      STG_GL_LOB_MAPPING M
      --                 INNER JOIN
      --                    STG_GL_DATA GL
      --                 ON GL.V_GL_CODE = M.V_GL_CODE
      --                 INNER JOIN
      --                    OFSERM.VW_JURISDICTION_BR_MAP VJB
      --                 ON GL.V_BRANCH_CODE = VJB.V_BRANCH_CODE
      --         WHERE   GL.FIC_MIS_DATE = CQD
      --                 AND M.V_LOB_CODE <> 'IRR'
      --      --AND GL.V_LV_CODE = 'ABL'
      --      GROUP BY   GL.FIC_MIS_DATE,
      --                 GL.V_LV_CODE,
      --                 GL.V_CCY_CODE,
      --                 GL.V_GAAP_CODE;
      --
      --      COMMIT;

      /* -- NEW LOGIC OF OPERATIONAL RISK THROUGH MANUAL FORM/EXCEL UPLOAD (3 MARCH 2026) --- */

      DELETE FROM STG_OPS_RISK_DATA
            WHERE FIC_MIS_DATE = CQD;

      COMMIT;

      INSERT INTO STG_OPS_RISK_DATA ORD (ORD.FIC_MIS_DATE,
                                         ORD.N_ALPHA_PERCENT,
                                         ORD.V_GAAP_CODE,
                                         ORD.D_FINANCIAL_YEAR,
                                         ORD.V_LOB_CODE,
                                         ORD.V_LV_CODE,
                                         ORD.V_CCY_CODE,
                                         ORD.V_DATA_PROCESSING_TYPE,
                                         ORD.N_ANNUAL_GROSS_INCOME)
         SELECT CQD FIC_MIS_DATE,
                0.15,
                V_GAAP_CODE,
                D_FINANCIAL_YEAR,
                V_LOB_CODE,
                V_LV_CODE,
                V_CCY_CODE,
                V_DATA_PROCESSING_TYPE,
                TO_NUMBER(N_ANNUAL_GROSS_INCOME)
           FROM ABL_OPS_RISK_DATA M
          WHERE M.FIC_MIS_DATE = CQD;

      COMMIT;

      /* Deduction from CFI/RBA with ratio */

      SELECT ROUND (
                (  SUM (NVL (GLD.N_AMOUNT_LCY, 0))
                 * (MAX (LM.N_DEDUCTION_RATIO))),
                2),
             ROUND (
                (  SUM (NVL (GLD.N_AMOUNT_LCY, 0))
                 * (1 - MAX (LM.N_DEDUCTION_RATIO))),
                2)
        INTO LN_DEDUCITON_RATIO_1, LN_DEDUCITON_RATIO_2
        FROM STG_GL_DATA GLD, STG_GL_LOB_MAPPING LM
       WHERE     GLD.V_GL_CODE = LM.V_GL_CODE
             AND LM.V_LOB_CODE = 'DBS'
             AND LM.N_DEDUCTION_RATIO IS NOT NULL
             AND GLD.FIC_MIS_DATE = CQD
             AND GLD.V_LV_CODE = 'ABL';


      /* Deduction from CFI/RBA with ratio for ABLIBG*/

      SELECT ROUND (
                (  SUM (NVL (GLD.N_AMOUNT_LCY, 0))
                 * (MAX (LM.N_DEDUCTION_RATIO))),
                2),
             ROUND (
                (  SUM (NVL (GLD.N_AMOUNT_LCY, 0))
                 * (1 - MAX (LM.N_DEDUCTION_RATIO))),
                2)
        INTO LN_DEDUCT_RATIO_ABLIBG_1, LN_DEDUCT_RATIO_ABLIBG_2
        FROM STG_GL_DATA GLD,
             STG_GL_LOB_MAPPING LM,
             OFSERM.VW_JURISDICTION_BR_MAP VJBM
       WHERE     GLD.V_GL_CODE = LM.V_GL_CODE
             AND GLD.V_BRANCH_CODE = VJBM.V_BRANCH_CODE
             AND LM.V_LOB_CODE = 'DBS'
             AND LM.N_DEDUCTION_RATIO IS NOT NULL
             AND GLD.FIC_MIS_DATE = CQD
             AND GLD.V_LV_CODE = 'ABL';


      /* Deduction from CBA */


      SELECT SUM (GD.N_AMOUNT_ACY)
        INTO CBA_DEDUCTION
        FROM STG_GL_DATA GD
       WHERE     GD.FIC_MIS_DATE = CQD
             AND GD.V_LV_CODE = 'ABL'
             AND GD.V_GL_CODE IN
                    ('601010601-0000', '601010701-0000', '601010702-0000');

      SELECT ROUND (SUM (NVL (GLD.N_AMOUNT_LCY, 0)), 2)
        INTO LN_TOTAL_DEDUCT
        FROM STG_GL_DATA GLD, STG_GL_LOB_MAPPING LM
       WHERE     GLD.V_GL_CODE = LM.V_GL_CODE
             AND LM.V_LOB_CODE = 'DBS'
             AND LM.N_DEDUCTION_RATIO IS NOT NULL
             AND GLD.FIC_MIS_DATE = CQD
             AND GLD.V_LV_CODE = 'ABL';

      /*Total Deduction for ABLIBG*/

      SELECT ROUND (SUM (NVL (GLD.N_AMOUNT_LCY, 0)), 2)
        INTO LN_TOTAL_DEDUCT_ABLIBG
        FROM STG_GL_DATA GLD,
             STG_GL_LOB_MAPPING LM,
             OFSERM.VW_JURISDICTION_BR_MAP JBM
       WHERE     GLD.V_GL_CODE = LM.V_GL_CODE
             AND GLD.V_BRANCH_CODE = JBM.V_BRANCH_CODE
             AND LM.V_LOB_CODE = 'DBS'
             AND LM.N_DEDUCTION_RATIO IS NOT NULL
             AND GLD.FIC_MIS_DATE = CQD
             AND GLD.V_LV_CODE = 'ABL';

      DBMS_OUTPUT.PUT_LINE ('heelo 2');
      DBMS_OUTPUT.PUT_LINE (LN_TOTAL_DEDUCT + (-1 * LN_DEDUCITON_RATIO_1));
      -- DBMS_OUTPUT.PUT_LINE('ABLIBG Total Deduction' || LN_TOTAL_DEDUCT_ABLIBG + (-1 * LN_DEDUCT_RATIO_ABLIBG_1));

      TOT1 := LN_TOTAL_DEDUCT + (-1 * LN_DEDUCITON_RATIO_1);
      LN_SUB_TOTAL_ABLIBG :=
         LN_TOTAL_DEDUCT_ABLIBG + (-1 * LN_DEDUCT_RATIO_ABLIBG_1); /* Sub Total For ABLIBG*/

      UPDATE STG_OPS_RISK_DATA OPS
         SET OPS.N_ANNUAL_GROSS_INCOME =
                CASE
                   WHEN OPS.V_LOB_CODE = 'CBA'
                   THEN
                      NVL (OPS.N_ANNUAL_GROSS_INCOME + TOT1 + CBA_DEDUCTION,
                           0)
                   --  WHEN OPS.V_LOB_CODE = 'CFI' THEN NVL(OPS.N_ANNUAL_GROSS_INCOME,0) - LN_DEDUCITON_RATIO_2
                   WHEN OPS.V_LOB_CODE = 'RBA'
                   THEN
                        NVL (OPS.N_ANNUAL_GROSS_INCOME, 0)
                      + LN_DEDUCITON_RATIO_1
                END
       WHERE     OPS.FIC_MIS_DATE = CQD
             AND OPS.V_LOB_CODE IN ('CBA', 'RBA')
             AND OPS.V_LV_CODE <> 'ABLIBG';

      COMMIT;

      /*Update Value of CBA and RBA for ABLIBG*/
      UPDATE STG_OPS_RISK_DATA OPS
         SET OPS.N_ANNUAL_GROSS_INCOME =
                CASE
                   WHEN OPS.V_LOB_CODE = 'CBA'
                   THEN
                      NVL (OPS.N_ANNUAL_GROSS_INCOME + LN_SUB_TOTAL_ABLIBG,
                           0)
                   --  WHEN OPS.V_LOB_CODE = 'CFI' THEN NVL(OPS.N_ANNUAL_GROSS_INCOME,0) - LN_DEDUCITON_RATIO_2
                   WHEN OPS.V_LOB_CODE = 'RBA'
                   THEN
                        NVL (OPS.N_ANNUAL_GROSS_INCOME, 0)
                      + LN_DEDUCT_RATIO_ABLIBG_1
                END
       WHERE     OPS.FIC_MIS_DATE = CQD
             AND OPS.V_LOB_CODE IN ('CBA', 'RBA')
             AND OPS.V_LV_CODE = 'ABLIBG';

      COMMIT;
   END IF;

   CNUMBER := 1;
   RETURN CNUMBER;

   COMMIT;
EXCEPTION
   WHEN OTHERS
   THEN
      CNUMBER := 0;

      LN_SQLCODE := TO_CHAR (SQLCODE);
      LV_CURDATE := SYSDATE;
      LV_MESSAGE_E := CQD || '   ' || SQLERRM;

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
