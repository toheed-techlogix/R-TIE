CREATE OR REPLACE FUNCTION OFSMDM.POPULATE_STDACC_FROMGL (
   BATCHID   IN VARCHAR2,
   MISDATE   IN VARCHAR2)
   RETURN NUMBER
IS
   RESULT        NUMBER;
   LD_MIS_DATE   DATE := TO_DATE (MISDATE, 'YYYYMMDD');
BEGIN
   DBMS_OUTPUT.PUT_LINE ('Insert 1');


/* V_LV_CODE =BH will be part of ABL in SOLO run (chnages done on 31st MArch 2025  */

   UPDATE STG_GL_DATA
      SET V_LV_CODE = 'ABL'
    WHERE FIC_MIS_DATE = LD_MIS_DATE AND V_LV_CODE = 'BH';

   COMMIT;


   INSERT INTO STG_STANDARD_ACCT_HEAD B (B.FIC_MIS_DATE,
                                         B.F_CONSOLIDATION_FLAG,
                                         B.N_AMOUNT,
                                         B.N_AMOUNT_RCY,
                                         B.V_CCY_CODE,
                                         B.V_COUNTERPARTY_CODE,
                                         B.V_GAAP_CODE,
                                         B.V_LV_CODE,
                                         B.V_RCY_CODE,
                                         B.V_STD_ACCT_HEAD_ID)
        SELECT MAX (FIC_MIS_DATE) FIC_MIS_DATE,
               MAX (F_CONSOLIDATION_FLAG) F_CONSOLIDATION_FLAG,
               SUM (
                  CASE
                     /*WHEN V_STD_ACCT_HEAD_ID = 'ABL_CAP011'
                     THEN
                        nvl(N_AMOUNT,0) + nvl(AMT2,0)*/
                     WHEN V_STD_ACCT_HEAD_ID = 'ABL_CAP007'
                     THEN
                          N_AMOUNT
                        + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                             FROM STG_GL_DATA GL
                            WHERE     GL.V_GL_CODE LIKE '7%'
                                  AND GL.V_LV_CODE = 'ABL'
                                  AND GL.FIC_MIS_DATE = LD_MIS_DATE)
                        + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                             FROM STG_GL_DATA GL
                            WHERE     GL.V_GL_CODE LIKE '6%'
                                  AND GL.V_LV_CODE = 'ABL'
                                  AND GL.FIC_MIS_DATE = LD_MIS_DATE)
                     ----
                     WHEN V_STD_ACCT_HEAD_ID = 'AMC_CAP007'
                     THEN
                          N_AMOUNT
                        + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                             FROM STG_GL_DATA GL
                            WHERE     GL.V_GL_CODE LIKE '7%'
                                  AND GL.V_LV_CODE = 'AMC'
                                  AND GL.FIC_MIS_DATE = LD_MIS_DATE)
                        + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                             FROM STG_GL_DATA GL
                            WHERE     GL.V_GL_CODE LIKE '6%'
                                  AND GL.V_LV_CODE = 'AMC'
                                  AND GL.FIC_MIS_DATE = LD_MIS_DATE)
                     ----
                     ELSE
                        NVL (N_AMOUNT, 0) + NVL (AMT2, 0)
                  END)
                  N_AMOUNT,
               SUM (
                  CASE
                     /*WHEN V_STD_ACCT_HEAD_ID = 'ABL_CAP011'
                     THEN
                        nvl(N_AMOUNT_RCY,0 ) + nvl(AMT2,0)*/
                     WHEN V_STD_ACCT_HEAD_ID = 'ABL_CAP007'
                     THEN
                          N_AMOUNT_RCY
                        + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                             FROM STG_GL_DATA GL
                            WHERE     GL.V_GL_CODE LIKE '7%'
                                  AND GL.V_LV_CODE = 'ABL'
                                  AND GL.FIC_MIS_DATE = LD_MIS_DATE)
                        + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                             FROM STG_GL_DATA GL
                            WHERE     GL.V_GL_CODE LIKE '6%'
                                  AND GL.V_LV_CODE = 'ABL'
                                  AND GL.FIC_MIS_DATE = LD_MIS_DATE)
                     ---

                     WHEN V_STD_ACCT_HEAD_ID = 'AMC_CAP007'
                     THEN
                          N_AMOUNT_RCY
                        + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                             FROM STG_GL_DATA GL
                            WHERE     GL.V_GL_CODE LIKE '7%'
                                  AND GL.V_LV_CODE = 'AMC'
                                  AND GL.FIC_MIS_DATE = LD_MIS_DATE)
                        + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                             FROM STG_GL_DATA GL
                            WHERE     GL.V_GL_CODE LIKE '6%'
                                  AND GL.V_LV_CODE = 'AMC'
                                  AND GL.FIC_MIS_DATE = LD_MIS_DATE)
                     ---
                     ELSE
                        NVL (N_AMOUNT, 0) + NVL (AMT2, 0)
                  END)
                  N_AMOUNT_RCY,
               MAX (NVL (V_CCY_CODE, 'PKR')) V_CCY_CODE,
               MAX (V_COUNTERPARTY_CODE) V_COUNTERPARTY_CODE,
               MAX (V_GAAP_CODE) V_GAAP_CODE,
               MAX (V_LV_CODE) V_LV_CODE,
               MAX (NVL (V_RCY_CODE, 'PKR')) V_RCY_CODE,
               V_STD_ACCT_HEAD_ID
          FROM (  SELECT MAX (
                            CASE
                               WHEN (N_BALANCE) IS NOT NULL THEN LD_MIS_DATE
                               ELSE GL.FIC_MIS_DATE
                            END)
                            FIC_MIS_DATE,
                         'Y' F_CONSOLIDATION_FLAG,
                         SUM (GL.N_AMOUNT_LCY) N_AMOUNT,
                         SUM (GL.N_AMOUNT_LCY) N_AMOUNT_RCY,
                         GL.V_CCY_CODE V_CCY_CODE,
                         NULL V_COUNTERPARTY_CODE,
                         NVL (GL.V_GAAP_CODE, 'PKGAAP') V_GAAP_CODE,
                         BNKCAP.V_LV_CODE V_LV_CODE,
                         GL.V_CCY_CODE V_RCY_CODE,
                         BNKCAP.V_CAP_HEAD_CD V_STD_ACCT_HEAD_ID,
                         SUM (BNKCAP.N_BALANCE) AMT2
                    /*(SELECT   SUM (n_balance)
                       FROM   SETUP_BANK_CAPITAL_DTL
                      WHERE   V_CAP_HEAD_CD=BNKCAP.V_CAP_HEAD_CD and v_lv_code=BNKCAP.V_LV_CODE
                      --v_from_gl_code LIKE '%ABL%'
                      ) AMT2 */
                    FROM    SETUP_BANK_CAPITAL_DTL BNKCAP
                         LEFT JOIN
                            STG_GL_DATA GL
                         ON     GL.V_GL_CODE BETWEEN BNKCAP.V_FROM_GL_CODE
                                                 AND BNKCAP.V_TO_GL_CODE
                            AND (    GL.FIC_MIS_DATE = LD_MIS_DATE
                                 AND BNKCAP.V_LV_CODE = GL.V_LV_CODE)
                   WHERE     BNKCAP.V_FROM_GL_CODE != ' '
                         AND BNKCAP.V_TO_GL_CODE != ' '
                GROUP BY BNKCAP.V_CAP_HEAD_CD,
                         GL.V_CCY_CODE,
                         GL.V_GAAP_CODE,
                         BNKCAP.V_LV_CODE,
                         GL.FIC_MIS_DATE)
         WHERE FIC_MIS_DATE = LD_MIS_DATE
      GROUP BY V_STD_ACCT_HEAD_ID;

   COMMIT;


   DBMS_OUTPUT.PUT_LINE ('Insert 2');


   --------------------------------LOADING CAPITAL AMOUNTS  OF ABLIBG----------------------
   INSERT INTO STG_STANDARD_ACCT_HEAD B (B.FIC_MIS_DATE,
                                         B.F_CONSOLIDATION_FLAG,
                                         B.N_AMOUNT,
                                         B.N_AMOUNT_RCY,
                                         B.V_CCY_CODE,
                                         B.V_COUNTERPARTY_CODE,
                                         B.V_GAAP_CODE,
                                         B.V_LV_CODE,
                                         B.V_RCY_CODE,
                                         B.V_STD_ACCT_HEAD_ID)
        SELECT GL.FIC_MIS_DATE FIC_MIS_DATE,
               'Y' F_CONSOLIDATION_FLAG,
               (CASE
                   WHEN BNKCAP.V_CAP_HEAD_CD = 'ABL_CAP007'
                   THEN
                        SUM (NVL (GL.N_AMOUNT_LCY, 0))
                      + (  (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                              FROM STG_GL_DATA GL
                             WHERE     GL.V_GL_CODE LIKE '7%'
                                   AND GL.V_LV_CODE = 'ABL'
                                   AND GL.FIC_MIS_DATE = LD_MIS_DATE
                                   AND GL.V_BRANCH_CODE LIKE 'PK002%')
                         + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                              FROM STG_GL_DATA GL
                             WHERE     GL.V_GL_CODE LIKE '6%'
                                   AND GL.V_LV_CODE = 'ABL'
                                   AND GL.FIC_MIS_DATE = LD_MIS_DATE
                                   AND GL.V_BRANCH_CODE LIKE 'PK002%'))
                   ELSE
                      SUM (NVL (GL.N_AMOUNT_LCY, 0))
                END)
                  N_AMOUNT,
               (CASE
                   WHEN BNKCAP.V_CAP_HEAD_CD = 'ABL_CAP007'
                   THEN
                        SUM (NVL (GL.N_AMOUNT_LCY, 0))
                      + (  (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                              FROM STG_GL_DATA GL
                             WHERE     GL.V_GL_CODE LIKE '7%'
                                   AND GL.V_LV_CODE = 'ABL'
                                   AND GL.FIC_MIS_DATE = LD_MIS_DATE
                                   AND GL.V_BRANCH_CODE LIKE 'PK002%')
                         + (SELECT SUM (NVL (N_AMOUNT_LCY, 0))
                              FROM STG_GL_DATA GL
                             WHERE     GL.V_GL_CODE LIKE '6%'
                                   AND GL.V_LV_CODE = 'ABL'
                                   AND GL.FIC_MIS_DATE = LD_MIS_DATE
                                   AND GL.V_BRANCH_CODE LIKE 'PK002%'))
                   ELSE
                      SUM (NVL (GL.N_AMOUNT_LCY, 0))
                END)
                  N_AMOUNT_RCY,
               GL.V_CCY_CODE V_CCY_CODE,
               NULL V_COUNTERPARTY_CODE,
               NVL (GL.V_GAAP_CODE, 'PKGAAP') V_GAAP_CODE,
               'ABLIBG' V_LV_CODE,
               GL.V_CCY_CODE V_RCY_CODE,
               --BNKCAP.V_CAP_HEAD_CD V_STD_ACCT_HEAD_ID
               'ABLIBG' || SUBSTR (BNKCAP.V_CAP_HEAD_CD, 4) V_STD_ACCT_HEAD_ID
          FROM SETUP_BANK_CAPITAL_DTL BNKCAP
               LEFT JOIN STG_GL_DATA GL
                  ON     GL.V_GL_CODE BETWEEN BNKCAP.V_FROM_GL_CODE
                                          AND BNKCAP.V_TO_GL_CODE
                     AND (    GL.FIC_MIS_DATE = LD_MIS_DATE
                          AND BNKCAP.V_LV_CODE = GL.V_LV_CODE)
               INNER JOIN (SELECT DISTINCT V_BRANCH_CODE
                             FROM STG_GEOGRAPHY_MASTER
                            WHERE NVL (V_BRANCH_TYPE, 'N') = 'I') G
                  ON G.V_BRANCH_CODE = GL.V_BRANCH_CODE
         WHERE BNKCAP.V_FROM_GL_CODE != ' ' AND BNKCAP.V_TO_GL_CODE != ' '
      GROUP BY GL.FIC_MIS_DATE,
               GL.V_CCY_CODE,
               GL.V_GAAP_CODE,
               BNKCAP.V_CAP_HEAD_CD;

   COMMIT;


   RETURN 1;
EXCEPTION
   WHEN OTHERS
   THEN
      RAISE_APPLICATION_ERROR (
         -20001,
         'An error was encountered - ' || SQLCODE || ' -ERROR- ' || SQLERRM);
      --RESULT=0;
      RETURN 0;
END POPULATE_STDACC_FROMGL;
/
