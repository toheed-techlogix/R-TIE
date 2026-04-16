CREATE OR REPLACE FUNCTION OFSMDM.POPULATE_PP_FROMGL (BATCHID   IN VARCHAR2,
                                                      MISDATE   IN VARCHAR2)
   RETURN NUMBER
IS
   RESULT                 NUMBER;
   LD_MIS_DATE            DATE := TO_DATE (MISDATE, 'YYYYMMDD');
   LN_GL_AMOUNT           NUMBER;
   LN_20_MN_LIMIT         NUMBER := 20000000;
   LN_EXP_AMOUNT          NUMBER;
   LN_SIGN                NUMBER;
   LN_CNT                 NUMBER := 0;
   LN_HR_PROV             NUMBER;
   LN_CFI_PROV            NUMBER;
   LN_CC_PROV             NUMBER;
   LN_HR_EXP              NUMBER;
   LN_CFI_EXP             NUMBER;
   LN_CC_EXP              NUMBER;
   LN_PROV_UPD            NUMBER;
   LV_PROD_CODE           VARCHAR2 (100);
   LV_CUST_REF_CODE       VARCHAR2 (100);
   LV_LOB_CODE            VARCHAR2 (100);
   LV_EXP_CATEGORY_CODE   VARCHAR2 (100);
   LV_CHK_ORIG_ACCT_NO    VARCHAR2 (100);
BEGIN
   --for updating  abl_guarantee_eids accounts to pp

   MERGE INTO STG_PRODUCT_PROCESSOR TRG
        USING (SELECT DISTINCT
                      SPP.V_ORIG_ACCT_NO,
                      V_ORIGNAL_ACCOUNT_NO,
                      SPP.V_PROD_CODE
                 FROM    STG_PRODUCT_PROCESSOR SPP
                      INNER JOIN
                         ABL_GUARANTEE_EIDS ABL
                      ON SPP.V_ORIG_ACCT_NO = ABL.V_ORIGNAL_ACCOUNT_NO
                WHERE V_PROD_CODE <> 'ABL-Gurnte') SRC
           ON (SRC.V_ORIG_ACCT_NO = TRG.V_ORIG_ACCT_NO)
   WHEN MATCHED
   THEN
      UPDATE SET TRG.V_PROD_CODE = 'ABL-Gurnte';

   COMMIT;

   --for blocking prod codes in pp
   MERGE INTO STG_PRODUCT_PROCESSOR TRG
        USING (SELECT DISTINCT
                      SPP.V_PROD_CODE PROD_CODE,
                      SPP.FIC_MIS_DATE,
                      IAP.V_PROD_CODE
                 FROM    STG_PRODUCT_PROCESSOR SPP
                      INNER JOIN
                         INTERNAL_ACCOUNTS_PRODUCTS IAP
                      ON IAP.V_PROD_CODE = SPP.V_PROD_CODE
                WHERE SPP.FIC_MIS_DATE = LD_MIS_DATE) SRC
           ON (    SRC.PROD_CODE = TRG.V_PROD_CODE
               AND SRC.FIC_MIS_DATE = TRG.FIC_MIS_DATE)
   WHEN MATCHED
   THEN
      UPDATE SET TRG.F_EXPOSURE_ENABLED_IND = 'N';


   -- REGARDING TO ISSUE NUMBER 12 -ON CONCATINATED WITH THE PRODUCT CODE

   UPDATE OFSMDM.STG_PRODUCT_PROCESSOR PP
      SET PP.V_PROD_CODE =
             CASE
                WHEN PP.V_PROD_CODE NOT LIKE '%-ON'
                THEN
                   CONCAT (PP.V_PROD_CODE, '-ON')
                ELSE
                   PP.V_PROD_CODE
             END
    WHERE     PP.FIC_MIS_DATE = LD_MIS_DATE
          AND PP.V_GL_LINE IN ('GLB.8650', 'GLB.8215', 'ISB.8291');

   COMMIT;

   UPDATE STG_PRODUCT_PROCESSOR PP
      SET PP.F_EXPOSURE_ENABLED_IND = 'N'
    WHERE     PP.FIC_MIS_DATE = LD_MIS_DATE
          AND PP.V_PROD_CODE NOT LIKE '%ON'
          AND PP.V_GL_CODE IN
                 ('401020114-0000',
                  '406010101-0000',
                  '403010101-0000',
                  '404010501-1107',
                  '403010113-0000',
                  '403010106-0000',
                  '403010109-0000',
                  '403010111-0000',
                  '402010305-0000',
                  '404010501-1128',
                  '403010119-0000',
                  '402010203-0000',
                  '404010301-1201',
                  '402010306-0000',
                  '404010401-4008',
                  '403010115-0000',
                  '401020115-0000',
                  '403030101-0000',
                  '402010104-0000',
                  '401020110-0000',
                  '404010401-4005',
                  '404010501-1401',
                  '404010501-1104',
                  '401020111-0000',
                  '403010104-0000',
                  '404010301-3003',
                  '402010201-0000',
                  '401030101-0000',
                  '402010202-0000',
                  '403010105-0000',
                  '403010118-0000',
                  '402010103-0000',
                  '404020102-0000',
                  '403010116-0000',
                  '404020103-0000',
                  '401010109-0000',
                  '403010102-0000',
                  '401020112-0000',
                  '402010204-0000',
                  '402010101-0000',
                  '404010501-1136',
                  '402010102-0000');

   COMMIT;

   -- As per Bank Following GLs will be Blocked from the Dec,2019 run onwards,As these are Stuckup gls.As these are Off balance Sheet GLs
   --this will not used as per muddaser on 29 mar 2022
   /* UPDATE stg_product_processor pp
    SET
        pp.f_exposure_enabled_ind = 'N'
    WHERE
        pp.fic_mis_date = ld_mis_date
        AND pp.v_prod_code NOT LIKE '%ON'
        AND pp.v_gl_code LIKE '40%';

    COMMIT;*/

   --in('404010401-4005','401020115-0000','404010501-1107','404010501-1128','401020103-0000','401020104-0000','401020105-0000','404010501-1401','404010301-3003');

   -- app per muddaser on MAR 2022 following GL is unblocked
   /* UPDATE stg_product_processor pp444
    SET
        pp.f_exposure_enabled_ind = 'Y'
    WHERE
        pp.fic_mis_date = ld_mis_date
        AND pp.v_prod_code NOT LIKE '%ON'
        AND pp.v_gl_code = '401020108-0000';*/

   COMMIT;

   DBMS_OUTPUT.PUT_LINE ('Start of Insert');



   INSERT INTO STG_PRODUCT_PROCESSOR B (B.FIC_MIS_DATE,
                                        B.V_ACCOUNT_NUMBER,
                                        B.N_EOP_BAL,
                                        B.N_ACCRUED_INTEREST,
                                        B.V_CCY_CODE,
                                        B.V_COUNTRY_ID,
                                        B.V_BRANCH_CODE,
                                        B.V_GL_CODE,
                                        B.V_LOB_CODE,
                                        B.V_CUST_REF_CODE,
                                        B.V_GAAP_CODE,
                                        B.V_PROD_CODE,
                                        B.D_MATURITY_DATE,
                                        B.D_START_DATE,
                                        B.N_MTM_VALUE,
                                        B.V_DATA_ORIGIN,
                                        B.N_EXCHANGE_RATE,
                                        B.V_LV_CODE,
                                        B.F_PAST_DUE_FLAG,
                                        B.N_PROVISION_AMOUNT,
                                        B.V_EXP_RCY_CODE,
                                        B.F_AUTO_CANCELLATION_FLAG,
                                        B.F_UNCOND_CANCELLED_EXP_IND,
                                        B.F_INST_SHORT_TERM_QUALIF_FLAG,
                                        B.V_BOOK_TYPE,
                                        B.V_LINE_CODE,
                                        B.V_ISSUER_CODE,
                                        B.F_RECIPROCAL_CROSS_HLDG_IND,
                                        B.N_REMARGIN_FREQUENCY,
                                        B.N_REVALUATION_FREQUENCY,
                                        B.N_EOP_BAL_NPL,
                                        B.V_LOAN_CLASSIFICATION,
                                        B.N_INT_ACCRUED_MTD,
                                        B.V_PROV_GL_CODE,
                                        B.V_ACCR_INT_GL_CODE,
                                        B.N_UNDRAWN_AMT,
                                        B.F_MAIN_INDEX_EQUITY_FLAG,
                                        B.F_EQUITY_TRADED_FLAG,
                                        B.N_CURRENT_CREDIT_LIMIT,
                                        B.V_BAL_TYPE,
                                        B.V_ORIG_ACCT_NO,
                                        B.V_LCY_CCY_CODE,
                                        B.N_LCY_AMT,
                                        B.V_LEG_REP_CODE,
                                        B.V_TRADING_DESK_ID,
                                        B.N_NO_OF_UNITS,
                                        B.V_INSTRUMENT_POSITION,
                                        B.D_EFFECTIVE_DATE,
                                        B.D_ISSUE_DATE,
                                        B.D_NEXT_TO_LAST_COUPON_DATE,
                                        B.F_OTC_IND,
                                        B.N_COUPON_FREQUENCY,
                                        B.N_FACE_VALUE,
                                        B.N_COUPON_RATE,
                                        B.N_FLOATING_RATE_SPREAD,
                                        B.N_STRIKE_RATE_PRICE,
                                        B.V_BANK_INSTR_TYPE_CODE,
                                        B.V_BENCHMARK_CODE,
                                        B.V_INSTRUMENT_SHORT_NAME,
                                        B.V_CCY2_CODE,
                                        B.V_COUNTERPARTY_ID,
                                        B.V_COUNTERPARTY_RATING,
                                        B.V_DAY_COUNT_IND,
                                        B.V_EXP_CATEGORY_CODE,
                                        B.V_INSTRUMENT_CODE,
                                        B.V_INSTR_TYPE_CODE,
                                        B.V_ISSUER_ID,
                                        B.V_ISSUER_NAME,
                                        B.V_ISSUER_TYPE,
                                        B.V_STOCK_INDEX_CODE,
                                        B.V_RATING_CODE,
                                        B.F_GP_FUND_CCY_SAME_IND,
                                        B.N_REDEMPTION_VALUE,
                                        B.N_SETTLEMENT_DAYS,
                                        B.V_BENCHMARK_CCY_CODE,
                                        B.V_HOLIDAY_ROLL_CONVENTION_CD,
                                        B.V_BENCHMARK_DAY_COUNTER,
                                        B.V_PARENT_GL_CODE,
                                        B.V_EXP_PROD_CODE,
                                        F_EXPOSURE_ENABLED_IND)
      SELECT GL.FIC_MIS_DATE FIC_MIS_DATE1,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ACCOUNT_NUMBER,
             GL.N_AMOUNT_LCY N_AMOUNT_LCY1,
             NULL N_ACCRUED_INTEREST,
             NVL ( (SELECT V_CCY_CODE
                      FROM SETUP_GL_ATTRIBUTES SGA
                     WHERE SGA.V_GL_CODE = GL.V_GL_CODE),
                  GL.V_CCY_CODE)
                V_CCY_CODE1,
             'PAK' V_COUNTRY_ID,
             GL.V_BRANCH_CODE V_BRANCH_CO4DE,
             GL.V_GL_CODE V_GL_CODE1,
             BNKDT.V_LOB_CODE V_LOB_CODE,
             BNKDT.V_PARTY_TYPE_CODE V_CUST_REF_CODE,
             GL.V_GAAP_CODE V_GAAP_CODE1,
             BNKDT.V_EXP_PROD_CODE V_PROD_CODE1,
             LD_MIS_DATE + 15 D_MATURITY_DATE,
             NULL START_DATE,
             NULL N_MTM_VALUE,
             CASE
                WHEN GL.V_GL_CODE IN
                        ('106020101-0000',
                         '206020101-0000',
                         '106010110-0000',
                         '205080402-0000')
                THEN
                   'EXCLUDE'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'ADVANCES'
                THEN
                   'MANUAL-ADVANCES'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Advance taxation'
                THEN
                   'MANUAL-ADVTAX'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'BALANCE WITH OTHER BANKS'
                THEN
                   'MANUAL-BWB'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Cash and Bank Balances'
                THEN
                   'MANUAL-CBB'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Fixed Assets'
                THEN
                   'MANUAL-FIXEDASSET'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'INVESTMENTS'
                THEN
                   'MANUAL-INVESTMENTS'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Other Assets'
                THEN
                   'MANUAL-OTHASSETS'
                ELSE
                   'MANUAL-MISCELLANEOUS'
             END
                V_DATA_ORIGIN,
             NULL N_EXCHANGE_RATE,
             'ABL' V_LV_CODE1,
             NULL F_PAST_DUE_FLAG,
             NULL N_PROVISION_AMOUNT,
             NULL V_EXP_RCY_CODE,
             'Y' F_AUTO_CANCELLATION_FLAG,
             'Y' F_UNCOND_CANCELLED_EXP_IND,
             NULL F_INST_SHORT_TERM_QUALIF_FLAG,
             NULL V_BOOK_TYPE,
             GL.V_LV_CODE V_LINE_CODE,
             NULL V_ISSUER_CODE,
             NULL F_RECIPROCAL_CROSS_HLDG_IND,
             1 N_REMARGIN_FREQUENCY,
             1 N_REVALUATION_FREQUENCY,
             NULL N_EOP_BAL_NPL,
             NULL V_LOAN_CLASSIFICATION,
             NULL N_INT_ACCRUED_MTD,
             NULL V_PROV_GL_CODE,
             NULL V_ACCR_INT_GL_CODE,
             NULL N_UNDRAWN_AMT,
             NULL F_MAIN_INDEX_EQUITY_FLAG,
             NULL F_EQUITY_TRADED_FLAG,
             NULL N_CURRENT_CREDIT_LIMIT,
             'LIVEDB' V_BAL_TYPE,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ORIG_ACCT_NO,
             NVL ( (SELECT V_CCY_CODE
                      FROM SETUP_GL_ATTRIBUTES SGA
                     WHERE SGA.V_GL_CODE = GL.V_GL_CODE),
                  'PKR')
                V_LCY_CCY_CODE,
             GL.N_AMOUNT_LCY N_LCY_AMT,
             NULL V_LEG_REP_CODE,
             NULL V_TRADING_DESK_ID,
             NULL N_NO_OF_UNITS,
             NULL V_INSTRUMENT_POSITION,
             NULL D_EFFECTIVE_DATE,
             NULL D_ISSUE_DATE,
             NULL D_NEXT_TO_LAST_COUPON_DATE,
             NULL F_OTC_IND,
             NULL N_COUPON_FREQUENCY,
             NULL N_FACE_VALUE,
             NULL N_COUPON_RATE,
             NULL N_FLOATING_RATE_SPREAD,
             NULL N_STRIKE_RATE_PRICE,
             NULL V_BANK_INSTR_TYPE_CODE,
             NULL V_BENCHMARK_CODE,
             NULL V_INSTRUMENT_SHORT_NAME,
             NULL V_CCY2_CODE,
             NULL V_COUNTERPARTY_ID,
             NULL V_COUNTERPARTY_RATING,
             NULL V_DAY_COUNT_IND,
             BNKDT.V_EXP_CATEGORY_CODE V_EXP_CATEGORY_CODE,
             NULL V_INSTRUMENT_CODE,
             NULL V_INSTR_TYPE_CODE,
             NULL V_ISSUER_ID,
             NULL V_ISSUER_NAME,
             NULL V_ISSUER_TYPE,
             NULL V_STOCK_INDEX_CODE,
             NULL V_RATING_CODE,
             NULL F_GP_FUND_CCY_SAME_IND,
             NULL N_REDEMPTION_VALUE,
             NULL N_SETTLEMENT_DAYS,
             NULL V_BENCHMARK_CCY_CODE,
             NULL V_HOLIDAY_ROLL_CONVENTION_CD,
             NULL V_BENCHMARK_DAY_COUNTER,
             BNKDT.V_PARENT_GL_CODE V_PARENT_GL_CODE,
             BNKDT.V_EXP_PROD_CODE V_EXP_PROD_CODE,
             NULL F_EXPOSURE_ENABLED_IND
        FROM    STG_GL_DATA GL
             INNER JOIN
                STG_BNK_COA_DETAILS BNKDT
             ON GL.V_GL_CODE = BNKDT.V_GL_CODE
       WHERE     GL.FIC_MIS_DATE = LD_MIS_DATE
             AND GL.V_LV_CODE = 'ABL'
             AND BNKDT.V_GL_HEAD_CATEGORY NOT IN
                    ('Fixed Assets',
                     'Advance taxation',
                     'Cash and Bank Balances',
                     'Other Assets')
             AND GL.V_BRANCH_CODE IN ('PK0011045', 'PK0025045')
             AND BNKDT.V_PARTY_TYPE_CODE NOT IN ('RET', 'RET_GL')
      UNION
      SELECT GL.FIC_MIS_DATE FIC_MIS_DATE1,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ACCOUNT_NUMBER,
             DECODE (GL.V_GL_CODE, '108012501-1107', 0, GL.N_AMOUNT_LCY)
                N_AMOUNT_LCY1,
             NULL N_ACCRUED_INTEREST,
             NVL ( (SELECT V_CCY_CODE
                      FROM SETUP_GL_ATTRIBUTES SGA
                     WHERE SGA.V_GL_CODE = GL.V_GL_CODE),
                  GL.V_CCY_CODE)
                V_CCY_CODE1,
             'PAK' V_COUNTRY_ID,
             GL.V_BRANCH_CODE V_BRANCH_CODE,
             GL.V_GL_CODE V_GL_CODE1,
             BNKDT.V_LOB_CODE V_LOB_CODE,
             BNKDT.V_PARTY_TYPE_CODE V_CUST_REF_CODE,
             GL.V_GAAP_CODE V_GAAP_CODE1,
             BNKDT.V_EXP_PROD_CODE V_PROD_CODE1,
             LD_MIS_DATE + 15 D_MATURITY_DATE,
             NULL START_DATE,
             NULL N_MTM_VALUE,
             CASE
                WHEN GL.V_GL_CODE IN
                        ('106020101-0000',
                         '206020101-0000',
                         '106010110-0000',
                         '205080402-0000')
                THEN
                   'EXCLUDE'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'ADVANCES'
                THEN
                   'MANUAL-ADVANCES'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Advance taxation'
                THEN
                   'MANUAL-ADVTAX'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'BALANCE WITH OTHER BANKS'
                THEN
                   'MANUAL-BWB'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Cash and Bank Balances'
                THEN
                   'MANUAL-CBB'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Fixed Assets'
                THEN
                   'MANUAL-FIXEDASSET'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'INVESTMENTS'
                THEN
                   'MANUAL-INVESTMENTS'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Other Assets'
                THEN
                   'MANUAL-OTHASSETS'
                ELSE
                   'MANUAL-MISCELLANEOUS'
             END
                V_DATA_ORIGIN,
             NULL N_EXCHANGE_RATE,
             'ABL' V_LV_CODE1,
             NULL F_PAST_DUE_FLAG,
             NULL N_PROVISION_AMOUNT,
             NULL V_EXP_RCY_CODE,
             'Y' F_AUTO_CANCELLATION_FLAG,
             'Y' F_UNCOND_CANCELLED_EXP_IND,
             NULL F_INST_SHORT_TERM_QUALIF_FLAG,
             NULL V_BOOK_TYPE,
             GL.V_LV_CODE V_LINE_CODE,
             NULL V_ISSUER_CODE,
             NULL F_RECIPROCAL_CROSS_HLDG_IND,
             1 N_REMARGIN_FREQUENCY,
             1 N_REVALUATION_FREQUENCY,
             NULL N_EOP_BAL_NPL,
             NULL V_LOAN_CLASSIFICATION,
             NULL N_INT_ACCRUED_MTD,
             NULL V_PROV_GL_CODE,
             NULL V_ACCR_INT_GL_CODE,
             NULL N_UNDRAWN_AMT,
             NULL F_MAIN_INDEX_EQUITY_FLAG,
             NULL F_EQUITY_TRADED_FLAG,
             NULL N_CURRENT_CREDIT_LIMIT,
             'LIVEDB' V_BAL_TYPE,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ORIG_ACCT_NO,
             NVL ( (SELECT V_CCY_CODE
                      FROM SETUP_GL_ATTRIBUTES SGA
                     WHERE SGA.V_GL_CODE = GL.V_GL_CODE),
                  'PKR')
                V_LCY_CCY_CODE,
             DECODE (GL.V_GL_CODE || '-' || GL.V_BRANCH_CODE,
                     '108012501-1107-PK0010343', 0,
                     GL.N_AMOUNT_LCY)
                N_LCY_AMT,
             NULL V_LEG_REP_CODE,
             NULL V_TRADING_DESK_ID,
             NULL N_NO_OF_UNITS,
             NULL V_INSTRUMENT_POSITION,
             NULL D_EFFECTIVE_DATE,
             NULL D_ISSUE_DATE,
             NULL D_NEXT_TO_LAST_COUPON_DATE,
             NULL F_OTC_IND,
             NULL N_COUPON_FREQUENCY,
             NULL N_FACE_VALUE,
             NULL N_COUPON_RATE,
             NULL N_FLOATING_RATE_SPREAD,
             NULL N_STRIKE_RATE_PRICE,
             NULL V_BANK_INSTR_TYPE_CODE,
             NULL V_BENCHMARK_CODE,
             NULL V_INSTRUMENT_SHORT_NAME,
             NULL V_CCY2_CODE,
             NULL V_COUNTERPARTY_ID,
             NULL V_COUNTERPARTY_RATING,
             NULL V_DAY_COUNT_IND,
             BNKDT.V_EXP_CATEGORY_CODE V_EXP_CATEGORY_CODE,
             NULL V_INSTRUMENT_CODE,
             NULL V_INSTR_TYPE_CODE,
             NULL V_ISSUER_ID,
             NULL V_ISSUER_NAME,
             NULL V_ISSUER_TYPE,
             NULL V_STOCK_INDEX_CODE,
             NULL V_RATING_CODE,
             NULL F_GP_FUND_CCY_SAME_IND,
             NULL N_REDEMPTION_VALUE,
             NULL N_SETTLEMENT_DAYS,
             NULL V_BENCHMARK_CCY_CODE,
             NULL V_HOLIDAY_ROLL_CONVENTION_CD,
             NULL V_BENCHMARK_DAY_COUNTER,
             BNKDT.V_PARENT_GL_CODE V_PARENT_GL_CODE,
             BNKDT.V_EXP_PROD_CODE V_EXP_PROD_CODE,
             CASE
                WHEN BNKDT.V_GL_HEAD_CATEGORY IN ('Cash and Bank Balances')
                THEN
                   'N'
             END
                F_EXPOSURE_ENABLED_IND
        FROM    STG_GL_DATA GL
             INNER JOIN
                STG_BNK_COA_DETAILS BNKDT
             ON GL.V_GL_CODE = BNKDT.V_GL_CODE
       WHERE     GL.FIC_MIS_DATE = LD_MIS_DATE
             AND GL.V_LV_CODE = 'ABL'
             AND BNKDT.V_GL_HEAD_CATEGORY IN
                    ('Cash and Bank Balances', 'Other Assets')
             AND GL.V_BRANCH_CODE IN
                    ('PK0011045', 'PK0025045', 'PK0010343', 'PK0010513')
             AND BNKDT.V_PARTY_TYPE_CODE NOT IN ('RET', 'RET_GL')
      UNION
      SELECT GL.FIC_MIS_DATE FIC_MIS_DATE1,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ACCOUNT_NUMBER,
             GL.N_AMOUNT_LCY N_AMOUNT_LCY1,
             NULL N_ACCRUED_INTEREST,
             GL.V_CCY_CODE V_CCY_CODE1,
             'PAK' V_COUNTRY_ID,
             GL.V_BRANCH_CODE V_BRANCH_CODE,
             GL.V_GL_CODE V_GL_CODE1,
             BNKDT.V_LOB_CODE V_LOB_CODE,
             BNKDT.V_PARTY_TYPE_CODE V_CUST_REF_CODE,
             GL.V_GAAP_CODE V_GAAP_CODE1,
             BNKDT.V_EXP_PROD_CODE V_PROD_CODE1,
             LD_MIS_DATE + 15 D_MATURITY_DATE,
             NULL START_DATE,
             NULL N_MTM_VALUE,
             CASE
                WHEN GL.V_GL_CODE IN
                        ('106020101-0000',
                         '206020101-0000',
                         '106010110-0000',
                         '205080402-0000')
                THEN
                   'EXCLUDE'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'ADVANCES'
                THEN
                   'MANUAL-ADVANCES'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Advance taxation'
                THEN
                   'MANUAL-ADVTAX'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'BALANCE WITH OTHER BANKS'
                THEN
                   'MANUAL-BWB'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Cash and Bank Balances'
                THEN
                   'MANUAL-CBB'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Fixed Assets'
                THEN
                   'MANUAL-FIXEDASSET'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'INVESTMENTS'
                THEN
                   'MANUAL-INVESTMENTS'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Other Assets'
                THEN
                   'MANUAL-OTHASSETS'
                ELSE
                   'MANUAL-MISCELLANEOUS'
             END
                V_DATA_ORIGIN,
             NULL N_EXCHANGE_RATE,
             'ABL' V_LV_CODE1,
             NULL F_PAST_DUE_FLAG,
             NULL N_PROVISION_AMOUNT,
             NULL V_EXP_RCY_CODE,
             'Y' F_AUTO_CANCELLATION_FLAG,
             'Y' F_UNCOND_CANCELLED_EXP_IND,
             NULL F_INST_SHORT_TERM_QUALIF_FLAG,
             NULL V_BOOK_TYPE,
             GL.V_LV_CODE V_LINE_CODE,
             NULL V_ISSUER_CODE,
             NULL F_RECIPROCAL_CROSS_HLDG_IND,
             1 N_REMARGIN_FREQUENCY,
             1 N_REVALUATION_FREQUENCY,
             NULL N_EOP_BAL_NPL,
             NULL V_LOAN_CLASSIFICATION,
             NULL N_INT_ACCRUED_MTD,
             NULL V_PROV_GL_CODE,
             NULL V_ACCR_INT_GL_CODE,
             NULL N_UNDRAWN_AMT,
             NULL F_MAIN_INDEX_EQUITY_FLAG,
             NULL F_EQUITY_TRADED_FLAG,
             NULL N_CURRENT_CREDIT_LIMIT,
             'LIVEDB' V_BAL_TYPE,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ORIG_ACCT_NO,
             'PKR' V_LCY_CCY_CODE,
             GL.N_AMOUNT_LCY N_LCY_AMT,
             NULL V_LEG_REP_CODE,
             NULL V_TRADING_DESK_ID,
             NULL N_NO_OF_UNITS,
             NULL V_INSTRUMENT_POSITION,
             NULL D_EFFECTIVE_DATE,
             NULL D_ISSUE_DATE,
             NULL D_NEXT_TO_LAST_COUPON_DATE,
             NULL F_OTC_IND,
             NULL N_COUPON_FREQUENCY,
             NULL N_FACE_VALUE,
             NULL N_COUPON_RATE,
             NULL N_FLOATING_RATE_SPREAD,
             NULL N_STRIKE_RATE_PRICE,
             NULL V_BANK_INSTR_TYPE_CODE,
             NULL V_BENCHMARK_CODE,
             NULL V_INSTRUMENT_SHORT_NAME,
             NULL V_CCY2_CODE,
             NULL V_COUNTERPARTY_ID,
             NULL V_COUNTERPARTY_RATING,
             NULL V_DAY_COUNT_IND,
             BNKDT.V_EXP_CATEGORY_CODE V_EXP_CATEGORY_CODE,
             NULL V_INSTRUMENT_CODE,
             NULL V_INSTR_TYPE_CODE,
             NULL V_ISSUER_ID,
             NULL V_ISSUER_NAME,
             NULL V_ISSUER_TYPE,
             NULL V_STOCK_INDEX_CODE,
             NULL V_RATING_CODE,
             NULL F_GP_FUND_CCY_SAME_IND,
             NULL N_REDEMPTION_VALUE,
             NULL N_SETTLEMENT_DAYS,
             NULL V_BENCHMARK_CCY_CODE,
             NULL V_HOLIDAY_ROLL_CONVENTION_CD,
             NULL V_BENCHMARK_DAY_COUNTER,
             BNKDT.V_PARENT_GL_CODE V_PARENT_GL_CODE,
             BNKDT.V_EXP_PROD_CODE V_EXP_PROD_CODE,
             NULL F_EXPOSURE_ENABLED_IND
        FROM    STG_GL_DATA GL
             INNER JOIN
                STG_BNK_COA_DETAILS BNKDT
             ON GL.V_GL_CODE = BNKDT.V_GL_CODE
       WHERE     GL.FIC_MIS_DATE = LD_MIS_DATE
             AND GL.V_LV_CODE = 'ABL'
             AND BNKDT.V_GL_HEAD_CATEGORY IN ('Fixed Assets')
             AND BNKDT.V_PARTY_TYPE_CODE NOT IN ('RET', 'RET_GL')
      UNION
      SELECT GL.FIC_MIS_DATE FIC_MIS_DATE1,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ACCOUNT_NUMBER,
             GL.N_AMOUNT_LCY N_AMOUNT_LCY1,
             NULL N_ACCRUED_INTEREST,
             GL.V_CCY_CODE V_CCY_CODE1,
             'PAK' V_COUNTRY_ID,
             GL.V_BRANCH_CODE V_BRANCH_CODE,
             GL.V_GL_CODE V_GL_CODE1,
             BNKDT.V_LOB_CODE V_LOB_CODE,
             BNKDT.V_PARTY_TYPE_CODE V_CUST_REF_CODE,
             GL.V_GAAP_CODE V_GAAP_CODE1,
             BNKDT.V_EXP_PROD_CODE V_PROD_CODE1,
             LD_MIS_DATE + 15 D_MATURITY_DATE,
             NULL START_DATE,
             NULL N_MTM_VALUE,
             CASE
                WHEN GL.V_GL_CODE IN
                        ('106020101-0000',
                         '206020101-0000',
                         '106010110-0000',
                         '205080402-0000')
                THEN
                   'EXCLUDE'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'ADVANCES'
                THEN
                   'MANUAL-ADVANCES'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Advance taxation'
                THEN
                   'MANUAL-ADVTAX'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'BALANCE WITH OTHER BANKS'
                THEN
                   'MANUAL-BWB'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Cash and Bank Balances'
                THEN
                   'MANUAL-CBB'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Fixed Assets'
                THEN
                   'MANUAL-FIXEDASSET'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'INVESTMENTS'
                THEN
                   'MANUAL-INVESTMENTS'
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Other Assets'
                THEN
                   'MANUAL-OTHASSETS'
                ELSE
                   'MANUAL-MISCELLANEOUS'
             END
                V_DATA_ORIGIN,
             NULL N_EXCHANGE_RATE,
             'ABL' V_LV_CODE1,
             NULL F_PAST_DUE_FLAG,
             NULL N_PROVISION_AMOUNT,
             NULL V_EXP_RCY_CODE,
             'Y' F_AUTO_CANCELLATION_FLAG,
             'Y' F_UNCOND_CANCELLED_EXP_IND,
             NULL F_INST_SHORT_TERM_QUALIF_FLAG,
             NULL V_BOOK_TYPE,
             GL.V_LV_CODE V_LINE_CODE,
             NULL V_ISSUER_CODE,
             NULL F_RECIPROCAL_CROSS_HLDG_IND,
             1 N_REMARGIN_FREQUENCY,
             1 N_REVALUATION_FREQUENCY,
             NULL N_EOP_BAL_NPL,
             NULL V_LOAN_CLASSIFICATION,
             NULL N_INT_ACCRUED_MTD,
             NULL V_PROV_GL_CODE,
             NULL V_ACCR_INT_GL_CODE,
             NULL N_UNDRAWN_AMT,
             NULL F_MAIN_INDEX_EQUITY_FLAG,
             NULL F_EQUITY_TRADED_FLAG,
             NULL N_CURRENT_CREDIT_LIMIT,
             'LIVEDB' V_BAL_TYPE,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ORIG_ACCT_NO,
             'PKR' V_LCY_CCY_CODE,
             GL.N_AMOUNT_LCY N_LCY_AMT,
             NULL V_LEG_REP_CODE,
             NULL V_TRADING_DESK_ID,
             NULL N_NO_OF_UNITS,
             NULL V_INSTRUMENT_POSITION,
             NULL D_EFFECTIVE_DATE,
             NULL D_ISSUE_DATE,
             NULL D_NEXT_TO_LAST_COUPON_DATE,
             NULL F_OTC_IND,
             NULL N_COUPON_FREQUENCY,
             NULL N_FACE_VALUE,
             NULL N_COUPON_RATE,
             NULL N_FLOATING_RATE_SPREAD,
             NULL N_STRIKE_RATE_PRICE,
             NULL V_BANK_INSTR_TYPE_CODE,
             NULL V_BENCHMARK_CODE,
             NULL V_INSTRUMENT_SHORT_NAME,
             NULL V_CCY2_CODE,
             NULL V_COUNTERPARTY_ID,
             NULL V_COUNTERPARTY_RATING,
             NULL V_DAY_COUNT_IND,
             BNKDT.V_EXP_CATEGORY_CODE V_EXP_CATEGORY_CODE,
             NULL V_INSTRUMENT_CODE,
             NULL V_INSTR_TYPE_CODE,
             NULL V_ISSUER_ID,
             NULL V_ISSUER_NAME,
             NULL V_ISSUER_TYPE,
             NULL V_STOCK_INDEX_CODE,
             NULL V_RATING_CODE,
             NULL F_GP_FUND_CCY_SAME_IND,
             NULL N_REDEMPTION_VALUE,
             NULL N_SETTLEMENT_DAYS,
             NULL V_BENCHMARK_CCY_CODE,
             NULL V_HOLIDAY_ROLL_CONVENTION_CD,
             NULL V_BENCHMARK_DAY_COUNTER,
             BNKDT.V_PARENT_GL_CODE V_PARENT_GL_CODE,
             BNKDT.V_EXP_PROD_CODE V_EXP_PROD_CODE,
             NULL F_EXPOSURE_ENABLED_IND
        FROM    STG_GL_DATA GL
             INNER JOIN
                STG_BNK_COA_DETAILS BNKDT
             ON GL.V_GL_CODE = BNKDT.V_GL_CODE
       WHERE     GL.FIC_MIS_DATE = LD_MIS_DATE
             AND GL.V_LV_CODE = 'ABL'
             AND BNKDT.V_GL_HEAD_CATEGORY IN ('Advance taxation')
             AND BNKDT.V_PARTY_TYPE_CODE NOT IN ('RET', 'RET_GL')
      UNION
      /******************  Retail Protfolio from GLs ******************/

      SELECT GL.FIC_MIS_DATE FIC_MIS_DATE,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ACCOUNT_NUMBER,
             GL.N_AMOUNT_LCY N_AMOUNT_LCY,
             NULL N_ACCRUED_INTEREST,
             NVL ( (SELECT V_CCY_CODE
                      FROM SETUP_GL_ATTRIBUTES SGA
                     WHERE SGA.V_GL_CODE = GL.V_GL_CODE),
                  GL.V_CCY_CODE)
                V_CCY_CODE,
             'PAK' V_COUNTRY_ID,
             GL.V_BRANCH_CODE V_BRANCH_CODE,
             GL.V_GL_CODE V_GL_CODE,
             BNKDT.V_LOB_CODE V_LOB_CODE,
             BNKDT.V_PARTY_TYPE_CODE V_CUST_REF_CODE,
             GL.V_GAAP_CODE V_GAAP_CODE,
             BNKDT.V_EXP_PROD_CODE V_PROD_CODE,
             LD_MIS_DATE + 15 D_MATURITY_DATE,
             NULL START_DATE,
             NULL N_MTM_VALUE,
             CASE
                WHEN BNKDT.V_GL_HEAD_CATEGORY = 'Retail' THEN 'MANUAL-RETAIL'
             END
                V_DATA_ORIGIN,
             NULL N_EXCHANGE_RATE,
             'ABL' V_LV_CODE1,
             NULL F_PAST_DUE_FLAG,
             NULL N_PROVISION_AMOUNT,
             NULL V_EXP_RCY_CODE,
             'Y' F_AUTO_CANCELLATION_FLAG,
             'Y' F_UNCOND_CANCELLED_EXP_IND,
             NULL F_INST_SHORT_TERM_QUALIF_FLAG,
             NULL V_BOOK_TYPE,
             GL.V_LV_CODE V_LINE_CODE,
             NULL V_ISSUER_CODE,
             NULL F_RECIPROCAL_CROSS_HLDG_IND,
             1 N_REMARGIN_FREQUENCY,
             1 N_REVALUATION_FREQUENCY,
             NULL N_EOP_BAL_NPL,
             NULL V_LOAN_CLASSIFICATION,
             NULL N_INT_ACCRUED_MTD,
             NULL V_PROV_GL_CODE,
             NULL V_ACCR_INT_GL_CODE,
             NULL N_UNDRAWN_AMT,
             NULL F_MAIN_INDEX_EQUITY_FLAG,
             NULL F_EQUITY_TRADED_FLAG,
             NULL N_CURRENT_CREDIT_LIMIT,
             'LIVEDB' V_BAL_TYPE,
                GL.V_PROD_CODE
             || GL.V_GL_CODE
             || GL.V_LV_CODE
             || GL.V_BRANCH_CODE
             || 'ABL'
                V_ORIG_ACCT_NO,
             NVL ( (SELECT V_CCY_CODE
                      FROM SETUP_GL_ATTRIBUTES SGA
                     WHERE SGA.V_GL_CODE = GL.V_GL_CODE),
                  'PKR')
                V_LCY_CCY_CODE,
             GL.N_AMOUNT_LCY N_LCY_AMT,
             NULL V_LEG_REP_CODE,
             NULL V_TRADING_DESK_ID,
             NULL N_NO_OF_UNITS,
             NULL V_INSTRUMENT_POSITION,
             NULL D_EFFECTIVE_DATE,
             NULL D_ISSUE_DATE,
             NULL D_NEXT_TO_LAST_COUPON_DATE,
             NULL F_OTC_IND,
             NULL N_COUPON_FREQUENCY,
             NULL N_FACE_VALUE,
             NULL N_COUPON_RATE,
             NULL N_FLOATING_RATE_SPREAD,
             NULL N_STRIKE_RATE_PRICE,
             NULL V_BANK_INSTR_TYPE_CODE,
             NULL V_BENCHMARK_CODE,
             NULL V_INSTRUMENT_SHORT_NAME,
             NULL V_CCY2_CODE,
             NULL V_COUNTERPARTY_ID,
             NULL V_COUNTERPARTY_RATING,
             NULL V_DAY_COUNT_IND,
             BNKDT.V_EXP_CATEGORY_CODE V_EXP_CATEGORY_CODE,
             NULL V_INSTRUMENT_CODE,
             NULL V_INSTR_TYPE_CODE,
             NULL V_ISSUER_ID,
             NULL V_ISSUER_NAME,
             NULL V_ISSUER_TYPE,
             NULL V_STOCK_INDEX_CODE,
             NULL V_RATING_CODE,
             NULL F_GP_FUND_CCY_SAME_IND,
             NULL N_REDEMPTION_VALUE,
             NULL N_SETTLEMENT_DAYS,
             NULL V_BENCHMARK_CCY_CODE,
             NULL V_HOLIDAY_ROLL_CONVENTION_CD,
             NULL V_BENCHMARK_DAY_COUNTER,
             BNKDT.V_PARENT_GL_CODE V_PARENT_GL_CODE,
             BNKDT.V_EXP_PROD_CODE V_EXP_PROD_CODE,
             'Y' F_EXPOSURE_ENABLED_IND
        FROM    STG_GL_DATA GL
             INNER JOIN
                STG_BNK_COA_DETAILS BNKDT
             ON GL.V_GL_CODE = BNKDT.V_GL_CODE
       WHERE     GL.FIC_MIS_DATE = LD_MIS_DATE
             AND GL.V_LV_CODE = 'ABL'
             AND BNKDT.V_GL_HEAD_CATEGORY = 'Retail'
             AND BNKDT.V_PARTY_TYPE_CODE = 'RET_GL';


   DBMS_OUTPUT.PUT_LINE ('End of Insert');

   DELETE FROM STG_PARTY_MASTER
         WHERE FIC_MIS_DATE = LD_MIS_DATE AND V_DATA_ORIGIN = 'RMG - Manual';

   COMMIT;


   INSERT INTO STG_PARTY_MASTER (V_PARTY_ID,
                                 FIC_MIS_DATE,
                                 V_PARTY_NAME,
                                 V_DATA_ORIGIN,
                                 V_BRANCH_CODE,
                                 V_TYPE,
                                 V_LOB,
                                 V_INDUSTRY_CODE,
                                 F_FINANCIAL_ENTITY_IND,
                                 F_GROUP_ENTITY_FLAG)
      SELECT 'RET_GL',
             LD_MIS_DATE,
             'Retail GL Customer',
             'RMG - Manual',
             'PK0010001',
             'RET',
             '100',
             '8252',
             0,
             0
        FROM DUAL;

   COMMIT;

   /* Dummy Exposures for Credit Card null */
   /* As per discussed with business team on 8th-jan 2020 dummy exposure for CCnull need to be exclude because they aer
   being populated from te ETL*/

   /* FOR I IN (SELECT   GL.V_GL_CODE,
                 GL.V_BRANCH_CODE,
                 GL.N_AMOUNT_LCY,
                 ABS (GL.N_AMOUNT_LCY) N_AMOUNT_LCY_ABS,
                 GL.FIC_MIS_DATE,
                 GL.V_LV_CODE,
                 GL.V_CCY_CODE,
                 BNKDT.V_LOB_CODE,
                 BNKDT.V_PARTY_TYPE_CODE,
                 GL.V_GAAP_CODE,
                 BNKDT.V_EXP_PROD_CODE,
                 GL.V_PROD_CODE,
                 BNKDT.V_EXP_CATEGORY_CODE,
                 BNKDT.V_PARENT_GL_CODE V_PARENT_GL_CODE,
                 BNKDT.V_TAGGED
          FROM      STG_GL_DATA GL
                 INNER JOIN
                    STG_BNK_COA_DETAILS BNKDT
                 ON GL.V_GL_CODE = BNKDT.V_GL_CODE
         WHERE       GL.FIC_MIS_DATE = LD_MIS_DATE
                 AND gl.v_lv_code = 'ABL'
                 AND BNKDT.V_PARTY_TYPE_CODE IN ('RET'))
    LOOP
       LN_CNT := 0;

       DBMS_OUTPUT.PUT_LINE (
          I.V_GL_CODE || ' | ' || I.V_BRANCH_CODE || ' | ' || I.N_AMOUNT_LCY
       );

       LN_SIGN := SIGN (I.N_AMOUNT_LCY);

       IF (I.N_AMOUNT_LCY_ABS > LN_20_MN_LIMIT)
       THEN
          DBMS_OUTPUT.PUT_LINE (
             ' ---------> Amount > 20 Mn = ' || I.N_AMOUNT_LCY_ABS
          );

          LN_GL_AMOUNT := I.N_AMOUNT_LCY_ABS;

          WHILE LN_GL_AMOUNT > 0
          LOOP
             IF LN_GL_AMOUNT >= LN_20_MN_LIMIT
             THEN
                LN_GL_AMOUNT := LN_GL_AMOUNT - LN_20_MN_LIMIT;

                LN_EXP_AMOUNT := LN_20_MN_LIMIT;
             ELSE
                LN_EXP_AMOUNT := LN_GL_AMOUNT;

                LN_GL_AMOUNT := 0;
             END IF;

             DBMS_OUTPUT.PUT_LINE (
                ' -----------------> Parcel = ' || LN_SIGN * LN_EXP_AMOUNT
             );

             LN_CNT := LN_CNT + 1;

             INSERT INTO STG_PRODUCT_PROCESSOR B (B.FIC_MIS_DATE,
                                                  B.V_ACCOUNT_NUMBER,
                                                  B.N_EOP_BAL,
                                                  B.N_ACCRUED_INTEREST,
                                                  B.V_CCY_CODE,
                                                  B.V_COUNTRY_ID,
                                                  B.V_BRANCH_CODE,
                                                  B.V_GL_CODE,
                                                  B.V_LOB_CODE,
                                                  B.V_CUST_REF_CODE,
                                                  B.V_GAAP_CODE,
                                                  B.V_PROD_CODE,
                                                  B.D_MATURITY_DATE,
                                                  B.D_START_DATE,
                                                  B.N_MTM_VALUE,
                                                  B.V_DATA_ORIGIN,
                                                  B.N_EXCHANGE_RATE,
                                                  B.V_LV_CODE,
                                                  B.F_PAST_DUE_FLAG,
                                                  B.N_PROVISION_AMOUNT,
                                                  B.V_EXP_RCY_CODE,
                                                  B.F_AUTO_CANCELLATION_FLAG,
                                                  B.F_UNCOND_CANCELLED_EXP_IND,
                                                  B.F_INST_SHORT_TERM_QUALIF_FLAG,
                                                  B.V_BOOK_TYPE,
                                                  B.V_LINE_CODE,
                                                  B.V_ISSUER_CODE,
                                                  B.F_RECIPROCAL_CROSS_HLDG_IND,
                                                  B.N_REMARGIN_FREQUENCY,
                                                  B.N_REVALUATION_FREQUENCY,
                                                  B.N_EOP_BAL_NPL,
                                                  B.V_LOAN_CLASSIFICATION,
                                                  B.N_INT_ACCRUED_MTD,
                                                  B.V_PROV_GL_CODE,
                                                  B.V_ACCR_INT_GL_CODE,
                                                  B.N_UNDRAWN_AMT,
                                                  B.F_MAIN_INDEX_EQUITY_FLAG,
                                                  B.F_EQUITY_TRADED_FLAG,
                                                  B.N_CURRENT_CREDIT_LIMIT,
                                                  B.V_BAL_TYPE,
                                                  B.V_ORIG_ACCT_NO,
                                                  B.V_LCY_CCY_CODE,
                                                  B.N_LCY_AMT,
                                                  B.V_LEG_REP_CODE,
                                                  B.V_TRADING_DESK_ID,
                                                  B.N_NO_OF_UNITS,
                                                  B.V_INSTRUMENT_POSITION,
                                                  B.D_EFFECTIVE_DATE,
                                                  B.D_ISSUE_DATE,
                                                  B.D_NEXT_TO_LAST_COUPON_DATE,
                                                  B.F_OTC_IND,
                                                  B.N_COUPON_FREQUENCY,
                                                  B.N_FACE_VALUE,
                                                  B.N_COUPON_RATE,
                                                  B.N_FLOATING_RATE_SPREAD,
                                                  B.N_STRIKE_RATE_PRICE,
                                                  B.V_BANK_INSTR_TYPE_CODE,
                                                  B.V_BENCHMARK_CODE,
                                                  B.V_INSTRUMENT_SHORT_NAME,
                                                  B.V_CCY2_CODE,
                                                  B.V_COUNTERPARTY_ID,
                                                  B.V_COUNTERPARTY_RATING,
                                                  B.V_DAY_COUNT_IND,
                                                  B.V_EXP_CATEGORY_CODE,
                                                  B.V_INSTRUMENT_CODE,
                                                  B.V_INSTR_TYPE_CODE,
                                                  B.V_ISSUER_ID,
                                                  B.V_ISSUER_NAME,
                                                  B.V_ISSUER_TYPE,
                                                  B.V_STOCK_INDEX_CODE,
                                                  B.V_RATING_CODE,
                                                  B.F_GP_FUND_CCY_SAME_IND,
                                                  B.N_REDEMPTION_VALUE,
                                                  B.N_SETTLEMENT_DAYS,
                                                  B.V_BENCHMARK_CCY_CODE,
                                                  B.V_HOLIDAY_ROLL_CONVENTION_CD,
                                                  B.V_BENCHMARK_DAY_COUNTER,
                                                  B.V_PARENT_GL_CODE,
                                                  B.V_EXP_PROD_CODE)
                SELECT   I.FIC_MIS_DATE FIC_MIS_DATE1,
                            I.V_PROD_CODE
                         || I.V_GL_CODE
                         || I.V_LV_CODE
                         || I.V_BRANCH_CODE
                         || LN_CNT
                            --ROUND(DBMS_RANDOM.VALUE(1,1000),0)
                            V_ACCOUNT_NUMBER,
                         (LN_SIGN * LN_EXP_AMOUNT) N_AMOUNT_LCY1,
                         NULL N_ACCRUED_INTEREST,
                         I.V_CCY_CODE V_CCY_CODE1,
                         'PAK' V_COUNTRY_ID,
                         I.V_BRANCH_CODE V_BRANCH_CODE,
                         I.V_GL_CODE V_GL_CODE1,
                         I.V_LOB_CODE V_LOB_CODE,
                         I.V_PARTY_TYPE_CODE V_CUST_REF_CODE,
                         I.V_GAAP_CODE V_GAAP_CODE1,
                         I.V_EXP_PROD_CODE V_PROD_CODE1,
                         LD_MIS_DATE + 30 D_MATURITY_DATE,
                         NULL START_DATE,
                         NULL N_MTM_VALUE,
                         DECODE (I.V_TAGGED,
                                 'DMY-CFI', 'MANUAL-DMY-CFI',
                                 'DMY-CC', 'MANUAL-DMY-CC',
                                 'DMY-HR', 'MANUAL-DMY-HR',
                                 'MANUAL-CCnull')
                            V_DATA_ORIGIN,
                         NULL N_EXCHANGE_RATE,
                         'ABL' V_LV_CODE1,
                         NULL F_PAST_DUE_FLAG,
                         NULL N_PROVISION_AMOUNT,
                         NULL V_EXP_RCY_CODE,
                         'Y' F_AUTO_CANCELLATION_FLAG,
                         'Y' F_UNCOND_CANCELLED_EXP_IND,
                         NULL F_INST_SHORT_TERM_QUALIF_FLAG,
                         NULL V_BOOK_TYPE,
                         I.V_LV_CODE V_LINE_CODE,
                         NULL V_ISSUER_CODE,
                         NULL F_RECIPROCAL_CROSS_HLDG_IND,
                         1 N_REMARGIN_FREQUENCY,
                         1 N_REVALUATION_FREQUENCY,
                         NULL N_EOP_BAL_NPL,
                         NULL V_LOAN_CLASSIFICATION,
                         NULL N_INT_ACCRUED_MTD,
                         NULL V_PROV_GL_CODE,
                         NULL V_ACCR_INT_GL_CODE,
                         NULL N_UNDRAWN_AMT,
                         NULL F_MAIN_INDEX_EQUITY_FLAG,
                         NULL F_EQUITY_TRADED_FLAG,
                         NULL N_CURRENT_CREDIT_LIMIT,
                         DECODE(I.V_GL_CODE,'108012801-1304','51001','LIVEDB') V_BAL_TYPE,
                            I.V_PROD_CODE
                         || I.V_GL_CODE
                         || I.V_LV_CODE
                         || I.V_BRANCH_CODE
                         || 'ABL'
                         || LN_CNT
                            V_ORIG_ACCT_NO,
                         'PKR' V_LCY_CCY_CODE,
                         (LN_SIGN * LN_EXP_AMOUNT) N_LCY_AMT,
                         NULL V_LEG_REP_CODE,
                         NULL V_TRADING_DESK_ID,
                         NULL N_NO_OF_UNITS,
                         NULL V_INSTRUMENT_POSITION,
                         NULL D_EFFECTIVE_DATE,
                         NULL D_ISSUE_DATE,
                         NULL D_NEXT_TO_LAST_COUPON_DATE,
                         NULL F_OTC_IND,
                         NULL N_COUPON_FREQUENCY,
                         NULL N_FACE_VALUE,
                         NULL N_COUPON_RATE,
                         NULL N_FLOATING_RATE_SPREAD,
                         NULL N_STRIKE_RATE_PRICE,
                         NULL V_BANK_INSTR_TYPE_CODE,
                         NULL V_BENCHMARK_CODE,
                         NULL V_INSTRUMENT_SHORT_NAME,
                         NULL V_CCY2_CODE,
                         NULL V_COUNTERPARTY_ID,
                         NULL V_COUNTERPARTY_RATING,
                         NULL V_DAY_COUNT_IND,
                         I.V_EXP_CATEGORY_CODE V_EXP_CATEGORY_CODE,
                         NULL V_INSTRUMENT_CODE,
                         NULL V_INSTR_TYPE_CODE,
                         NULL V_ISSUER_ID,
                         NULL V_ISSUER_NAME,
                         NULL V_ISSUER_TYPE,
                         NULL V_STOCK_INDEX_CODE,
                         NULL V_RATING_CODE,
                         NULL F_GP_FUND_CCY_SAME_IND,
                         NULL N_REDEMPTION_VALUE,
                         NULL N_SETTLEMENT_DAYS,
                         NULL V_BENCHMARK_CCY_CODE,
                         NULL V_HOLIDAY_ROLL_CONVENTION_CD,
                         NULL V_BENCHMARK_DAY_COUNTER,
                         I.V_PARENT_GL_CODE V_PARENT_GL_CODE,
                         I.V_EXP_PROD_CODE V_EXP_PROD_CODE
                  FROM   DUAL;
          END LOOP;
       ELSIF (ABS (NVL (I.N_AMOUNT_LCY, 0)) > 0)
       THEN
          DBMS_OUTPUT.PUT_LINE (
             ' -----------------> FULL = ' || I.N_AMOUNT_LCY
          );

          INSERT INTO STG_PRODUCT_PROCESSOR B (B.FIC_MIS_DATE,
                                               B.V_ACCOUNT_NUMBER,
                                               B.N_EOP_BAL,
                                               B.N_ACCRUED_INTEREST,
                                               B.V_CCY_CODE,
                                               B.V_COUNTRY_ID,
                                               B.V_BRANCH_CODE,
                                               B.V_GL_CODE,
                                               B.V_LOB_CODE,
                                               B.V_CUST_REF_CODE,
                                               B.V_GAAP_CODE,
                                               B.V_PROD_CODE,
                                               B.D_MATURITY_DATE,
                                               B.D_START_DATE,
                                               B.N_MTM_VALUE,
                                               B.V_DATA_ORIGIN,
                                               B.N_EXCHANGE_RATE,
                                               B.V_LV_CODE,
                                               B.F_PAST_DUE_FLAG,
                                               B.N_PROVISION_AMOUNT,
                                               B.V_EXP_RCY_CODE,
                                               B.F_AUTO_CANCELLATION_FLAG,
                                               B.F_UNCOND_CANCELLED_EXP_IND,
                                               B.F_INST_SHORT_TERM_QUALIF_FLAG,
                                               B.V_BOOK_TYPE,
                                               B.V_LINE_CODE,
                                               B.V_ISSUER_CODE,
                                               B.F_RECIPROCAL_CROSS_HLDG_IND,
                                               B.N_REMARGIN_FREQUENCY,
                                               B.N_REVALUATION_FREQUENCY,
                                               B.N_EOP_BAL_NPL,
                                               B.V_LOAN_CLASSIFICATION,
                                               B.N_INT_ACCRUED_MTD,
                                               B.V_PROV_GL_CODE,
                                               B.V_ACCR_INT_GL_CODE,
                                               B.N_UNDRAWN_AMT,
                                               B.F_MAIN_INDEX_EQUITY_FLAG,
                                               B.F_EQUITY_TRADED_FLAG,
                                               B.N_CURRENT_CREDIT_LIMIT,
                                               B.V_BAL_TYPE,
                                               B.V_ORIG_ACCT_NO,
                                               B.V_LCY_CCY_CODE,
                                               B.N_LCY_AMT,
                                               B.V_LEG_REP_CODE,
                                               B.V_TRADING_DESK_ID,
                                               B.N_NO_OF_UNITS,
                                               B.V_INSTRUMENT_POSITION,
                                               B.D_EFFECTIVE_DATE,
                                               B.D_ISSUE_DATE,
                                               B.D_NEXT_TO_LAST_COUPON_DATE,
                                               B.F_OTC_IND,
                                               B.N_COUPON_FREQUENCY,
                                               B.N_FACE_VALUE,
                                               B.N_COUPON_RATE,
                                               B.N_FLOATING_RATE_SPREAD,
                                               B.N_STRIKE_RATE_PRICE,
                                               B.V_BANK_INSTR_TYPE_CODE,
                                               B.V_BENCHMARK_CODE,
                                               B.V_INSTRUMENT_SHORT_NAME,
                                               B.V_CCY2_CODE,
                                               B.V_COUNTERPARTY_ID,
                                               B.V_COUNTERPARTY_RATING,
                                               B.V_DAY_COUNT_IND,
                                               B.V_EXP_CATEGORY_CODE,
                                               B.V_INSTRUMENT_CODE,
                                               B.V_INSTR_TYPE_CODE,
                                               B.V_ISSUER_ID,
                                               B.V_ISSUER_NAME,
                                               B.V_ISSUER_TYPE,
                                               B.V_STOCK_INDEX_CODE,
                                               B.V_RATING_CODE,
                                               B.F_GP_FUND_CCY_SAME_IND,
                                               B.N_REDEMPTION_VALUE,
                                               B.N_SETTLEMENT_DAYS,
                                               B.V_BENCHMARK_CCY_CODE,
                                               B.V_HOLIDAY_ROLL_CONVENTION_CD,
                                               B.V_BENCHMARK_DAY_COUNTER,
                                               B.V_PARENT_GL_CODE,
                                               B.V_EXP_PROD_CODE)
             SELECT   I.FIC_MIS_DATE FIC_MIS_DATE1,
                         I.V_PROD_CODE
                      || I.V_GL_CODE
                      || I.V_LV_CODE
                      || I.V_BRANCH_CODE
                      || LN_CNT
                         --ROUND(DBMS_RANDOM.VALUE(1,1000),0)
                         V_ACCOUNT_NUMBER,
                      I.N_AMOUNT_LCY N_AMOUNT_LCY1,
                      NULL N_ACCRUED_INTEREST,
                      I.V_CCY_CODE V_CCY_CODE1,
                      'PAK' V_COUNTRY_ID,
                      I.V_BRANCH_CODE V_BRANCH_CODE,
                      I.V_GL_CODE V_GL_CODE1,
                      I.V_LOB_CODE V_LOB_CODE,
                      I.V_PARTY_TYPE_CODE V_CUST_REF_CODE,
                      I.V_GAAP_CODE V_GAAP_CODE1,
                      I.V_EXP_PROD_CODE V_PROD_CODE1,
                      LD_MIS_DATE + 30 D_MATURITY_DATE,
                      NULL START_DATE,
                      NULL N_MTM_VALUE,
                      DECODE (I.V_TAGGED,
                              'DMY-CFI', 'MANUAL-DMY-CFI',
                              'DMY-CC', 'MANUAL-DMY-CC',
                              'DMY-HR', 'MANUAL-DMY-HR',
                              'MANUAL-CCnull')
                         V_DATA_ORIGIN,
                      NULL N_EXCHANGE_RATE,
                      'ABL' V_LV_CODE1,
                      NULL F_PAST_DUE_FLAG,
                      NULL N_PROVISION_AMOUNT,
                      NULL V_EXP_RCY_CODE,
                      'Y' F_AUTO_CANCELLATION_FLAG,
                      'Y' F_UNCOND_CANCELLED_EXP_IND,
                      NULL F_INST_SHORT_TERM_QUALIF_FLAG,
                      NULL V_BOOK_TYPE,
                      I.V_LV_CODE V_LINE_CODE,
                      NULL V_ISSUER_CODE,
                      NULL F_RECIPROCAL_CROSS_HLDG_IND,
                      1 N_REMARGIN_FREQUENCY,
                      1 N_REVALUATION_FREQUENCY,
                      NULL N_EOP_BAL_NPL,
                      NULL V_LOAN_CLASSIFICATION,
                      NULL N_INT_ACCRUED_MTD,
                      NULL V_PROV_GL_CODE,
                      NULL V_ACCR_INT_GL_CODE,
                      NULL N_UNDRAWN_AMT,
                      NULL F_MAIN_INDEX_EQUITY_FLAG,
                      NULL F_EQUITY_TRADED_FLAG,
                      NULL N_CURRENT_CREDIT_LIMIT,
                      DECODE(I.V_GL_CODE,'108012801-1304','51001','LIVEDB') V_BAL_TYPE,
                         I.V_PROD_CODE
                      || I.V_GL_CODE
                      || I.V_LV_CODE
                      || I.V_BRANCH_CODE
                      || 'ABL'
                      || LN_CNT
                         V_ORIG_ACCT_NO,
                      'PKR' V_LCY_CCY_CODE,
                      I.N_AMOUNT_LCY N_LCY_AMT,
                      NULL V_LEG_REP_CODE,
                      NULL V_TRADING_DESK_ID,
                      NULL N_NO_OF_UNITS,
                      NULL V_INSTRUMENT_POSITION,
                      NULL D_EFFECTIVE_DATE,
                      NULL D_ISSUE_DATE,
                      NULL D_NEXT_TO_LAST_COUPON_DATE,
                      NULL F_OTC_IND,
                      NULL N_COUPON_FREQUENCY,
                      NULL N_FACE_VALUE,
                      NULL N_COUPON_RATE,
                      NULL N_FLOATING_RATE_SPREAD,
                      NULL N_STRIKE_RATE_PRICE,
                      NULL V_BANK_INSTR_TYPE_CODE,
                      NULL V_BENCHMARK_CODE,
                      NULL V_INSTRUMENT_SHORT_NAME,
                      NULL V_CCY2_CODE,
                      NULL V_COUNTERPARTY_ID,
                      NULL V_COUNTERPARTY_RATING,
                      NULL V_DAY_COUNT_IND,
                      I.V_EXP_CATEGORY_CODE V_EXP_CATEGORY_CODE,
                      NULL V_INSTRUMENT_CODE,
                      NULL V_INSTR_TYPE_CODE,
                      NULL V_ISSUER_ID,
                      NULL V_ISSUER_NAME,
                      NULL V_ISSUER_TYPE,
                      NULL V_STOCK_INDEX_CODE,
                      NULL V_RATING_CODE,
                      NULL F_GP_FUND_CCY_SAME_IND,
                      NULL N_REDEMPTION_VALUE,
                      NULL N_SETTLEMENT_DAYS,
                      NULL V_BENCHMARK_CCY_CODE,
                      NULL V_HOLIDAY_ROLL_CONVENTION_CD,
                      NULL V_BENCHMARK_DAY_COUNTER,
                      I.V_PARENT_GL_CODE V_PARENT_GL_CODE,
                      I.V_EXP_PROD_CODE V_EXP_PROD_CODE
               FROM   DUAL;
       END IF;
    END LOOP;

    /* Provision Assignment for dummy exposures*/
   /*
   FOR J
   IN (  SELECT   *
           FROM   STG_PRODUCT_PROCESSOR
          WHERE   V_DATA_ORIGIN IN
                        ('MANUAL-DMY-CC', 'MANUAL-DMY-CFI', 'MANUAL-DMY-HR')
                  AND FIC_MIS_DATE = LD_MIS_DATE
       ORDER BY   V_DATA_ORIGIN)
   LOOP
      SELECT   SUM (ABS (NVL (N_EOP_BAL, 0)))
        INTO   LN_CC_EXP
        FROM   STG_PRODUCT_PROCESSOR
       WHERE   V_DATA_ORIGIN = 'MANUAL-DMY-CC' AND FIC_MIS_DATE = LD_MIS_DATE;

      SELECT   SUM (ABS (NVL (N_EOP_BAL, 0)))
        INTO   LN_HR_EXP
        FROM   STG_PRODUCT_PROCESSOR
       WHERE   V_DATA_ORIGIN = 'MANUAL-DMY-HR' AND FIC_MIS_DATE = LD_MIS_DATE;

      SELECT   SUM (ABS (NVL (N_EOP_BAL, 0)))
        INTO   LN_CFI_EXP
        FROM   STG_PRODUCT_PROCESSOR
       WHERE   V_DATA_ORIGIN = 'MANUAL-DMY-CFI'
               AND FIC_MIS_DATE = LD_MIS_DATE;

      SELECT   A.N_LCY_AMT
        INTO   LN_CC_PROV
        FROM   STG_PRODUCT_PROCESSOR A
       WHERE       V_DATA_ORIGIN IN ('RMG-PROV-REVAL-RMV')
               AND FIC_MIS_DATE = LD_MIS_DATE
               AND V_ACCOUNT_NUMBER LIKE '%CC-PRV-HO%';

      SELECT   A.N_LCY_AMT
        INTO   LN_HR_PROV
        FROM   STG_PRODUCT_PROCESSOR A
       WHERE       V_DATA_ORIGIN IN ('RMG-PROV-REVAL-RMV')
               AND FIC_MIS_DATE = LD_MIS_DATE
               AND V_ACCOUNT_NUMBER LIKE '%HR-PRV-HO%';

      SELECT   A.N_LCY_AMT
        INTO   LN_CFI_PROV
        FROM   STG_PRODUCT_PROCESSOR A
       WHERE       V_DATA_ORIGIN IN ('RMG-PROV-REVAL-RMV')
               AND FIC_MIS_DATE = LD_MIS_DATE
               AND V_ACCOUNT_NUMBER LIKE '%CAMS-PRV-HO%';

      IF J.V_DATA_ORIGIN = 'MANUAL-DMY-CC'
      THEN
         LN_PROV_UPD :=
            ROUND ( ( (ABS (J.N_EOP_BAL) / LN_CC_EXP) * LN_CC_PROV), 4);

         UPDATE   STG_PRODUCT_PROCESSOR PP
            SET   PP.N_PROVISION_AMOUNT = LN_PROV_UPD
          WHERE       PP.V_ACCOUNT_NUMBER = J.V_ACCOUNT_NUMBER
                  AND PP.FIC_MIS_DATE = J.FIC_MIS_DATE
                  AND PP.V_DATA_ORIGIN = J.V_DATA_ORIGIN
                  AND PP.V_GL_CODE = J.V_GL_CODE
                  AND PP.V_BRANCH_CODE = J.V_BRANCH_CODE;

         DBMS_OUTPUT.PUT_LINE(   'UPDATED ->'
                              || J.V_ACCOUNT_NUMBER
                              || '|'
                              || LN_PROV_UPD
                              || '|'
                              || J.N_EOP_BAL);
      ELSIF J.V_DATA_ORIGIN = 'MANUAL-DMY-CFI'
      THEN
         LN_PROV_UPD :=
            ROUND ( ( (ABS (J.N_EOP_BAL) / LN_CFI_EXP) * LN_CFI_PROV), 4);

         UPDATE   STG_PRODUCT_PROCESSOR PP
            SET   PP.N_PROVISION_AMOUNT = LN_PROV_UPD
          WHERE       PP.V_ACCOUNT_NUMBER = J.V_ACCOUNT_NUMBER
                  AND PP.FIC_MIS_DATE = J.FIC_MIS_DATE
                  AND PP.V_DATA_ORIGIN = J.V_DATA_ORIGIN
                  AND PP.V_GL_CODE = J.V_GL_CODE
                  AND PP.V_BRANCH_CODE = J.V_BRANCH_CODE;

         DBMS_OUTPUT.PUT_LINE(   'UPDATED ->'
                              || J.V_ACCOUNT_NUMBER
                              || '|'
                              || LN_PROV_UPD
                              || '|'
                              || J.N_EOP_BAL);
      ELSIF J.V_DATA_ORIGIN = 'MANUAL-DMY-HR'
      THEN
         LN_PROV_UPD :=
            ROUND ( ( (ABS (J.N_EOP_BAL) / LN_HR_EXP) * LN_HR_PROV), 4);

         UPDATE   STG_PRODUCT_PROCESSOR PP
            SET   PP.N_PROVISION_AMOUNT = LN_PROV_UPD
          WHERE       PP.V_ACCOUNT_NUMBER = J.V_ACCOUNT_NUMBER
                  AND PP.FIC_MIS_DATE = J.FIC_MIS_DATE
                  AND PP.V_DATA_ORIGIN = J.V_DATA_ORIGIN
                  AND PP.V_GL_CODE = J.V_GL_CODE
                  AND PP.V_BRANCH_CODE = J.V_BRANCH_CODE;

         DBMS_OUTPUT.PUT_LINE(   'UPDATED ->'
                              || J.V_ACCOUNT_NUMBER
                              || '|'
                              || LN_PROV_UPD
                              || '|'
                              || J.N_EOP_BAL);
      END IF;
   END LOOP; */

   /* RMG is providing provision/ revaluation data manually, In this data they are only providing Original Acct No. and  amount column. Other mandetory columns are updated by this code */

   FOR I
      IN (SELECT PP.V_DATA_ORIGIN,
                 PP.V_BAL_TYPE,
                 PP.V_ORIG_ACCT_NO,
                 PP.V_ACCOUNT_NUMBER,
                 PP.FIC_MIS_DATE
            FROM STG_PRODUCT_PROCESSOR PP
           WHERE     PP.FIC_MIS_DATE = LD_MIS_DATE
                 AND PP.V_DATA_ORIGIN = 'RMG-PROV-REVAL')
   LOOP
      DBMS_OUTPUT.PUT_LINE (
            ' EXPOSURE:'
         || I.V_ORIG_ACCT_NO
         || ' ========> '
         || I.V_ORIG_ACCT_NO
         || ' PROD:'
         || LV_PROD_CODE
         || ' CUST:'
         || LV_CUST_REF_CODE);

      SELECT NVL (
                (SELECT V_ORIG_ACCT_NO
                   FROM STG_PRODUCT_PROCESSOR PPI
                  WHERE     PPI.FIC_MIS_DATE = I.FIC_MIS_DATE
                        AND PPI.V_ORIG_ACCT_NO = I.V_ORIG_ACCT_NO
                        AND PPI.V_DATA_ORIGIN <> 'RMG-PROV-REVAL'
                        AND PPI.V_BAL_TYPE IN
                               (SELECT FBT.V_ASSET_TYPE
                                  FROM FSI_BALANCE_TYPES FBT
                                 WHERE FBT.V_BALANCE_TYPE =
                                          CASE
                                             WHEN (SELECT FBI.V_BALANCE_TYPE
                                                     FROM FSI_BALANCE_TYPES FBI
                                                    WHERE FBI.V_ASSET_TYPE =
                                                             PPI.V_BAL_TYPE) =
                                                     'EOP'
                                             THEN
                                                'EOP'
                                             ELSE
                                                'ACCRIN'
                                          END)
                        AND ROWNUM = 1),
                0)
        INTO LV_CHK_ORIG_ACCT_NO
        FROM DUAL;

      IF LV_CHK_ORIG_ACCT_NO <> '0'
      THEN
         SELECT DISTINCT PPI.V_PROD_CODE,
                         PPI.V_CUST_REF_CODE,
                         PPI.V_LOB_CODE,
                         PPI.V_EXP_CATEGORY_CODE
           INTO LV_PROD_CODE,
                LV_CUST_REF_CODE,
                LV_LOB_CODE,
                LV_EXP_CATEGORY_CODE
           FROM STG_PRODUCT_PROCESSOR PPI
          WHERE     PPI.FIC_MIS_DATE = I.FIC_MIS_DATE
                AND PPI.V_ORIG_ACCT_NO = I.V_ORIG_ACCT_NO
                AND PPI.V_DATA_ORIGIN <> 'RMG-PROV-REVAL'
                AND PPI.V_BAL_TYPE IN
                       (SELECT FBT.V_ASSET_TYPE
                          FROM FSI_BALANCE_TYPES FBT
                         WHERE FBT.V_BALANCE_TYPE =
                                  CASE
                                     WHEN (SELECT FBI.V_BALANCE_TYPE
                                             FROM FSI_BALANCE_TYPES FBI
                                            WHERE FBI.V_ASSET_TYPE =
                                                     PPI.V_BAL_TYPE) = 'EOP'
                                     THEN
                                        'EOP'
                                     ELSE
                                        'ACCRIN'
                                  END)
                AND ROWNUM = 1;

         DBMS_OUTPUT.PUT_LINE ('Before updating');

         UPDATE STG_PRODUCT_PROCESSOR PP
            SET PP.V_PROD_CODE = LV_PROD_CODE,
                PP.V_CUST_REF_CODE = LV_CUST_REF_CODE,
                PP.V_LOB_CODE = LV_LOB_CODE,
                PP.V_EXP_CATEGORY_CODE = LV_EXP_CATEGORY_CODE
          WHERE     PP.V_DATA_ORIGIN = 'RMG-PROV-REVAL'
                AND PP.FIC_MIS_DATE = I.FIC_MIS_DATE
                AND PP.V_ORIG_ACCT_NO = I.V_ORIG_ACCT_NO;
      END IF;
   END LOOP;

   COMMIT;


   /* June 2025 unblocking Certain Products of GL starting with 40% */


   UPDATE STG_PRODUCT_PROCESSOR
      SET F_EXPOSURE_ENABLED_IND = 'Y'
    WHERE     FIC_MIS_DATE = LD_MIS_DATE
          AND V_PROD_CODE IN
                 ('LD-21098-6122',
                  'LD-21098-6192',
                  'LD-21098-6181',
                  'PD-21072-6231');

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
END POPULATE_PP_FROMGL;
/
