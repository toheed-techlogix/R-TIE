CREATE OR REPLACE FUNCTION OFSMDM.POPULATE_PP_FROMGL_AMC (
   BatchId                IN VARCHAR2,
   MisDate                IN VARCHAR2
)
   RETURN NUMBER
IS
   result        NUMBER;
   ld_mis_date   DATE := TO_DATE (MisDate, 'YYYYMMDD');
BEGIN
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
                                        B.F_EXPOSURE_ENABLED_IND)
      SELECT   GL.FIC_MIS_DATE FIC_MIS_DATE1,
                  GL.V_PROD_CODE
               || GL.V_GL_CODE
               || GL.V_LV_CODE
               || GL.V_BRANCH_CODE
               || 'AMC'
                  V_ACCOUNT_NUMBER,
               GL.N_AMOUNT_LCY N_AMOUNT_LCY1,
               NULL N_ACCRUED_INTEREST,
               GL.V_CCY_CODE V_CCY_CODE1,
               'PAK' V_COUNTRY_ID,
               'PK0035001' V_BRANCH_CODE,
               GL.V_GL_CODE V_GL_CODE1,
               BNKDT.V_LOB_CODE V_LOB_CODE,
               BNKDT.V_PARTY_TYPE_CODE V_CUST_REF_CODE,
               GL.V_GAAP_CODE V_GAAP_CODE1,
               BNKDT.V_EXP_PROD_CODE V_PROD_CODE1,
               CASE
                  WHEN bnkdt.V_GL_HEAD_CATEGORY = 'Fixed Assets'
                  THEN
                     ld_mis_date + 15
                  ELSE
                     ld_mis_date + 80
               END
                  D_MATURITY_DATE,
               NULL START_DATE,
               NULL N_MTM_VALUE,
               CASE
                  WHEN gl.V_GL_CODE IN
                             ('106020101-0000',
                              '206020101-0000',
                              '106010112-0000',
                              '106010113-0000',
                              '108100178-0000',
                              '205080402-0000')
                  THEN
                     'EXCLUDE'
                  WHEN bnkdt.V_GL_HEAD_CATEGORY = 'Fixed Assets'
                  THEN
                     'MANUAL-FIXEDASSET'
                  ELSE
                     'MANUAL-INV'
               END
                  V_DATA_ORIGIN,
               NULL N_EXCHANGE_RATE,
               'AMC' V_LV_CODE1,
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
               || 'AMC'
                  V_ORIG_ACCT_NO,
               COALESCE(GL.V_CCY_CODE,'PKR') V_LCY_CCY_CODE,
               GL.N_AMOUNT_LCY N_LCY_AMT,
               NULL V_LEG_REP_CODE,
               NULL V_TRADING_DESK_ID,
               GL.N_AMOUNT_LCY N_NO_OF_UNITS,
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
               GL.V_GL_CODE
               || GL.V_LV_CODE V_INSTRUMENT_CODE,
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
                  WHEN gl.v_gl_code = '207010403-0000'
                  THEN
                     'Y'
                  ELSE
                   trim(BNKDT.F_EXPOSURE_ENABLED_IND)---before it's using BNKDT.F_EXPOSURE_ENABLED_IND trim function used to remove spaces 
               END
               F_EXPOSURE_ENABLED_IND
        FROM      STG_GL_DATA GL
               INNER JOIN
                  STG_BNK_COA_DETAILS BNKDT
               ON GL.V_GL_CODE = BNKDT.V_GL_CODE
       WHERE   GL.FIC_MIS_DATE = ld_mis_date AND gl.v_lv_code = 'AMC';

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
END POPULATE_PP_FROMGL_AMC;
/
