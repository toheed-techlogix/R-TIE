CREATE OR REPLACE FUNCTION OFSMDM.FN_UPDATE_RATING_CODE (
   p_v_batch_id            VARCHAR2,
   p_v_mis_date            VARCHAR2
 )
   RETURN VARCHAR2
AS
   ld_mis_date            DATE := TO_DATE (p_v_mis_date, 'YYYYMMDD');
 
   BANK_INSTR_TYPE_CODE   VARCHAR2 (64);
   SKEY                   NUMBER (5);
BEGIN
   MERGE INTO stg_product_processor B
        USING (SELECT v_instrument_code,
                      v_issuer_type,
                      v_rating_code,
                      --                          N_COUPON_RATE,
                      N_FLOATING_RATE_SPREAD
                 FROM MR_RATING_CODE_MAP) A
           ON (    A.V_INSTRUMENT_CODE = B.V_INSTRUMENT_CODE
               AND B.FIC_MIS_DATE = ld_mis_date) ---- Date to be replaced by MIS date----
   WHEN MATCHED
   THEN
      UPDATE SET
         B.v_issuer_type = A.v_issuer_type,
         B.v_rating_code = A.v_rating_code,
         --         B.N_COUPON_RATE = A.N_COUPON_RATE,
         B.N_FLOATING_RATE_SPREAD = A.N_FLOATING_RATE_SPREAD;

   --      WHEN NOT MATCHED
   --      THEN
   --     DBMS_OUTPUT.put_line ('No Need to Update');
   COMMIT;
   RETURN '1';
EXCEPTION
   WHEN OTHERS
   THEN
      DBMS_OUTPUT.put_line ('Human Bomb');
      ROLLBACK;
      RETURN '0';
END FN_UPDATE_RATING_CODE;
/
