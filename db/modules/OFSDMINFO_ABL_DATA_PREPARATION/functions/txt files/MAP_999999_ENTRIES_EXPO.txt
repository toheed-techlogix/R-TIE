CREATE OR REPLACE FUNCTION OFSMDM.MAPPING_999999_ENTRIES_EXPO (
   BatchId                IN VARCHAR2,
   MisDate                IN VARCHAR2)
  
   RETURN NUMBER
AS
   result        NUMBER;
   ld_mis_date   DATE := TO_DATE (MisDate, 'YYYYMMDD');
BEGIN
   BEGIN
      MERGE INTO OFSMDM.MAPPING_99999_TLX MAP
           USING (SELECT DISTINCT
                         PP.V_ORIG_ACCT_NO,
                         PP.V_INSTRUMENT_CODE,
                         pp.v_cust_ref_code
                    FROM STG_PRODUCT_PROCESSOR PP
                   WHERE     PP.FIC_MIS_DATE = ld_mis_date
                         AND PP.V_CUST_REF_CODE IN ('999999', '999889')
                         AND PP.V_ORIG_ACCT_NO NOT IN
                                (SELECT MP.V_ACCOUNT_NUMBER
                                   FROM OFSMDM.MAPPING_99999_TLX MP)) y
              ON (MAP.V_ACCOUNT_NUMBER = y.V_ORIG_ACCT_NO)
      WHEN NOT MATCHED
      THEN
         INSERT     (MAP.V_ACCOUNT_NUMBER,
                     MAP.V_INSTRUMENT_CODE,
                     MAP.V_PARTY_ID)
             VALUES (
                       Y.V_ORIG_ACCT_NO,
                       Y.V_INSTRUMENT_CODE,
                       Y.V_CUST_REF_CODE);

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (
            'Function failed: MAPPING_999999_ENTRIES_EXPO');
         RETURN 0;
   END;

   DBMS_OUTPUT.put_line ('Function successful: MAPPING_999999_ENTRIES_EXPO');
   RETURN 1;
END MAPPING_999999_ENTRIES_EXPO;
/
