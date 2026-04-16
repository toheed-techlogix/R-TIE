CREATE OR REPLACE FUNCTION OFSMDM.TLX_LOB_MAPPING (
   BatchId                IN VARCHAR2,
   MisDate                IN VARCHAR2)
   RETURN NUMBER
AS
   result        NUMBER;
   ld_mis_date   DATE := TO_DATE (MisDate, 'YYYYMMDD');
   lob_code      VARCHAR2 (20);
   beta_value    NUMBER;
BEGIN
   BEGIN
      /* DBMS_OUTPUT.PUT_LINE('hello 1');
      LOOP
         select ORD.V_LOB_CODE, ORD.N_BETA_FACTOR into lob_code, beta_value from STG_OPS_RISK_DATA ord where ORD.D_FINANCIAL_YEAR = ld_mis_date;
       CASE
       WHEN lob_code = 'AMA' THEN
         beta_value := 0.12;
        WHEN lob_code = 'ASE' THEN
         beta_value := 0.15;
        WHEN lob_code = 'CAP' THEN
         beta_value := 0.12;
          WHEN lob_code = 'CBA' THEN
          beta_value := 0.15;
           WHEN lob_code = 'CFI' THEN
           beta_value := 0.18;
            WHEN lob_code = 'PSE' THEN
            beta_value := 0.18;
             WHEN lob_code = 'RBA' THEN
             beta_value := 0.15;
       ELSE
         beta_value := 0.18;
     END CASE;

          update STG_OPS_RISK_DATA a
          set a.N_BETA_FACTOR = beta_value
          where A.V_LOB_CODE = lob_code;
     END LOOP;     */


     /* UPDATE STG_PARTY_MASTER SPM
         SET SPM.F_HAS_BNK_SECTOR_EXP = 'Y'
       WHERE     SPM.V_PARTY_ID IN (SELECT V_CUST_REF_CODE
                                      FROM STG_BANK_SECTOR_EXPOSURES
                                     WHERE FIC_MIS_DATE = ld_mis_date)
             AND SPM.FIC_MIS_DATE = ld_mis_date;

      COMMIT;*/

      UPDATE STG_OPS_RISK_DATA OPS
         SET OPS.N_BETA_FACTOR =
                CASE
                   WHEN OPS.V_LOB_CODE = 'AMA' THEN 0.12
                   WHEN OPS.V_LOB_CODE = 'ASE' THEN 0.15
                   WHEN OPS.V_LOB_CODE = 'CAP' THEN 0.12
                   WHEN OPS.V_LOB_CODE = 'CBA' THEN 0.15
                   WHEN OPS.V_LOB_CODE = 'CFI' THEN 0.18
                   WHEN OPS.V_LOB_CODE = 'PSE' THEN 0.18
                   WHEN OPS.V_LOB_CODE = 'RBA' THEN 0.15
                   WHEN OPS.V_LOB_CODE = 'TSA' THEN 0.18
                END
       WHERE OPS.D_FINANCIAL_YEAR = ld_mis_date;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line ('Function failed: TLX_LOB_MAPPING');
         RETURN 0;
   END;

   DBMS_OUTPUT.put_line ('Function successfull: TLX_LOB_MAPPING');
   RETURN 1;
END TLX_LOB_MAPPING;
/
