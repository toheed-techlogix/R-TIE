CREATE OR REPLACE FUNCTION OFSMDM.TLX_LOAD_DELETE_OFSMDM (
   BatchId   IN VARCHAR2,
   MisDate   IN VARCHAR2)
   RETURN NUMBER
AS
   result        NUMBER;
   ld_mis_date   DATE := TO_DATE (MisDate, 'YYYYMMDD');
BEGIN
   BEGIN
      DELETE FROM STG_PRODUCT_PROCESSOR
            WHERE     fic_mis_date = ld_mis_date
                  AND V_LV_CODE IN ('ABL', 'AMC')
                  AND V_DATA_ORIGIN IN
                         ('EXCLUDE',
                          'MANUAL-ADVANCES',
                          'MANUAL-ADVTAX',
                          'MANUAL-BWB',
                          'MANUAL-CBB',
                          'MANUAL-FIXEDASSET',
                          'MANUAL-INVESTMENTS',
                          'MANUAL-INV',
                          'MANUAL-OTHASSETS',
                          'MANUAL-MISCELLANEOUS',
                          'MANUAL-CC&HR',
                          'MANUAL-DMY-HR',
                          'MANUAL-DMY-CFI',
                          'MANUAL-DMY-CC',
                          'MANUAL-OTHR',
                          'DMY-PROV-ADJ-EXP','MANUAL-RETAIL');

      DELETE FROM STG_GL_DATA
            WHERE     fic_mis_date = ld_mis_date
                  AND V_LV_CODE IN ('AMC', 'EXCH')
                  AND V_DATA_ORIGIN IN ('MAN-GL');

      COMMIT;

      DELETE FROM STG_OPS_ADJ_MISDATE_TLX
            WHERE fic_mis_date = ld_mis_date;

      COMMIT;


      DELETE FROM STG_OPS_RISK_DATA a
            WHERE     A.FIC_MIS_DATE = ld_mis_date
                  AND A.FIC_MIS_DATE <> '31-dec-2016'; --AND A.D_FINANCIAL_YEAR > '31-dec-2015';

      COMMIT;

      DELETE FROM STG_OPS_RISK_DATA ops
            WHERE     OPS.D_FINANCIAL_YEAR = ld_mis_date
                  AND OPS.FIC_MIS_DATE = '31-dec-2016';

      COMMIT;


      DELETE FROM STG_STANDARD_ACCT_HEAD
            WHERE fic_mis_date = ld_mis_date;

      COMMIT;


      DELETE FROM OFSMDM.STG_PRODUCT_PROCESSOR SPP
            WHERE     SPP.FIC_MIS_DATE = ld_mis_date
                  AND SPP.V_GL_CODE = '105020101-1103'
                  AND SPP.V_ACCOUNT_NUMBER LIKE '%-DMY'
                  AND SPP.V_ORIG_ACCT_NO LIKE '%-DMY'
                  AND SPP.V_CUST_REF_CODE = 'OTHR';

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line ('Function failed: TLX_LOAD_DELETE');
         RETURN 0;
   END;

   DBMS_OUTPUT.put_line ('Function successfull: TLX_LOAD_DELETE');
   RETURN 1;
END TLX_LOAD_DELETE_OFSMDM;
/
