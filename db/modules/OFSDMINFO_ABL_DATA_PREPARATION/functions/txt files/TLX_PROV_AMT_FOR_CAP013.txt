CREATE OR REPLACE FUNCTION OFSMDM.TLX_PROV_AMT_FOR_CAP013(
   BatchId                IN VARCHAR2,
   MisDate                IN VARCHAR2
)
   RETURN NUMBER
AS
   result        NUMBER;
   ld_mis_date   DATE := TO_DATE (MisDate, 'YYYYMMDD');
   n_balance_TMP     NUMBER;
   AMOUNT NUMBER;
BEGIN
   
  begin 
   DBMS_OUTPUT.put_line (ld_mis_date);
   SELECT   sum(a.N_PROVISION_SHORTFALL) into n_balance_TMP
  FROM   STG_PRODUCT_PROCESSOR a
 WHERE       a.N_PROVISION_SHORTFALL IS NOT NULL
         AND A.F_PAST_DUE_FLAG = 'Y'
        -- AND a.V_ACCOUNT_NUMBER = 'LD1323300008-PROVAMT'
         AND A.FIC_MIS_DATE = ld_mis_date;
         
         DBMS_OUTPUT.put_line (n_balance_TMP);
         
         EXCEPTION
      WHEN NO_DATA_FOUND THEN
        n_balance_TMP := 0;
      end;   
        update SETUP_BANK_CAPITAL_DTL
   set N_BALANCE = n_balance_TMP
   where V_CAP_HEAD_CD = 'ABL_CAP013';

SELECT N_BALANCE INTO AMOUNT
 FROM SETUP_BANK_CAPITAL_DTL where V_CAP_HEAD_CD = 'ABL_CAP013';

DBMS_OUTPUT.put_line('AMOUNT' || AMOUNT);
   COMMIT;
   DBMS_OUTPUT.put_line ('Function successfull: TLX_PROV_AMT_FOR_CAP013');   
   RETURN 1;

   /*INSERT INTO STG_STANDARD_ACCT_HEAD(
   FIC_MIS_DATE,
N_AMOUNT_RCY,
V_GAAP_CODE,
V_LV_CODE,
V_RCY_CODE,
V_STD_ACCT_HEAD_ID,
F_CONSOLIDATION_FLAG,
V_COUNTERPARTY_CODE,
N_AMOUNT,
V_CCY_CODE)
Values(
       '31-DEC-2016',
        n_balance,
       'PKGAAP',
       'ABL',
       'PKR',
       'ABL_CAP013',
       'Y',
       null,
       n_balance,
       'PKR'
      );

      COMMIT;*/
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line ('Function failed: TLX_PROV_AMT_FOR_CAP013');
         RETURN 0;
   

   
END TLX_PROV_AMT_FOR_CAP013;
/
