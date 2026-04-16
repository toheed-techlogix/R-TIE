CREATE OR REPLACE FUNCTION OFSMDM.MAPPING_PARTY_TLX_EXPO (
   BatchId                IN VARCHAR2,
   MisDate                IN VARCHAR2
)
   RETURN NUMBER
AS
   result        NUMBER;
   ld_mis_date   DATE := TO_DATE (MisDate, 'YYYYMMDD');
BEGIN
   BEGIN
      MERGE INTO   OFSMDM.mapping_party_type MAP
           USING   (SELECT   DISTINCT pm.v_party_id,pm.v_party_name,pm.V_TYPE
                      FROM   STG_PARTY_MASTER PM
                     WHERE   PM.FIC_MIS_DATE = ld_mis_date
                             --AND PP.V_CUST_REF_CODE IN ('999999','999889')
                             AND PM.v_party_id NOT IN
                                      (SELECT   MP.V_PARTY_ID
                                         FROM   OFSMDM.mapping_party_type MP))
                   y
              ON   (MAP.V_PARTY_ID = y.V_PARTY_ID)
      WHEN NOT MATCHED
      THEN
         INSERT              (map.v_party_id,map.v_party_name,map.v_party_type)
             VALUES   (Y.V_PARTY_ID, Y.v_party_name,Y.V_TYPE);

      COMMIT;
      
      
     
      
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line ('Function failed: MAPPING_PARTY_TLX_EXPO');
         RETURN 0;
   END;

   DBMS_OUTPUT.put_line ('Function successful: MAPPING_PARTY_TLX_EXPO');
   RETURN 1;
END MAPPING_PARTY_TLX_EXPO;
/
