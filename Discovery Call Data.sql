-------Disco Call Dataset
WITH ACTIVITY_UNION_BLOCK AS (
    SELECT * FROM (
    SELECT t.id ACTIVITY_ID,
           u.NAME OWNER_NAME,
           t.TASKOWNERROLE__C OWNER_ROLE,
           u.PREVIOUS_ROLE__C OWNER_PREVIOUS_ROLE,
           u.PREVIOUS_ROLE_END_DATE__C OWNER_PREVIOUS_ROLE_END_DATE,
           t.ACTIVITYDATE DATE,
           t.SUBJECT SUBJECT,
           t.type ACTIVITY_TYPE,
           t.WHOID WHOID,
           t.WHATID WHATID,
           CASE WHEN left(WHATID,3)='001' THEN 'ACCOUNT_ID'
               WHEN left(WHATID,3)='006' THEN 'OPPORTUNITY_ID'
               --WHEN WHATID IS NULL THEN 'BLANK'
               ELSE WHATID END WHAT_ID_TYPE,
            CASE WHEN left(WHOID,3)='003' THEN 'CONTACT_ID'
               WHEN left(WHOID,3)='00Q' THEN 'LEAD_ID'
               --WHEN WHATID IS NULL THEN 'BLANK'
               ELSE WHOID END WHO_ID_TYPE,
               CASE WHEN left(WHATID,3)='001' THEN WHATID END as ACCOUNT_ID,
           CASE WHEN left(WHATID,3)='006' THEN WHATID END as OPPORTUNITY_ID,
            CASE WHEN left(WHOID,3)='003' THEN WHOID END as CONTACT_ID,
            CASE   WHEN left(WHOID,3)='00Q' THEN WHOID END AS LEAD_ID
    FROM SALESFORCE.RAW_TESTING.TASK t
             LEFT JOIN SALESFORCE.RAW_TESTING.USER u on t.OWNERID=u.id
             LEFT JOIN SALESFORCE.RAW_TESTING.USERROLE ur on u.USERROLEID=ur.id
    UNION ALL
    SELECT e.id ACTIVITY_ID,
           u.NAME OWNER_NAME,
           ur.NAME OWNER_ROLE,
           u.PREVIOUS_ROLE__C OWNER_PREVIOUS_ROLE,
           u.PREVIOUS_ROLE_END_DATE__C OWNER_PREVIOUS_ROLE_END_DATE,
           e.ACTIVITYDATE DATE,
           e.SUBJECT SUBJECT,
           e.TYPE ACTIVITY_TYPE,
           e.WHOID WHOID,
           e.WHATID WHATID,
           CASE WHEN left(WHATID,3)='001' THEN 'ACCOUNT_ID'
                WHEN left(WHATID,3)='006' THEN 'OPPORTUNITY_ID'
                --WHEN WHATID IS NULL THEN 'BLANK'
                ELSE WHATID END WHAT_ID_TYPE,
                       CASE WHEN left(WHOID,3)='003' THEN 'CONTACT_ID'
               WHEN left(WHOID,3)='00Q' THEN 'LEAD_ID'
               --WHEN WHATID IS NULL THEN 'BLANK'
               ELSE WHOID END WHO_ID_TYPE,
           CASE WHEN left(WHATID,3)='001' THEN WHATID END as ACCOUNT_ID,
           CASE WHEN left(WHATID,3)='006' THEN WHATID END as OPPORTUNITY_ID,
            CASE WHEN left(WHOID,3)='003' THEN WHOID END as CONTACT_ID,
            CASE WHEN left(WHOID,3)='00Q' THEN WHOID END AS LEAD_ID
         FROM SALESFORCE.RAW_TESTING.EVENT e
             LEFT JOIN SALESFORCE.RAW_TESTING.USER u on e.OWNERID=u.id
             LEFT JOIN SALESFORCE.RAW_TESTING.USERROLE ur on u.USERROLEID=ur.id) a
    ),
    
DISCOVERY_IDs AS (
    SELECT t.ID DISCOVERY_IDs,
           t.WHOID, ---pretty much all contacts
            CASE WHEN left(WHOID,3)='003' THEN WHOID END as CONTACT_ID,
            CASE WHEN left(WHOID,3)='00Q' THEN WHOID END AS LEAD_ID
    FROM SALESFORCE.RAW_TESTING.event t
    LEFT JOIN SALESFORCE.RAW_TESTING.USER ut on t.OWNERID=ut.ID
    LEFT JOIN SALESFORCE.RAW_TESTING.USERROLE urt on urt.id=ut.USERROLEID
    WHERE (t.SUBJECT ilike '%Discovery call%' OR t.SUBJECT ilike '%Conference Meeting%' OR t.SUBJECT ilike '%In-Person Meeting%')
    --Weston added or statements beyond account executive here
    AND (urt.NAME='BDA' or urt.NAME ilike '%account executive%' or urt.name LIKE '%Sales Manager%' 
         or urt.NAME = 'Head of Sales' or urt.NAME = 'BDA Manager')
   
---------------
    --Discovery Date    
    AND t.CREATEDDATE>='2021-07-01' AND t.createddate < '2022-10-01'
----------------    
    ),
    
ACTIVITIES AS (
    SELECT ac.*,
           a.id activity_account_id,
           c.id activity_contact_id,
           a.ACCOUNT_SEGMENT__C ACTIVITY_ACCOUNT_SEGMENT,
           a.VERTICAL__C ACTIVITY_ACCOUNT_VERTICAL
    FROM ACTIVITY_UNION_BLOCK ac
    LEFT JOIN prod__us.DBT_ANALYTICS.DATE_DIM d on ac.DATE=d.DATE_KEY
    LEFT JOIN SALESFORCE.RAW_TESTING.CONTACT c on c.id=ac.WHOID
    LEFT JOIN SALESFORCE.RAW_TESTING.ACCOUNT a on a.id=c.ACCOUNTID
    WHERE (OWNER_ROLE='BDA' OR (OWNER_PREVIOUS_ROLE='BDA' and DATE<=OWNER_PREVIOUS_ROLE_END_DATE)
    --Weston added lines below to include BDA and AE activity
                            OR OWNER_ROLE='Account Executive' OR (OWNER_PREVIOUS_ROLE='Account Executive' and DATE <=OWNER_PREVIOUS_ROLE_END_DATE)
                            OR OWNER_ROLE LIKE '%Sales Manager%' OR (OWNER_PREVIOUS_ROLE LIKE '%Sales Manager%' and DATE <=OWNER_PREVIOUS_ROLE_END_DATE)
                            OR OWNER_ROLE='Head of Sales' OR (OWNER_PREVIOUS_ROLE='Head of Sales' and DATE <=OWNER_PREVIOUS_ROLE_END_DATE)
                            OR OWNER_ROLE='BDA Manager' OR (OWNER_PREVIOUS_ROLE='BDA Manager' and DATE <=OWNER_PREVIOUS_ROLE_END_DATE))
    AND ACTIVITY_TYPE IN ('Linkedin','Outbound Call','Outbound Email')
    AND (SUBJECT IS NULL OR SUBJECT not ilike '%<<%' OR SUBJECT not ilike '%>>%')
-----------
 --Activity Date
      AND ac.date>='2021-07-01' AND ac.date<'2022-10-01'--d.FISCAL_YEAR>2020
------------
    )

    --probably want to use activity date
SELECT a.ACCOUNT_SEGMENT__C DISCO_ACCOUNT_SEGMENT,
       a.VERTICAL__C DISCO_ACCOUNT_VERTICAL,
       a.AMOUNT__C DISCO_ACCOUNT_TAS,
        t.ID DISCOVERY_IDs,
         t.CREATEDDATE DISCO_CREATE_DATE,
         v.OPPORTUNITY_ID DISCO_CREATED_OPP_ID,
         v.CREATED_DATE,
         --added two lines here for AE and BDA tracibility
         OWNER_NAME,
         OWNER_ROLE,
         --Weston added FY data
         d.fiscal_year,
         d.fiscal_quarter,
       CASE WHEN ac.ACTIVITY_ID is not null then DISCOVERY_IDs END AS ACTIVITY_DISCO_IDS,
       CASE WHEN ac.ACTIVITY_ID is not null then ac.activity_account_id END AS ACTIVITY_DISCO_ACCOUNT_IDS,
       CASE WHEN ac.ACTIVITY_ID is not null then ac.activity_contact_id END AS ACTIVITY_DISCO_CONTACT_IDS
    FROM SALESFORCE.RAW_TESTING.ACCOUNT a
    LEFT JOIN SALESFORCE.RAW_TESTING.event t on a.id=t.ACCOUNTID
    --Weston added date dim
    LEFT JOIN prod__us.DBT_ANALYTICS.DATE_DIM d on date(t.createddate) =d.DATE_KEY
    LEFT JOIN SALESFORCE.RAW_TESTING.USER ut on t.OWNERID=ut.ID
    LEFT JOIN SALESFORCE.RAW_TESTING.USERROLE urt on urt.id=ut.USERROLEID
    LEFT JOIN SALESFORCE.RAW_TESTING.CONTACT c on t.WHOID=c.id
    LEFT JOIN ACTIVITIES ac on c.id=ac.activity_contact_id
    LEFT JOIN PROD__WORKSPACE__US.SCRATCH_T_REVENUEOPS.V_OPEN_SALES_OPP_REV_GMV_PERFORMANCE v ON v.ACCOUNT_ID=a.id and v.CREATED_DATE>t.CREATEDDATE AND v.CREATED_DATE<DATEADD(month,6,t.CREATEDDATE) and v.OPPORTUNITY_SALES_FLAG=TRUE
    WHERE (t.SUBJECT ilike '%Discovery call%' OR t.SUBJECT ilike '%Conference Meeting%' OR t.SUBJECT ilike '%In-Person Meeting%')
---Weston added here to account for other Sales POC's
    AND (
        (urt.NAME='BDA' or urt.NAME ilike '%account executive%') 
         OR (ut.PREVIOUS_ROLE__C='Account Executive' AND t.CREATEDDATE<ut.PREVIOUS_ROLE_END_DATE__C)
         OR (urt.NAME LIKE '%Sales Manager%')
         OR (ut.PREVIOUS_ROLE__C LIKE '%Sales Manager' AND t.CREATEDDATE<ut.PREVIOUS_ROLE_END_DATE__C)
         OR (urt.NAME = 'Account Executive')
         OR (urt.NAME = 'Sales Manager')
         OR (ut.PREVIOUS_ROLE__C='Sales Manager' AND t.CREATEDDATE<ut.PREVIOUS_ROLE_END_DATE__C)
         OR (urt.NAME = 'Head of Sales')
         OR (ut.PREVIOUS_ROLE__C='Head of Sales' AND t.CREATEDDATE<ut.PREVIOUS_ROLE_END_DATE__C)
         OR (urt.NAME = 'BDA Manager')
         OR (ut.PREVIOUS_ROLE__C='BDA Manager' AND t.CREATEDDATE<ut.PREVIOUS_ROLE_END_DATE__C)
         )    
------------------------
--Disco Call Created Date
  AND t.CREATEDDATE>='2021-07-01' AND t.CREATEDDATE < '2022-10-01'
------------------------
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.id,v.OPPORTUNITY_ID ORDER BY c.CREATEDDATE ASC)=1
    
    
    
    
    
