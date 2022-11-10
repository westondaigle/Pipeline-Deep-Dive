---------------------
--Inbound Lead Analysis row level data
---------------------
WITH DISCOVERY_IDs AS (
    SELECT t.ID DISCOVERY_IDs,
            --Weston added disco date
           t.createddate as Disco_Create_Date,
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
    )
    
SELECT l.CREATEDDATE,
        dd.fiscal_year as lead_fiscal_year,
        dd.fiscal_quarter lead_fiscal_quarter,
       al.ACCOUNT_SEGMENT__C LEAD_ACCOUNT_SEGMENT1,
       al.VERTICAL__C LEAD_ACCOUNT_VERTICAL,
       l.MARKETING_QUALIFIED_DATE__C,
       --Weston added disco created date
       Disco_Create_Date,
       ddd.fiscal_year as disco_fiscal_year,
       ddd.fiscal_quarter as disco_fiscal_quarter,
       CASE
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='$5M-20M' THEN 10000000 --10M
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='>$75M' THEN 100000000 --100M use > or >= for inclusion here
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='$20M-75M' THEN 50000000 --50M
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C IS NULL THEN NULL
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='>$100M' THEN 105000000 --105M
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='$2M-5M' THEN 3000000 --3M
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='$1M-2M' THEN 2000000 --2M
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='$10M-25M' THEN 15000000 --15M
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='$5M-10M' THEN 7500000 --7.5M
        WHEN l.ANNUAL_SALES_THROUGH_WEBSITE__C='$25M-100M' THEN 75000000 --75M
        ELSE 0 END AS MAPPED_LEAD_TAS,
       CASE WHEN l.LEAD_ELIGIBILITY_STATUS__C='Eligible' THEN l.id END AS ELIGIBLE_LEAD_IDs,
       CASE WHEN l.PASSED_THROUGH_MQL_STATUS__C=TRUE THEN l.id END AS MQL_LEAD_ID,
       CASE WHEN url.NAME='BDA' THEN l.id END AS SAL_LEAD_ID,
       CASE WHEN c.PASSED_THROUGH_SQL_STATUS__C=TRUE THEN l.id END AS SQL_LEAD_ID,
       --May want to move discoery id and opp conversion to the next opp one.....
       d.DISCOVERY_IDs DISCOVERY_IDS_LEAD,
       CASE WHEN v.OPPORTUNITY_ID IS NOT NULL THEN l.id END TOTAL_CONVERTED_OPP_LEAD_ID, --may be better way to link this (revisit)
       CASE WHEN v.OPPORTUNITY_ID IS NOT NULL THEN /*l.id*/ v.OPPORTUNITY_ID END TOTAL_CONVERTED_OPP_OPP_ID, --may be better way to link this (revisit)
       CASE WHEN c.PASSED_THROUGH_SQL_STATUS__C=TRUE AND vao.OPPORTUNITY_ID /*AND c.PASSED_THROUGH_SQL_STATUS__C=TRUE*/ IS NOT NULL
        AND (l.EMAIL not ilike '%yahoo%' OR l.EMAIL not ilike '%aol%' OR l.EMAIL not ilike '%gmail%') THEN l.id END TOTAL_CONVERTED_ACC_OPP_LEAD_ID,
       v.OPPORTUNITY_ID as OPPORTUNITY_ID_LEAD_OPP,
       --vao.OPPORTUNITY_ID AS OPPORTUNITY_ID_LEAD_ACCOUNT,
       CASE WHEN c.PASSED_THROUGH_SQL_STATUS__C=TRUE AND vao.OPPORTUNITY_ID /*AND c.PASSED_THROUGH_SQL_STATUS__C=TRUE*/ IS NOT NULL
        AND (l.EMAIL not ilike '%yahoo%' AND l.EMAIL not ilike '%aol%' AND l.EMAIL not ilike '%gmail%') THEN vao.OPPORTUNITY_ID END AS TOTAL_CONVERTED_ACC_OPP_ID,
       l.LEADSOURCE,
       lm.LEAD_SOURCE_MAPPED,
       vao.OPPORTUNITY_ID AS OPPORTUNITY_ID_LEAD_ACCOUNT,
       CASE
        WHEN l.LEADSOURCE IN ('Paid Search', 'Paid Blended') THEN 'Paid Search'
        WHEN l.LEADSOURCE IN ('Direct traffic', 'Organic Search', 'Google Natural Search') THEN 'Organic'
        WHEN l.LEADSOURCE IN ('Display', 'skift_b2b', 'sharma_b2b') THEN 'Display'
        WHEN l.LEADSOURCE IN ('Email', 'affirm') THEN 'Email'
        WHEN l.LEADSOURCE IN ('Field Event') THEN 'Field Event'
        WHEN l.LEADSOURCE IN ('Paid Social', 'linkedin_b2b') THEN 'Paid Social'
        WHEN l.LEADSOURCE IN ('Partner') THEN 'Partner'
        WHEN l.LEADSOURCE IN ('LinkedIn', 'Facebook') THEN 'Organic Social'
        WHEN l.LEADSOURCE IN ('Webinar') THEN 'Webinar'
        WHEN l.LEADSOURCE IN ('osa_outreach') THEN 'Manually sourced'
        WHEN l.LEADSOURCE IN ('ZoomInfo') THEN 'ZoomInfo'
        WHEN l.LEADSOURCE IN ('Shopify') THEN 'Shopify'
        ELSE 'Other' END as Source_Condensed
    FROM SALESFORCE.RAW_TESTING.LEAD l
    LEFT JOIN SALESFORCE.RAW_TESTING.CONTACT c on l.CONVERTEDCONTACTID=c.id
    LEFT JOIN SALESFORCE.RAW_TESTING.OPPORTUNITYCONTACTROLE oc on l.CONVERTEDCONTACTID=oc.CONTACTID
    LEFT JOIN PROD__WORKSPACE__US.SCRATCH_T_REVENUEOPS.V_OPEN_SALES_OPP_REV_GMV_PERFORMANCE v ON v.OPPORTUNITY_ID=oc.OPPORTUNITYID and v.OPPORTUNITY_SALES_FLAG=TRUE
    --LEFT JOIN SALESFORCE.RAW_TESTING.OPPORTUNITY o on oc.OPPORTUNITYID=o.id
    LEFT JOIN PROD__WORKSPACE__US.SCRATCH_T_REVENUEOPS.LEAD_SOURCE_MAPPING_DIM lm on c.LEADSOURCE=lm.LEADSOURCE
    LEFT JOIN SALESFORCE.RAW_TESTING.ACCOUNT al on l.CONVERTEDACCOUNTID=al.id
    LEFT JOIN PROD__WORKSPACE__US.SCRATCH_T_REVENUEOPS.V_OPEN_SALES_OPP_REV_GMV_PERFORMANCE vao ON vao.ACCOUNT_ID=al.ID
                and vao.OPPORTUNITY_SALES_FLAG=TRUE and vao.EST_GMV<50000000
    LEFT JOIN SALESFORCE.RAW_TESTING.USER ul on l.OWNERID=ul.ID
    LEFT JOIN SALESFORCE.RAW_TESTING.USERROLE url on url.id=ul.USERROLEID
    LEFT JOIN DISCOVERY_IDs d on c.id=d.WHOID
    --Weston add FY data
    LEFT JOIN prod__us.DBT_ANALYTICS.DATE_DIM dd on date(l.CREATEDDATE)=dd.DATE_KEY
    LEFT JOIN prod__us.DBT_ANALYTICS.DATE_DIM ddd on date(Disco_Create_Date)=ddd.DATE_KEY
    WHERE l.AFFIRM_BUSINESS_UNIT__C='US' AND (l.LEADSOURCE IS NULL OR l.LEADSOURCE not ilike '%paybright%')
    AND l.CURRENT_PURCHASE_PATH__C ilike 'sales' and al.SELF_SERVICE__C=false--MSS Filter?
    AND l.EMAIL not ilike '%@affirm.com'
--------------
--Lead Update Date
    AND l.CREATEDDATE>='2021-07-01' AND l.CREATEDDATE <'2022-10-01'
--------------
