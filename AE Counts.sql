--trying to get reasonable AE per quarter

WITH Opps as(
SELECT OPPORTUNITY_NAME, 
       v.OPPORTUNITY_ID,
       STAGE,
       OPPORTUNITY_AMOUNT as TAS,
       CASE WHEN TAS >= 1000000000 THEN 'Key'
            WHEN TAS >= 100000000 THEN 'Enterprise'
            WHEN TAS >= 5000000 THEN 'SMB' END as Segment,
       CLOSE_DATE,
       CREATED_DATE,
       LEAD_SOURCE,
       OPPORTUNITY_TYPE,
       OPPORTUNITY_OWNER,
       OPPORTUNITY_OWNER_ROLE,
       OPPORTUNITY_OWNER_SEGMENT,
       SALES_REPORTING_SEGMENT,
       MAPPED_PRODUCT_TYPE,
       PRODUCT_TYPE,
       EXACT_AOV, 
       LAUNCH_FLAG,
       OPPORTUNITY_CAPTURE_DATE,
       MERCHANT_ARI,
       SIDE_BY_SIDE,
       OPPORTUNITY_DISABLED_REASON,
       ACCOUNT_NAME,
       VERTICAL,
       INDUSTRY,
       ACCOUNT_TYPE,
       ACCOUNT_ID,
       ACCOUNT_SOURCE,
       ACCOUNT_ECOMMERCE_PLATFORM,
       SELF_SERVICE_FLAG,
       SHOPIFY_FLAG,
       NEW_MARKET_OPPORTUNITY_FLAG,
       FISCAL_YEAR_CLOSE_DATE,
       FISCAL_MONTH_CLOSE_DATE,
       FISCAL_QUARTER_CLOSE_DATE,
       FISCAL_QUARTER_NAME_CLOSE_DATE,
       CONCAT(FISCAL_QUARTER_NAME_CLOSE_DATE,'-',FISCAL_YEAR_CLOSE_DATE) as FQ_Close,
       CONCAT(FISCAL_QUARTER_NAME_CREATE_DATE,'-',FISCAL_YEAR_CREATE_DATE) as FQ_Create, 
       FISCAL_YEAR_CREATE_DATE,
       FISCAL_MONTH_CREATE_DATE, 
       FISCAL_QUARTER_CREATE_DATE,
       FISCAL_QUARTER_NAME_CREATE_DATE,
       PRE_OPP_DAYS,
       CREATED_DAYS,
       DISCOVERY_DAYS,
       PROD_DECISION_DAYS,
       PROPOSAL_DAYS,
       NEGOTIATION_DAYS,
       EST_GMV, 
       TOTAL_SALES_CYCLE_DAYS,
       td.OPPORTUNITY_EST_GMV_FY23,
       concat(d.fiscal_year, '-', d.fiscal_quarter) as FQ
FROM PROD__WORKSPACE__US.SCRATCH_T_REVENUEOPS.V_OPEN_SALES_OPP_REV_GMV_PERFORMANCE v 
LEFT JOIN (
            SELECT tdm.OPPORTUNITY_ID,
                   tdm.OPPORTUNITY_EST_GMV_FY23
            FROM prod__us.DBT_ANALYTICS.SALES_TERRITORY_DEAL_MART tdm 
            QUALIFY ROW_NUMBER() OVER (PARTITION BY OPPORTUNITY_ID order by OPPORTUNITY_ID DESC )=1) as td
ON v.opportunity_id = td.OPPORTUNITY_ID
LEFT JOIN prod__us.DBT_ANALYTICS.DATE_DIM d on date(v.created_date)=d.DATE_KEY
WHERE v.OPPORTUNITY_SALES_FLAG=TRUE 
    AND CREATED_DATE >= '2020-07-01' 
    AND CREATED_DATE <= '2022-10-01' 
    AND TAS >= 5000000
    AND OPPORTUNITY_OWNER_ROLE = 'Account Executive')

SELECT
CASE 
    WHEN OPPORTUNITY_OWNER_SEGMENT = 'Mid-Market' THEN 'SMB'
    ELSE OPPORTUNITY_OWNER_SEGMENT END as r_Segment,
FQ,
COUNT(distinct(OPPORTUNITY_OWNER))
FROM Opps
WHERE CREATED_DATE >= '2021-07-01' AND CREATED_DATE < '2022-10-01' 
AND OPPORTUNITY_OWNER_SEGMENT <> 'N/A'
--AND STAGE IN ('Closed Won (Signed)', 'Closed Won')
GROUP BY 1,2
ORDER BY 2,1