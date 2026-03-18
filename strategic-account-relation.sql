-- =====================================================================
-- CTE: Get strategic DUNS and strategic account identifier
-- =====================================================================
WITH CTE_STRATEGIC_ACCOUNT_DUNS AS (
    SELECT
        DO.DUNS_NUMBER AS STRATEGIC_DUNS_NUMBER,
        DO.NAME AS STRATEGIC_ACCOUNT_NAME,
        DO.MSA_ID,
        DOC.CROSSWALKS_VALUE AS STRATEGIC_ACCOUNT_IDENTIFIER
    FROM
        STG_EDW.ODS.ODS_MDM_DUNSORG DO
        INNER JOIN (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY URI, CROSSWALKS_TYPE
                    ORDER BY CROSSWALKS_CREATE_DATE ASC, CROSSWALKS_UPDATE_DATE ASC
                ) AS rn
            FROM
                STG_EDW.ODS.ODS_MDM_DUNSORG_CROSSWALKS
        ) DOC
            ON DO.URI = DOC.URI
           AND DOC.rn = 1
           AND DOC.CROSSWALKS_TYPE = 'configuration/sources/StrategicAccount'
    WHERE
        DO.STRATEGIC_ACCOUNT_ID IS NOT NULL
        AND DOC.SYS_DEL_IND = 'N'
),
-- =====================================================================
-- CTE: Match account to strategic account through hierarchy DUNS fields
-- =====================================================================
CTE_ACCOUNT_DUNS_MATCH AS (
    SELECT DISTINCT
        A.URI,
        A.SRC_LAST_UPDATE_DTM,
        SD.STRATEGIC_ACCOUNT_IDENTIFIER,
        1 AS DUNS_MATCH_FLG,
        0 AS MSA_MATCH_FLG
    FROM
        STG_EDW.ODS.ODS_MDM_ACCOUNT A
        INNER JOIN CTE_STRATEGIC_ACCOUNT_DUNS SD
            ON A.PARENT_DUNS = SD.STRATEGIC_DUNS_NUMBER
            OR A.HEADQUARTERS_DUNS = SD.STRATEGIC_DUNS_NUMBER
            OR A.DOMESTIC_ULTIMATE_DUNS = SD.STRATEGIC_DUNS_NUMBER
            OR A.GLOBAL_ULTIMATE_DUNS = SD.STRATEGIC_DUNS_NUMBER
    WHERE
        A.STRATEGIC_ACCOUNT_ID IS NULL
        --AND A.SRC_LAST_UPDATE_DTM >= $$LastRun_Time
),
-- =====================================================================
-- CTE: Match account to strategic account through MSA_ID
-- =====================================================================
CTE_ACCOUNT_MSA_MATCH AS (
    SELECT DISTINCT
        A.URI,
        A.SRC_LAST_UPDATE_DTM,
        SD.STRATEGIC_ACCOUNT_IDENTIFIER,
        0 AS DUNS_MATCH_FLG,
        1 AS MSA_MATCH_FLG
    FROM
        STG_EDW.ODS.ODS_MDM_ACCOUNT A
        INNER JOIN CTE_STRATEGIC_ACCOUNT_DUNS SD
            ON A.MSA_ID = SD.MSA_ID
    WHERE
        A.STRATEGIC_ACCOUNT_ID IS NULL
        --AND A.SRC_LAST_UPDATE_DTM >= $$LastRun_Time
),
-- =====================================================================
-- CTE: Consolidate DUNS and MSA matches per account and strategic account
-- =====================================================================
CTE_ACCOUNT_MATCH_CONSOLIDATED AS (
    SELECT
        MATCHES.URI,
        MATCHES.STRATEGIC_ACCOUNT_IDENTIFIER,
        MAX(MATCHES.SRC_LAST_UPDATE_DTM) AS SRC_LAST_UPDATE_DTM,
        MAX(MATCHES.DUNS_MATCH_FLG) AS DUNS_MATCH_FLG,
        MAX(MATCHES.MSA_MATCH_FLG) AS MSA_MATCH_FLG
    FROM
        (
            SELECT * FROM CTE_ACCOUNT_DUNS_MATCH
            UNION ALL
            SELECT * FROM CTE_ACCOUNT_MSA_MATCH
        ) MATCHES
    GROUP BY
        MATCHES.URI,
        MATCHES.STRATEGIC_ACCOUNT_IDENTIFIER
),
-- =====================================================================
-- CTE: Build relationship output and prioritize account crosswalk source
-- =====================================================================
CTE_REL_ACCT_WITHOUT_STRATEGIC AS (
    SELECT *
    FROM (
        SELECT
            AC.CROSSWALKS_VALUE AS Account_Identifier,
            SPLIT_PART(AC.CROSSWALKS_TYPE, '/', 3) AS Account_Source,
            AC.CROSSWALKS_SOURCE_TABLE AS Account_Source_Table,
            CM.STRATEGIC_ACCOUNT_IDENTIFIER AS Strategic_Account_Identifier,
            'StrategicAccount' AS Strategic_Account_Source,
            '' AS Strategic_Account_Source_Table,
            AC.CROSSWALKS_VALUE || '_' || CM.STRATEGIC_ACCOUNT_IDENTIFIER AS Relationship_Identifier,
            'StrategicAccount' AS Relationship_Source,
            '' AS Relationship_Source_Table,
            CASE
                WHEN CM.DUNS_MATCH_FLG = 1 AND CM.MSA_MATCH_FLG = 1
                THEN 'DUNS Match and MSA Match'
                WHEN CM.DUNS_MATCH_FLG = 1 AND CM.MSA_MATCH_FLG = 0
                THEN 'DUNS Match but No MSA Match'
                WHEN CM.DUNS_MATCH_FLG = 0 AND CM.MSA_MATCH_FLG = 1
                THEN 'No DUNS Match but MSA Match'
            END AS Relationship_Type,
            TO_VARCHAR(CM.SRC_LAST_UPDATE_DTM, 'YYYY-MM-DD HH24:MI:SS.FF3') AS Audit_Updatetime,
            TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS Relationship_Created_Datetime,
            TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS Relationship_Last_Updated_Datetime,
            NULL AS Relationship_Deleted_Datetime,
            ROW_NUMBER() OVER (
                PARTITION BY A.URI, CM.STRATEGIC_ACCOUNT_IDENTIFIER
                ORDER BY
                    CASE
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/Salesforce' THEN 1
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/CDH' THEN 2
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/TRUX' THEN 3
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/D365' THEN 4
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/EQAI' THEN 5
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/ES_Salesforce' THEN 6
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/BASIS' THEN 7
                        ELSE 99
                    END
            ) AS rn
        FROM
            STG_EDW.ODS.ODS_MDM_ACCOUNT A
            INNER JOIN CTE_ACCOUNT_MATCH_CONSOLIDATED CM
                ON A.URI = CM.URI
            INNER JOIN STG_EDW.ODS.ODS_MDM_ACCOUNT_CROSSWALKS AC
                ON A.URI = AC.URI
        WHERE
            A.STRATEGIC_ACCOUNT_ID IS NULL
            --AND A.SRC_LAST_UPDATE_DTM >= $$LastRun_Time
    ) sub
    WHERE rn = 1
)

-- =====================================================================
-- FINAL SELECT
-- =====================================================================
SELECT
    R.Account_Identifier,
    R.Account_Source,
    R.Account_Source_Table,
    R.Strategic_Account_Identifier,
    R.Strategic_Account_Source,
    R.Strategic_Account_Source_Table,
    R.Relationship_Identifier,
    R.Relationship_Source,
    R.Relationship_Source_Table,
    R.Relationship_Type,
    R.Audit_Updatetime,
    R.Relationship_Created_Datetime,
    R.Relationship_Last_Updated_Datetime,
    R.Relationship_Deleted_Datetime
FROM
    CTE_REL_ACCT_WITHOUT_STRATEGIC R;