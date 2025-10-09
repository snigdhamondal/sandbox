-- CTE: Get strategic DUNS and build DUNS list
WITH CTE_STRATEGIC_ACCOUNT_DUNS AS (
    SELECT 
        DUNS_NUMBER AS STRATEGIC_DUNS_NUMBER,
        NAME AS STRATEGIC_ACCOUNT_NAME,
        CONCAT_WS(
            ',',
            DUNS_NUMBER,
            IFNULL(PARENT_DUNS, ''),
            IFNULL(HEAD_QUARTERS_DUNS, ''),
            IFNULL(DOMESTIC_ULTIMATE_DUNS, ''),
            IFNULL(GLOBAL_ULTIMATE_DUNS, '')
        ) AS DUNS_LIST
    FROM ODS_MDM_DUNSORG
    WHERE URI = '40MsSLM'
),

-- CTE: Flatten DUNS list for each strategic account
CTE_STRATEGIC_DUNS_LIST AS (
    SELECT DISTINCT
        A.STRATEGIC_ACCOUNT_NAME,
        B.value AS STRATEGIC_DUNS_NUMBER
    FROM CTE_STRATEGIC_ACCOUNT_DUNS A
    CROSS JOIN TABLE(SPLIT_TO_TABLE(A.DUNS_LIST, ',')) B
    WHERE B.value IS NOT NULL AND LENGTH(B.value) > 0
),

-- CTE: Find all DUNS linked to strategic DUNS
CTE_FINAL_DUNS_LIST AS (
    SELECT DISTINCT
        O.DUNS_NUMBER,
        S.STRATEGIC_DUNS_NUMBER,
        S.STRATEGIC_ACCOUNT_NAME
    FROM ODS_MDM_DUNSORG O
    INNER JOIN CTE_STRATEGIC_DUNS_LIST S
        ON  O.GLOBAL_ULTIMATE_DUNS = S.STRATEGIC_DUNS_NUMBER
         OR O.DOMESTIC_ULTIMATE_DUNS = S.STRATEGIC_DUNS_NUMBER
         OR O.HEAD_QUARTERS_DUNS = S.STRATEGIC_DUNS_NUMBER
         OR O.PARENT_DUNS = S.STRATEGIC_DUNS_NUMBER
),

-- CTE: Accounts without strategic account, prioritized by crosswalk type
CTE_ACCOUNTS_WITHOUT_STRATEGIC AS (
    SELECT *
    FROM (
        SELECT 
            A.*, 
            AC.*,
            CASE 
                WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/Salesforce' THEN 1
                WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/CDH' THEN 2
                ELSE 3
            END AS crosswalk_priority,
            ROW_NUMBER() OVER (
                PARTITION BY A.URI 
                ORDER BY 
                    CASE 
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/Salesforce' THEN 1
                        WHEN AC.CROSSWALKS_TYPE = 'configuration/sources/CDH' THEN 2
                        ELSE 3
                    END
            ) AS rn
        FROM ODS_MDM_ACCOUNT A 
        INNER JOIN ODS_MDM_ACCOUNT_CROSSWALKS AC 
            ON A.URI = AC.URI 
        WHERE A.DUNS_NUMBER IS NOT NULL 
          -- AND A.STRATEGIC_ACCOUNT_ID IS NULL
    ) sub
    WHERE rn = 1
)

-- Final SELECT: Build relationship records
SELECT DISTINCT
    A.CROSSWALKS_VALUE AS Account_Identifier,
    SPLIT_PART(A.CROSSWALKS_TYPE, '/', 3) AS Account_Source,
    A.CROSSWALKS_SOURCE_TABLE AS Account_Source_Table,
    BC.CROSSWALKS_VALUE AS Strategic_Account_Identifier,
    'StrategicAccount' AS Strategic_Account_Source,
    '' AS Strategic_Account_Source_Table,
    A.CROSSWALKS_VALUE || '_' || BC.CROSSWALKS_VALUE AS Relationship_Identifier,
    'StrategicAccount' AS Relationship_Source,
    '' AS Relationship_Source_Table,
    'DUNS Match' AS Relationship_Type,
    CURRENT_TIMESTAMP AS Relationship_Created_Datetime,
    CURRENT_TIMESTAMP AS Relationship_Last_Updated_Datetime,
    NULL AS Relationship_Deleted_Datetime
FROM CTE_ACCOUNTS_WITHOUT_STRATEGIC A
INNER JOIN ODS_MDM_DUNSORG B
    ON A.DUNS_NUMBER = B.DUNS_NUMBER
INNER JOIN ODS_MDM_DUNSORG_CROSSWALKS BC
    ON B.URI = BC.URI
    -- AND BC.CROSSWALKS_TYPE = 'configuration/sources/StrategicAccount'
INNER JOIN CTE_FINAL_DUNS_LIST C
    ON A.DUNS_NUMBER = C.DUNS_NUMBER;