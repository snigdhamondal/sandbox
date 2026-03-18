-- CTE: Get strategic DUNS and build DUNS list
WITH
    CTE_STRATEGIC_ACCOUNT_DUNS AS (
        SELECT
            DO.DUNS_NUMBER AS STRATEGIC_DUNS_NUMBER,
            DO.NAME AS STRATEGIC_ACCOUNT_NAME,
            DOC.CROSSWALKS_VALUE
        FROM
            STG_EDW.ODS.ODS_MDM_DUNSORG DO
            INNER JOIN (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            URI,
                            CROSSWALKS_TYPE
                        ORDER BY
                            CROSSWALKS_CREATE_DATE ASC,
                            CROSSWALKS_UPDATE_DATE ASC
                    ) AS rn
                FROM
                    STG_EDW.ODS.ODS_MDM_DUNSORG_CROSSWALKS
            ) DOC ON DO.URI = DOC.URI
            AND DOC.rn = 1
            AND DOC.CROSSWALKS_TYPE = 'configuration/sources/StrategicAccount'
        WHERE
            DO.STRATEGIC_ACCOUNT_ID IS NOT NULL
            AND DOC.SYS_DEL_IND = 'N'
    ),
    -- CTE: Find all DUNS linked to strategic DUNS
    CTE_FINAL_DUNS_LIST AS (
        SELECT DISTINCT
            DO.DUNS_NUMBER,
            SDL.STRATEGIC_DUNS_NUMBER,
            SDL.STRATEGIC_ACCOUNT_NAME,
            SDL.CROSSWALKS_VALUE
        FROM
            STG_EDW.ODS.ODS_MDM_DUNSORG DO
            INNER JOIN CTE_STRATEGIC_ACCOUNT_DUNS SDL ON DO.GLOBAL_ULTIMATE_DUNS = SDL.STRATEGIC_DUNS_NUMBER
            OR DO.DOMESTIC_ULTIMATE_DUNS = SDL.STRATEGIC_DUNS_NUMBER
            OR DO.HEAD_QUARTERS_DUNS = SDL.STRATEGIC_DUNS_NUMBER
            OR DO.PARENT_DUNS = SDL.STRATEGIC_DUNS_NUMBER
    ),
    -- CTE: Accounts without strategic account, prioritized by crosswalk type
    CTE_REL_ACCT_WITHOUT_STRATEGIC AS (
        SELECT
            *
        FROM
            (
                SELECT
                    AC.CROSSWALKS_VALUE AS Account_Identifier,
                    SPLIT_PART (AC.CROSSWALKS_TYPE, '/', 3) AS Account_Source,
                    AC.CROSSWALKS_SOURCE_TABLE AS Account_Source_Table,
                    DF.CROSSWALKS_VALUE AS Strategic_Account_Identifier,
                    'StrategicAccount' AS Strategic_Account_Source,
                    '' AS Strategic_Account_Source_Table,
                    AC.CROSSWALKS_VALUE || '_' || DF.CROSSWALKS_VALUE AS Relationship_Identifier,
                    'StrategicAccount' AS Relationship_Source,
                    '' AS Relationship_Source_Table,
                    'DUNS Match' AS Relationship_Type,
                    TO_VARCHAR (
                        A.SRC_LAST_UPDATE_DTM,
                        'YYYY-MM-DD HH24:MI:SS.FF3'
                    ) AS Audit_Updatetime,
                    TO_VARCHAR (
                        CURRENT_TIMESTAMP(),
                        'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'
                    ) AS Relationship_Created_Datetime,
                    TO_VARCHAR (
                        CURRENT_TIMESTAMP(),
                        'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'
                    ) AS Relationship_Last_Updated_Datetime,
                    NULL AS Relationship_Deleted_Datetime,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            A.URI
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
                    INNER JOIN CTE_FINAL_DUNS_LIST DF ON A.DUNS_NUMBER = DF.DUNS_NUMBER
                    INNER JOIN STG_EDW.ODS.ODS_MDM_ACCOUNT_CROSSWALKS AC ON A.URI = AC.URI
                WHERE
                    A.DUNS_NUMBER IS NOT NULL
                    AND A.STRATEGIC_ACCOUNT_ID IS NULL
                    --AND A.SRC_LAST_UPDATE_DTM >= $$LastRun_Time
            ) sub
        WHERE
            rn = 1
    )
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