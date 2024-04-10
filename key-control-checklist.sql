WITH
    date_scaffold AS (
        SELECT
            *
        FROM
            UNNEST (
                GENERATE_DATE_ARRAY(
                    DATE_ADD(CURRENT_DATE, INTERVAL - 5 DAY),
                    DATE_ADD(CURRENT_DATE, INTERVAL - 1 DAY)
                )
            ) AS DATE
    ),
    breakdown_scaffold AS (
        SELECT DISTINCT
            ErrorMessageID AS breakdown
        FROM
            revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary
        UNION DISTINCT
        SELECT DISTINCT
            ErrorMessageID AS breakdown
        FROM
            revenue-assurance-prod.ime_suspense.IME_Tableau_Summary
        UNION DISTINCT
        SELECT DISTINCT
            sap_exception AS breakdown
        FROM
            revenue-assurance-prod.control_a04q_rebill.alteryx_output
        WHERE
            sap_exception IN (
                'Exception, SAP data found but totals mismatch',
                'SAP data not found',
                'No SAP Exceptions'
            )
        UNION DISTINCT
    ),
    date_breakdown_scaffold AS (
        SELECT
            date_scaffold.date,
            breakdown_scaffold.breakdown
        FROM
            date_scaffold
            CROSS JOIN breakdown_scaffold
    ) f12m_data AS (
        SELECT
            "F12-M" AS `Control`,
            CAST(ErrorMessageID AS STRING) AS Breakdown,
            CountOfErrors AS COUNT,
            scafdate AS DATE,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    scaffold.date AS scafdate,
                    ErrorMessageID,
                    ChargeStartDate,
                    CountOfErrors,
                    RANK,
                    LAG(CountOfErrors) OVER (
                        PARTITION BY
                            ErrorMessageID
                        ORDER BY
                            ChargeStartDate
                    ) AS errors_yesterday,
                    CountOfErrors - LAG(CountOfErrors) OVER (
                        PARTITION BY
                            ErrorMessageID
                        ORDER BY
                            ChargeStartDate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        CountOfErrors - LAG(CountOfErrors) OVER (
                            PARTITION BY
                                ErrorMessageID
                            ORDER BY
                                ChargeStartDate
                        ),
                        LAG(CountOfErrors) OVER (
                            PARTITION BY
                                ErrorMessageID
                            ORDER BY
                                ChargeStartDate
                        )
                    ) * 100 AS pct_diff
                FROM
                    scaffold
                    LEFT JOIN revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary a ON scaffold.date = a.ChargeStartDate
            )
    ),
    ime01w_data AS (
        SELECT
            "IME01-W" AS `Control`,
            CAST(ErrorMessageID AS STRING) AS `Breakdown`,
            CountOfErrors AS `Count`,
            scafdate AS `Date`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    scaffold.date AS scafdate,
                    ErrorMessageID,
                    ChargeStartDate,
                    CountOfErrors,
                    RANK,
                    LAG(CountOfErrors) OVER (
                        PARTITION BY
                            ErrorMessageID
                        ORDER BY
                            ChargeStartDate
                    ) AS errors_yesterday,
                    CountOfErrors - LAG(CountOfErrors) OVER (
                        PARTITION BY
                            ErrorMessageID
                        ORDER BY
                            ChargeStartDate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        CountOfErrors - LAG(CountOfErrors) OVER (
                            PARTITION BY
                                ErrorMessageID
                            ORDER BY
                                ChargeStartDate
                        ),
                        LAG(CountOfErrors) OVER (
                            PARTITION BY
                                ErrorMessageID
                            ORDER BY
                                ChargeStartDate
                        )
                    ) * 100 AS pct_diff
                FROM
                    scaffold
                    LEFT JOIN revenue-assurance-prod.ime_suspense.IME_Tableau_Summary a ON scaffold.date = a.ChargeStartDate
            )
        WHERE
            pct_diff IS NOT NULL
    ),
    a04q_data AS (
        SELECT
            "A04-Q" AS `Control`,
            sap_exception AS `Breakdown`,
            control_count AS `Control Count`,
            scafdate AS `Date`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    scafdate,
                    control_count,
                    IFNULL(sap_exception, 'No SAP Exceptions') AS sap_exception,
                    LAG(control_count) OVER (
                        PARTITION BY
                            sap_exception
                        ORDER BY
                            scafdate
                    ) AS errors_yesterday,
                    control_count-LAG (control_count) OVER (
                        PARTITION BY
                            sap_exception
                        ORDER BY
                            scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        control_count-LAG (control_count) OVER (
                            PARTITION BY
                                sap_exception
                            ORDER BY
                                scafdate
                        ),
                        LAG(control_count) OVER (
                            PARTITION BY
                                sap_exception
                            ORDER BY
                                scafdate
                        )
                    ) * 100 AS pct_diff
                FROM
                    (
                        SELECT
                            scaffold.date AS scafdate,
                            sap_exception,
                            CAST(crc_created_on AS DATE) AS crc_created_on,
                            COUNTIF(
                                sap_exception IS NOT NULL
                                AND sap_exception IN (
                                    'Exception, SAP data found but totals mismatch',
                                    'SAP data not found'
                                )
                            ) AS control_count
                        FROM
                            scaffold
                            LEFT JOIN revenue-assurance-prod.control_a04q_rebill.alteryx_output a ON scaffold.date = CAST(a.crc_created_on AS DATE)
                        GROUP BY
                            scaffold.date,
                            sap_exception,
                            crc_created_on
                    )
            )
        WHERE
            sap_exception IN (
                'Exception, SAP data found but totals mismatch',
                'SAP data not found',
                'No SAP Exceptions'
            )
            OR sap_exception IS NULL
    )
SELECT
    *
FROM
    f12m_data
UNION ALL
SELECT
    *
FROM
    ime01w_data
UNION ALL
SELECT
    *
FROM
    a04q_data;