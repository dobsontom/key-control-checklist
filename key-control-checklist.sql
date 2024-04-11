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
            ) AS scafdate
    ),
    breakdown_scaffold AS (
        SELECT DISTINCT
            'F12-M' AS control,
            CAST(ErrorMessageID AS STRING) AS breakdown
        FROM
            revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary
        UNION DISTINCT
        SELECT DISTINCT
            'IME01-W' AS control,
            ErrorMessageID AS breakdown
        FROM
            revenue-assurance-prod.ime_suspense.IME_Tableau_Summary
        UNION DISTINCT
        SELECT DISTINCT
            'A04-Q' AS control,
            sap_exception AS breakdown
        FROM
            revenue-assurance-prod.control_a04q_rebill.alteryx_output
        WHERE
            sap_exception IN (
                'Exception, SAP data found but totals mismatch',
                'SAP data not found',
                'No SAP Exceptions'
            )
    ),
    date_breakdown_scaffold AS (
        SELECT
            breakdown_scaffold.control,
            date_scaffold.scafdate,
            breakdown_scaffold.breakdown
        FROM
            date_scaffold
            CROSS JOIN breakdown_scaffold
    ),
    f12m_data AS (
        SELECT
            "F12-M" AS `Control`,
            scafdate AS `Date`,
            breakdown AS `Breakdown`,
            CountOfErrors AS `Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    date_breakdown_scaffold.scafdate,
                    breakdown,
                    IFNULL(CountOfErrors, 0) AS CountOfErrors,
                    LAG(CountOfErrors) OVER (
                        PARTITION BY
                            breakdown
                        ORDER BY
                            date_breakdown_scaffold.scafdate
                    ) AS errors_yesterday,
                    CountOfErrors - LAG(CountOfErrors) OVER (
                        PARTITION BY
                            breakdown
                        ORDER BY
                            date_breakdown_scaffold.scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        CountOfErrors - LAG(CountOfErrors) OVER (
                            PARTITION BY
                                breakdown
                            ORDER BY
                                date_breakdown_scaffold.scafdate
                        ),
                        LAG(CountOfErrors) OVER (
                            PARTITION BY
                                breakdown
                            ORDER BY
                                date_breakdown_scaffold.scafdate
                        )
                    ) * 100 AS pct_diff
                FROM
                    date_breakdown_scaffold
                    LEFT JOIN revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary a ON date_breakdown_scaffold.scafdate = a.ChargeStartDate
                    AND date_breakdown_scaffold.breakdown = CAST(a.ErrorMessageID AS STRING)
                WHERE
                    date_breakdown_scaffold.control = 'F12-M'
            )
    ),
    ime01w_data AS (
        SELECT
            "IME01-W" AS `Control`,
            scafdate AS `Date`,
            breakdown AS `Breakdown`,
            CountOfErrors AS `Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    date_breakdown_scaffold.scafdate,
                    breakdown,
                    IFNULL(CountOfErrors, 0) AS CountOfErrors,
                    LAG(CountOfErrors) OVER (
                        PARTITION BY
                            breakdown
                        ORDER BY
                            date_breakdown_scaffold.scafdate
                    ) AS errors_yesterday,
                    CountOfErrors - LAG(CountOfErrors) OVER (
                        PARTITION BY
                            breakdown
                        ORDER BY
                            date_breakdown_scaffold.scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        CountOfErrors - LAG(CountOfErrors) OVER (
                            PARTITION BY
                                breakdown
                            ORDER BY
                                date_breakdown_scaffold.scafdate
                        ),
                        LAG(CountOfErrors) OVER (
                            PARTITION BY
                                breakdown
                            ORDER BY
                                date_breakdown_scaffold.scafdate
                        )
                    ) * 100 AS pct_diff
                FROM
                    date_breakdown_scaffold
                    RIGHT JOIN revenue-assurance-prod.ime_suspense.IME_Tableau_Summary a ON date_breakdown_scaffold.scafdate = a.ChargeStartDate
                    AND date_breakdown_scaffold.breakdown = a.ErrorMessageID
                WHERE
                    date_breakdown_scaffold.control = 'IME01-W'
            )
    ),
    a04q_data AS (
        SELECT
            "A04-Q" AS `Control`,
            scafdate AS `Date`,
            breakdown AS `Breakdown`,
            control_count AS `Control Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    scafdate,
                    control_count,
                    breakdown,
                    LAG(control_count) OVER (
                        PARTITION BY
                            breakdown
                        ORDER BY
                            scafdate
                    ) AS errors_yesterday,
                    control_count - LAG(control_count) OVER (
                        PARTITION BY
                            breakdown
                        ORDER BY
                            scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        control_count - LAG(control_count) OVER (
                            PARTITION BY
                                breakdown
                            ORDER BY
                                scafdate
                        ),
                        LAG(control_count) OVER (
                            PARTITION BY
                                breakdown
                            ORDER BY
                                scafdate
                        )
                    ) * 100 AS pct_diff
                FROM
                    (
                        SELECT
                            date_breakdown_scaffold.scafdate,
                            date_breakdown_scaffold.breakdown,
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
                            date_breakdown_scaffold
                            LEFT JOIN revenue-assurance-prod.control_a04q_rebill.alteryx_output a ON date_breakdown_scaffold.scafdate = CAST(a.crc_created_on AS DATE)
                            AND date_breakdown_scaffold.breakdown = a.sap_exception
                        WHERE
                            date_breakdown_scaffold.control = 'A04-Q'
                        GROUP BY
                            date_breakdown_scaffold.scafdate,
                            date_breakdown_scaffold.breakdown,
                            sap_exception,
                            crc_created_on
                    )
            )
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