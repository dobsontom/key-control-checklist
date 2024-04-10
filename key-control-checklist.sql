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
            CAST(ErrorMessageID AS STRING) AS Breakdown,
            scafdate AS `Date`,
            CountOfErrors AS COUNT,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    date_breakdown_scaffold.scafdate,
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
                    date_breakdown_scaffold
                    LEFT JOIN revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary a ON date_breakdown_scaffold.scafdate = a.ChargeStartDate
            )
    ),
    ime01w_data AS (
        SELECT
            "IME01-W" AS `Control`,
            CAST(ErrorMessageID AS STRING) AS `Breakdown`,
            scafdate AS `Date`,
            CountOfErrors AS `Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    date_breakdown_scaffold.scafdate,
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
                    date_breakdown_scaffold
                    LEFT JOIN revenue-assurance-prod.ime_suspense.IME_Tableau_Summary a ON date_breakdown_scaffold.scafdate = a.ChargeStartDate
            )
        WHERE
            pct_diff IS NOT NULL
    ),
    a04q_data AS (
        SELECT
            "A04-Q" AS `Control`,
            breakdown AS `Breakdown`,
            scafdate AS `Date`,
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
                    control_count-LAG (control_count) OVER (
                        PARTITION BY
                            breakdown
                        ORDER BY
                            scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        control_count-LAG (control_count) OVER (
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