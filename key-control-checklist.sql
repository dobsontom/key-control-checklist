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
            'A04-Q' AS control,
            sap_exception AS breakdown,
            'Control Count' AS metric
        FROM
            revenue-assurance-prod.control_a04q_rebill.alteryx_output
        WHERE
            sap_exception IN (
                'Exception, SAP data found but totals mismatch',
                'SAP data not found',
                'No SAP Exceptions'
            )
        UNION DISTINCT
        SELECT DISTINCT
            'F12-M' AS control,
            CAST(ErrorMessageID AS STRING) AS breakdown,
            'Count of Errors' AS metric
        FROM
            revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary
        UNION DISTINCT
        SELECT DISTINCT
            'IME01-W' AS control,
            ErrorMessageID AS breakdown,
            'Count of Errors' AS metric
        FROM
            revenue-assurance-prod.ime_suspense.IME_Tableau_Summary
        UNION DISTINCT
        SELECT DISTINCT
            'IME02-W' AS control,
            CONCAT(traffic_type, ' - ', IME_AcquisitionPortal) AS breakdown,
            metric
        FROM
            revenue-assurance-prod.control_ime_sv.IME_SV_Summary
            CROSS JOIN (
                SELECT DISTINCT
                    metric
                FROM
                    revenue-assurance-prod.control_ime_sv.IME_SV_Summary UNPIVOT(
                        control_count
                        FOR metric IN (
                            files_collected,
                            IME_TotRecsRecvd,
                            IME_v_SV_difference
                        )
                    )
            )
    ),
    date_breakdown_scaffold AS (
        SELECT
            breakdown_scaffold.control,
            date_scaffold.scafdate,
            breakdown_scaffold.breakdown,
            breakdown_scaffold.metric
        FROM
            date_scaffold
            CROSS JOIN breakdown_scaffold
    ),
    a04q_data AS (
        SELECT
            "A04-Q" AS `Control`,
            scafdate AS `Date`,
            last_refresh AS `Last Refresh`,
            breakdown AS `Breakdown`,
            'Control Count' AS `Metric`,
            control_count AS `Control Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    scafdate,
                    last_refresh,
                    breakdown,
                    control_count,
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
                            (
                                SELECT
                                    MAX(CAST(crc_created_on AS DATE))
                                FROM
                                    revenue-assurance-prod.control_a04q_rebill.alteryx_output
                            ) AS last_refresh,
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
    ),
    f12m_data AS (
        SELECT
            "F12-M" AS `Control`,
            scafdate AS `Date`,
            last_refresh AS `Last Refresh`,
            breakdown AS `Breakdown`,
            'Count of Errors' AS `Metric`,
            CountOfErrors AS `Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    date_breakdown_scaffold.scafdate,
                    (
                        SELECT
                            MAX(ChargeStartDate)
                        FROM
                            revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary
                    ) AS last_refresh,
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
            last_refresh AS `Last Refresh`,
            breakdown AS `Breakdown`,
            'Count of Errors' AS `Metric`,
            CountOfErrors AS `Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    date_breakdown_scaffold.scafdate,
                    (
                        SELECT
                            MAX(ChargeStartDate)
                        FROM
                            revenue-assurance-prod.ime_suspense.IME_Tableau_Summary
                    ) AS last_refresh,
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
    ime02w_data AS (
        SELECT
            "IME02-W" AS `Control`,
            scafdate AS `Date`,
            last_refresh AS `Last Refresh`,
            breakdown AS `Breakdown`,
            metric AS `Metric`,
            control_count AS `Control Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    scafdate,
                    last_refresh,
                    breakdown,
                    metric,
                    control_count,
                    LAG(control_count) OVER (
                        PARTITION BY
                            breakdown,
                            metric
                        ORDER BY
                            scafdate
                    ) AS errors_yesterday,
                    control_count - LAG(control_count) OVER (
                        PARTITION BY
                            breakdown,
                            metric
                        ORDER BY
                            scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        control_count - LAG(control_count) OVER (
                            PARTITION BY
                                breakdown,
                                metric
                            ORDER BY
                                scafdate
                        ),
                        LAG(control_count) OVER (
                            PARTITION BY
                                breakdown,
                                metric
                            ORDER BY
                                scafdate
                        )
                    ) * 100 AS pct_diff
                FROM
                    (
                        SELECT
                            date_breakdown_scaffold.scafdate,
                            date_breakdown_scaffold.breakdown,
                            (
                                SELECT
                                    MAX(ime_ime_file_date)
                                FROM
                                    revenue-assurance-prod.control_ime_sv.IME_SV_Summary
                            ) AS last_refresh,
                            date_breakdown_scaffold.metric,
                            IFNULL(SUM(control_count), 0) AS control_count,
                        FROM
                            date_breakdown_scaffold
                            LEFT JOIN (
                                SELECT
                                    *
                                FROM
                                    revenue-assurance-prod.control_ime_sv.IME_SV_Summary UNPIVOT(
                                        control_count
                                        FOR metric IN (
                                            files_collected,
                                            IME_TotRecsRecvd,
                                            IME_v_SV_difference
                                        )
                                    )
                            ) a ON date_breakdown_scaffold.scafdate = a.ime_ime_file_date
                            AND date_breakdown_scaffold.breakdown = CONCAT(a.traffic_type, ' - ', a.IME_AcquisitionPortal)
                            AND date_breakdown_scaffold.metric = a.metric
                        WHERE
                            date_breakdown_scaffold.control = 'IME02-W'
                        GROUP BY
                            date_breakdown_scaffold.scafdate,
                            date_breakdown_scaffold.breakdown,
                            metric
                        ORDER BY
                            date_breakdown_scaffold.scafdate,
                            date_breakdown_scaffold.breakdown,
                            date_breakdown_scaffold.metric
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
    a04q_data
UNION ALL
SELECT
    *
FROM
    ime02w_data;