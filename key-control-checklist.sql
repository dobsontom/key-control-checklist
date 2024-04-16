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
            sap_exception AS scafbreakdown,
            'Control Count' AS scafmetric
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
            'A06-M' AS control,
            'None' AS scafbreakdown,
            metric AS scafmetric
        FROM
            revenue-assurance-prod.control_a06m_leases.vw_control_monthly
            CROSS JOIN (
                SELECT
                    metric
                    -- Need to create unified metric column via union rather than unpivot, as can't unpivot
                    -- fields with different data types into the same field.
                FROM
                    (
                        SELECT DISTINCT
                            IF(
                                billed_as_expected,
                                'Billed as Planned',
                                'Not Billed as Planned'
                            ) AS metric
                        FROM
                            revenue-assurance-prod.control_a06m_leases.vw_control_monthly
                        WHERE
                            billed_as_expected = TRUE
                        UNION ALL
                        SELECT DISTINCT
                            IF(
                                lease_contract_number LIKE '%FREE%',
                                'Unpriced Lease',
                                'Priced Lease'
                            ) AS metric
                        FROM
                            revenue-assurance-prod.control_a06m_leases.vw_control_monthly
                        WHERE
                            lease_contract_number LIKE '%FREE%'
                    )
            )
        UNION DISTINCT
        -- A17-M has one metric (count of rows) and two breakdown fields which need to be merged into a single breakdown
        -- field to create full scaffolding.
        SELECT DISTINCT
            'A17-M' AS control,
            'None' AS scafbreakdown,
            metric AS scafmetric
        FROM
            revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
            CROSS JOIN (
                SELECT
                    metric
                    -- Need to create unified metric column via unions rather than unpivot, as can't unpivot
                    -- fields with different data types, even when casting as string.
                FROM
                    (
                        SELECT DISTINCT
                            IFNULL(
                                CAST(sap_net_value AS STRING),
                                'Null SAP Net Value'
                            ) AS metric
                        FROM
                            revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
                        WHERE
                            sap_net_value IS NULL
                        UNION ALL
                        SELECT DISTINCT
                            is_vessel_ooc AS metric
                        FROM
                            revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
                        WHERE
                            is_vessel_ooc = 'Vessel is inside committment period'
                    )
            )
        UNION DISTINCT
        SELECT DISTINCT
            'F12-M' AS control,
            CAST(ErrorMessageID AS STRING) AS scafbreakdown,
            'Count of Errors' AS scafmetric
        FROM
            revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary
        UNION DISTINCT
        SELECT DISTINCT
            'IME01-W' AS control,
            ErrorMessageID AS scafbreakdown,
            'Count of Errors' AS scafmetric
        FROM
            revenue-assurance-prod.ime_suspense.IME_Tableau_Summary
        UNION DISTINCT
        SELECT DISTINCT
            'IME02-W' AS control,
            CONCAT(traffic_type, ' - ', IME_AcquisitionPortal) AS scafbreakdown,
            metric AS scafmetric
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
            breakdown_scaffold.scafmetric,
            breakdown_scaffold.scafbreakdown
        FROM
            date_scaffold
            CROSS JOIN breakdown_scaffold
    ),
    a04q_data AS (
        SELECT
            "A04-Q" AS `Control`,
            scafdate AS `Date`,
            last_refresh AS `Last Refresh`,
            scafbreakdown AS `Breakdown`,
            'Control Count' AS `Metric`,
            control_count AS `Control Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    scafdate,
                    last_refresh,
                    scafbreakdown,
                    control_count,
                    LAG(control_count) OVER (
                        PARTITION BY
                            scafbreakdown
                        ORDER BY
                            scafdate
                    ) AS errors_yesterday,
                    control_count - LAG(control_count) OVER (
                        PARTITION BY
                            scafbreakdown
                        ORDER BY
                            scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        control_count - LAG(control_count) OVER (
                            PARTITION BY
                                scafbreakdown
                            ORDER BY
                                scafdate
                        ),
                        LAG(control_count) OVER (
                            PARTITION BY
                                scafbreakdown
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
                            date_breakdown_scaffold.scafbreakdown,
                            sap_exception,
                            CAST(crc_created_on AS DATE) AS crc_created_on,
                            COUNTIF(
                                sap_exception IN (
                                    'Exception, SAP data found but totals mismatch',
                                    'SAP data not found'
                                )
                            ) AS control_count
                        FROM
                            date_breakdown_scaffold
                            LEFT JOIN revenue-assurance-prod.control_a04q_rebill.alteryx_output a04q ON date_breakdown_scaffold.scafdate = CAST(a04q.crc_created_on AS DATE)
                            AND date_breakdown_scaffold.scafbreakdown = a04q.sap_exception
                        WHERE
                            date_breakdown_scaffold.control = 'A04-Q'
                        GROUP BY
                            date_breakdown_scaffold.scafdate,
                            crc_created_on,
                            date_breakdown_scaffold.scafbreakdown,
                            sap_exception
                    )
            )
    ),
    a17m_data AS (
        SELECT
            "A17-M" AS `Control`,
            scafdate AS `Date`,
            last_refresh AS `Last Refresh`,
            'None' AS `Breakdown`,
            scafmetric AS `Metric`,
            control_count AS `Control Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            (
                SELECT
                    scafdate,
                    last_refresh,
                    scafmetric,
                    control_count,
                    LAG(control_count) OVER (
                        PARTITION BY
                            scafmetric
                        ORDER BY
                            scafdate
                    ) AS errors_yesterday,
                    control_count - LAG(control_count) OVER (
                        PARTITION BY
                            scafmetric
                        ORDER BY
                            scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        control_count - LAG(control_count) OVER (
                            PARTITION BY
                                scafmetric
                            ORDER BY
                                scafdate
                        ),
                        LAG(control_count) OVER (
                            PARTITION BY
                                scafmetric
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
                                    MAX(CAST(billing_task_completed_on AS DATE))
                                FROM
                                    revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
                            ) AS last_refresh,
                            date_breakdown_scaffold.scafmetric,
                            a17m.metric,
                            CAST(billing_task_completed_on AS DATE) AS billing_task_completed_on,
                            -- Count the number of incidents from the original data source rather than
                            -- from the scaffold.
                            COUNTIF(
                                a17m.metric IN (
                                    'Null SAP Net Value',
                                    'Vessel is inside committment period'
                                )
                            ) AS control_count
                        FROM
                            date_breakdown_scaffold
                            LEFT JOIN (
                                -- Union the two metrics into a single field and perform a left join to the scaffold
                                -- to get a row for every day and for every metric.
                                SELECT
                                    billing_task_completed_on,
                                    IFNULL(
                                        CAST(sap_net_value AS STRING),
                                        'Null SAP Net Value'
                                    ) AS metric
                                FROM
                                    revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
                                WHERE
                                    sap_net_value IS NULL
                                UNION ALL
                                SELECT
                                    billing_task_completed_on,
                                    is_vessel_ooc AS metric
                                FROM
                                    revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
                                WHERE
                                    is_vessel_ooc = 'Vessel is inside committment period'
                            ) a17m ON date_breakdown_scaffold.scafdate = CAST(a17m.billing_task_completed_on AS DATE)
                            AND date_breakdown_scaffold.scafmetric = a17m.metric
                        WHERE
                            date_breakdown_scaffold.control = 'A17-M'
                        GROUP BY
                            date_breakdown_scaffold.scafdate,
                            a17m.billing_task_completed_on,
                            date_breakdown_scaffold.scafmetric,
                            a17m.metric
                    )
            )
    ),
    f12m_data AS (
        SELECT
            "F12-M" AS `Control`,
            scafdate AS `Date`,
            last_refresh AS `Last Refresh`,
            scafbreakdown AS `Breakdown`,
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
                    scafbreakdown,
                    IFNULL(CountOfErrors, 0) AS CountOfErrors,
                    LAG(CountOfErrors) OVER (
                        PARTITION BY
                            scafbreakdown
                        ORDER BY
                            date_breakdown_scaffold.scafdate
                    ) AS errors_yesterday,
                    CountOfErrors - LAG(CountOfErrors) OVER (
                        PARTITION BY
                            scafbreakdown
                        ORDER BY
                            date_breakdown_scaffold.scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        CountOfErrors - LAG(CountOfErrors) OVER (
                            PARTITION BY
                                scafbreakdown
                            ORDER BY
                                date_breakdown_scaffold.scafdate
                        ),
                        LAG(CountOfErrors) OVER (
                            PARTITION BY
                                scafbreakdown
                            ORDER BY
                                date_breakdown_scaffold.scafdate
                        )
                    ) * 100 AS pct_diff
                FROM
                    date_breakdown_scaffold
                    LEFT JOIN revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary f12m ON date_breakdown_scaffold.scafdate = f12m.ChargeStartDate
                    AND date_breakdown_scaffold.scafbreakdown = CAST(f12m.ErrorMessageID AS STRING)
                WHERE
                    date_breakdown_scaffold.control = 'F12-M'
            )
    ),
    ime01w_data AS (
        SELECT
            "IME01-W" AS `Control`,
            scafdate AS `Date`,
            last_refresh AS `Last Refresh`,
            scafbreakdown AS `Breakdown`,
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
                    scafbreakdown,
                    IFNULL(CountOfErrors, 0) AS CountOfErrors,
                    LAG(CountOfErrors) OVER (
                        PARTITION BY
                            scafbreakdown
                        ORDER BY
                            date_breakdown_scaffold.scafdate
                    ) AS errors_yesterday,
                    CountOfErrors - LAG(CountOfErrors) OVER (
                        PARTITION BY
                            scafbreakdown
                        ORDER BY
                            date_breakdown_scaffold.scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        CountOfErrors - LAG(CountOfErrors) OVER (
                            PARTITION BY
                                scafbreakdown
                            ORDER BY
                                date_breakdown_scaffold.scafdate
                        ),
                        LAG(CountOfErrors) OVER (
                            PARTITION BY
                                scafbreakdown
                            ORDER BY
                                date_breakdown_scaffold.scafdate
                        )
                    ) * 100 AS pct_diff
                FROM
                    date_breakdown_scaffold
                    LEFT JOIN revenue-assurance-prod.ime_suspense.IME_Tableau_Summary ime01w ON date_breakdown_scaffold.scafdate = ime01w.ChargeStartDate
                    AND date_breakdown_scaffold.scafbreakdown = ime01w.ErrorMessageID
                WHERE
                    date_breakdown_scaffold.control = 'IME01-W'
            )
    ),
    ime02w_data AS (
        -- Select and format only the required fields to create the unified format.
        SELECT
            "IME02-W" AS `Control`,
            scafdate AS `Date`,
            last_refresh AS `Last Refresh`,
            scafbreakdown AS `Breakdown`,
            scafmetric AS `Metric`,
            control_count AS `Control Count`,
            actual_diff AS `Actual Difference vs Yesterday`,
            pct_diff `Pct Difference vs Yesterday`
        FROM
            -- Calculate the difference in incidents to the previous day for each 'breakdown' and metric.
            (
                SELECT
                    scafdate,
                    last_refresh,
                    scafbreakdown,
                    scafmetric,
                    control_count,
                    LAG(control_count) OVER (
                        PARTITION BY
                            scafbreakdown,
                            scafmetric
                        ORDER BY
                            scafdate
                    ) AS errors_yesterday,
                    control_count - LAG(control_count) OVER (
                        PARTITION BY
                            scafbreakdown,
                            scafmetric
                        ORDER BY
                            scafdate
                    ) AS actual_diff,
                    SAFE_DIVIDE(
                        control_count - LAG(control_count) OVER (
                            PARTITION BY
                                scafbreakdown,
                                scafmetric
                            ORDER BY
                                scafdate
                        ),
                        LAG(control_count) OVER (
                            PARTITION BY
                                scafbreakdown,
                                scafmetric
                            ORDER BY
                                scafdate
                        )
                    ) * 100 AS pct_diff
                FROM
                    (
                        SELECT
                            date_breakdown_scaffold.scafdate,
                            date_breakdown_scaffold.scafbreakdown,
                            -- Get the latest date in the table to use for last refresh indicator.
                            (
                                SELECT
                                    MAX(ime_ime_file_date)
                                FROM
                                    revenue-assurance-prod.control_ime_sv.IME_SV_Summary
                            ) AS last_refresh,
                            date_breakdown_scaffold.scafmetric,
                            -- If the count of incidents is null (due to being missing from the main data and brought in via scaffolding) replace with zero.
                            IFNULL(SUM(control_count), 0) AS control_count,
                        FROM
                            date_breakdown_scaffold
                            -- Left join main data to the scaffold to bring in any days/control combinations missing from the data.
                            LEFT JOIN (
                                -- Unpivot multiple metrics into a single field that can be used in the unified format.
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
                            ) ime02w ON date_breakdown_scaffold.scafdate = ime02w.ime_ime_file_date
                            AND date_breakdown_scaffold.scafbreakdown = CONCAT(
                                ime02w.traffic_type,
                                ' - ',
                                ime02w.IME_AcquisitionPortal
                            )
                            AND date_breakdown_scaffold.scafmetric = ime02w.metric
                        WHERE
                            date_breakdown_scaffold.control = 'IME02-W'
                        GROUP BY
                            date_breakdown_scaffold.scafdate,
                            date_breakdown_scaffold.scafbreakdown,
                            scafmetric
                    )
            )
    )
SELECT
    *
FROM
    a04q_data
UNION ALL
SELECT
    *
FROM
    a17m_data
UNION ALL
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
    ime02w_data;