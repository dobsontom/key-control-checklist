WITH
    a04q_data AS (
        SELECT
            "A04-Q" AS control,
            scaf.scafdate,
            (
                SELECT
                    MAX(CAST(crc_created_on AS DATE))
                FROM
                    revenue-assurance-prod.control_a04q_rebill.alteryx_output
            ) AS last_refresh,
            'Error Count' AS scafmetric,
            scaf.scafbreakdown,
            COUNTIF(
                sap_exception IN (
                    'Exception, SAP data found but totals mismatch',
                    'SAP data not found'
                )
            ) AS control_count
        FROM
            revenue-assurance-prod.key_control_checklist.control_scaffold scaf
            LEFT JOIN revenue-assurance-prod.control_a04q_rebill.alteryx_output a04q ON scaf.scafdate = CAST(a04q.crc_created_on AS DATE)
            AND scaf.scafbreakdown = a04q.sap_exception
        WHERE
            scaf.control = 'A04-Q'
        GROUP BY
            scaf.scafdate,
            scaf.scafbreakdown
    ),
    a06m_data AS (
        SELECT
            "A06-M" AS control,
            scafdate,
            (
                SELECT
                    MAX(contract_start_date)
                FROM
                    revenue-assurance-prod.control_a06m_leases.vw_control_monthly
            ) AS last_refresh,
            scafmetric,
            'None' AS scafbreakdown,
            -- Count the number of incidents from the original data source rather than
            -- from the scaffold.
            COUNTIF(
                a06m.metric IN (
                    'Null SAP Net Value',
                    'Vessel is inside committment period'
                )
            ) AS control_count
        FROM
            revenue-assurance-prod.key_control_checklist.control_scaffold scaf
            LEFT JOIN (
                -- Union the two metrics into a single field and perform a left join to the scaffold
                -- to get a row for every day and for every metric.
                SELECT
                    contract_start_date,
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
                SELECT
                    contract_start_date,
                    IF(
                        lease_contract_number LIKE '%FREE%',
                        'Unpriced Lease',
                        'Priced Lease'
                    ) AS metric
                FROM
                    revenue-assurance-prod.control_a06m_leases.vw_control_monthly
                WHERE
                    lease_contract_number LIKE '%FREE%'
            ) a06m ON scaf.scafdate = CAST(a06m.contract_start_date AS DATE)
            AND scaf.scafmetric = a06m.metric
        WHERE
            scaf.control = 'A06-M'
        GROUP BY
            scaf.scafdate,
            scaf.scafmetric
    ),
    a17m_data AS (
        SELECT
            "A17-M" AS `Control`,
            scafdate,
            (
                SELECT
                    MAX(billing_task_completed_on)
                FROM
                    revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
            ) AS last_refresh,
            scafmetric,
            'None' AS `Breakdown`,
            -- Count the number of incidents from the original data source rather than
            -- from the scaffold.
            COUNTIF(
                a17m.metric IN (
                    'Null SAP Net Value',
                    'Vessel is inside committment period'
                )
            ) AS control_count
        FROM
            revenue-assurance-prod.key_control_checklist.control_scaffold scaf
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
            ) a17m ON scaf.scafdate = CAST(a17m.billing_task_completed_on AS DATE)
            AND scaf.scafmetric = a17m.metric
        WHERE
            control = 'A17-M'
        GROUP BY
            scaf.scafdate,
            scaf.scafmetric
    ),
    f01m_data AS (
        SELECT
            "F01-M" AS control,
            scafdate,
            (
                SELECT
                    MAX(project_implementation_confirmed_date)
                FROM
                    revenue-assurance-prod.control_f01_m_pulse_projects_reconciliation.control_monthly_data
            ) AS last_refresh,
            'Tasks Remaining' AS scafmetric,
            scafbreakdown,
            COUNT(
                DISTINCT IF(
                    f01m.billing_status IN (
                        'No billing available - needs review',
                        'Old billing - needs review'
                    )
                    AND f01m.project_status != 'Stopped'
                    AND f01m.billed_arrears = 'Not billed in arrears',
                    project_task_order,
                    NULL
                )
            ) AS control_count
        FROM
            revenue-assurance-prod.key_control_checklist.control_scaffold scaf
            LEFT JOIN revenue-assurance-prod.control_f01_m_pulse_projects_reconciliation.control_monthly_data f01m ON scaf.scafdate = f01m.project_implementation_confirmed_date
            AND scaf.scafbreakdown = CAST(f01m.project_type AS STRING)
        WHERE
            control = 'F01-M'
        GROUP BY
            scaf.scafdate,
            scaf.scafmetric,
            scaf.scafbreakdown
    ),
    f12m_data AS (
        SELECT
            "F12-M" AS control,
            scafdate,
            (
                SELECT
                    MAX(ChargeStartDate)
                FROM
                    revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary
            ) AS last_refresh,
            'Count of Errors' AS scafmetric,
            scafbreakdown,
            IFNULL(CountOfErrors, 0) control_count
        FROM
            revenue-assurance-prod.key_control_checklist.control_scaffold scaf
            LEFT JOIN revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary f12m ON scaf.scafdate = f12m.ChargeStartDate
            AND scaf.scafbreakdown = CAST(f12m.ErrorMessageID AS STRING)
        WHERE
            control = 'F12-M'
    ),
    ime01w_data AS (
        SELECT
            "IME01-W" AS control,
            scafdate,
            (
                SELECT
                    MAX(ChargeStartDate)
                FROM
                    revenue-assurance-prod.ime_suspense.IME_Tableau_Summary
            ) AS last_refresh,
            'Count of Errors' AS scafmetric,
            scafbreakdown,
            IFNULL(CountOfErrors, 0) AS control_count
        FROM
            revenue-assurance-prod.key_control_checklist.control_scaffold scaf
            LEFT JOIN revenue-assurance-prod.ime_suspense.IME_Tableau_Summary ime01w ON scaf.scafdate = ime01w.ChargeStartDate
            AND scaf.scafbreakdown = ime01w.ErrorMessageID
        WHERE
            control = 'IME01-W'
    ),
    ime02w_data AS (
        SELECT
            "IME02-W" AS control,
            scafdate,
            -- Get the latest date in the table to use for last refresh indicator.
            (
                SELECT
                    MAX(ime_ime_file_date)
                FROM
                    revenue-assurance-prod.control_ime_sv.IME_SV_Summary
            ) AS last_refresh,
            scafmetric,
            scafbreakdown,
            -- If the count of incidents is null (due to being missing from the main data and brought in via scaffolding) replace with zero.
            IFNULL(SUM(control_count), 0) AS control_count,
        FROM
            revenue-assurance-prod.key_control_checklist.control_scaffold scaf
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
            ) ime02w ON scaf.scafdate = ime02w.ime_ime_file_date
            AND scaf.scafbreakdown = CONCAT(
                ime02w.traffic_type,
                ' - ',
                ime02w.IME_AcquisitionPortal
            )
            AND scaf.scafmetric = ime02w.metric
        WHERE
            scaf.control = 'IME02-W'
        GROUP BY
            scaf.scafdate,
            scaf.scafmetric,
            scaf.scafbreakdown
    )
    -- Select and format only the required fields to create the unified format.
SELECT
    control AS `Control`,
    scafdate AS `Date`,
    last_refresh AS `Last Refresh`,
    scafmetric AS `Metric`,
    scafbreakdown AS `Breakdown`,
    control_count AS `Control Count`,
    actual_diff AS `Actual Difference vs Yesterday`,
    pct_diff `Pct Difference vs Yesterday`
FROM
    (
        -- Calculate the difference in incidents to the previous day for each control, breakdown, and metric.
        SELECT
            control,
            scafdate,
            last_refresh,
            scafmetric,
            scafbreakdown,
            control_count,
            LAG(control_count) OVER (
                PARTITION BY
                    control,
                    scafmetric,
                    scafbreakdown
                ORDER BY
                    scafdate
            ) AS errors_yesterday,
            control_count - LAG(control_count) OVER (
                PARTITION BY
                    control,
                    scafmetric,
                    scafbreakdown
                ORDER BY
                    scafdate
            ) AS actual_diff,
            SAFE_DIVIDE(
                control_count - LAG(control_count) OVER (
                    PARTITION BY
                        control,
                        scafmetric,
                        scafbreakdown
                    ORDER BY
                        scafdate
                ),
                LAG(control_count) OVER (
                    PARTITION BY
                        control,
                        scafmetric,
                        scafbreakdown
                    ORDER BY
                        scafdate
                )
            ) * 100 AS pct_diff
        FROM
            (
                SELECT
                    *
                FROM
                    a04q_data
                UNION ALL
                SELECT
                    *
                FROM
                    a06m_data
                UNION ALL
                SELECT
                    *
                FROM
                    a17m_data
                UNION ALL
                SELECT
                    *
                FROM
                    f01m_data
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
                    ime02w_data
            )
    );