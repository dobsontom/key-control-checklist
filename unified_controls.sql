/*
 *===============================================================================
 * Title:    Refresh Unified Controls Every Six Hours
 * Author:   Tom Dobson (The Information Lab)
 * Date:     14-08-2024
 *===============================================================================
 * Purpose:  This script calculates the incident frequency for the last five days
 *           for controls tracked in Revenue Assurance's Key Control Checklist.
 *           Each control's data is joined to a scaffold (control_scaffold) to
 *           ensure that days without any incidents have a row with zero
 *           occurrences rather than no data, enabling day-to-day comparisons.
 *           The data for each control is minimized, combined into a unified
 *           structure, and stored in the table 'unified_controls', which feeds
 *           the Key Control Checklist Dashboard.
 * Docs:     https://bit.ly/key-control-docs
 *===============================================================================
 */
CREATE OR REPLACE TABLE `revenue-assurance-prod.key_control_checklist.unified_controls` AS (
    WITH
    control_scaffold AS (
        SELECT
            *
        FROM
            `revenue-assurance-prod.key_control_checklist.control_scaffold`
    ),

    a02q_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(
                a02q.category1 IN (
                    'Review for charges',
                    'Timing issue - active billing billing task - check in the next control run'
                )
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN
            `revenue-assurance-prod.control_a02_fx_completeness.output_fx_completeness_snb_control_monthly_data`
                AS a02q
            ON csf.date = a02q.current_commissioning_confirmed_date
            AND csf.metric_detail = a02q.category1
        WHERE
            csf.control = 'A02-Q'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    a04q_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(
                a04q.sap_exception IN (
                    'Exception, SAP data found but totals mismatch', 'SAP data not found'
                )
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN `revenue-assurance-prod.control_a04q_rebill.alteryx_output` AS a04q
            ON csf.date = CAST(a04q.crc_created_on AS DATE)
            AND csf.metric_detail = a04q.sap_exception
        WHERE
            csf.control = 'A04-Q'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    a06m_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(a06m.metric IN ('Not Billed as Planned', 'Unpriced Lease')) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN (
            -- Union two metrics into a single field
            SELECT
                contract_start_date,
                IF(billed_as_expected = TRUE, 'Billed as Planned', 'Not Billed as Planned')
                    AS metric
            FROM
                `revenue-assurance-prod.control_a06m_leases.vw_control_monthly`
            WHERE
                billed_as_expected = FALSE
                AND lease_cancelled = FALSE
                AND wholesale_part_of_retail_lease = FALSE
            UNION ALL
            SELECT
                contract_start_date,
                IF(lease_contract_number LIKE '%FREE%', 'Unpriced Lease', 'Priced Lease') AS metric
            FROM
                `revenue-assurance-prod.control_a06m_leases.vw_control_monthly`
            WHERE
                lease_contract_number LIKE '%FREE%'
                AND lease_cancelled = FALSE
                AND wholesale_part_of_retail_lease = FALSE
                AND (
                    contract_value IS NULL
                    OR contract_value = 0
                )
        ) AS a06m
            ON csf.date = CAST(a06m.contract_start_date AS DATE)
            AND csf.metric = a06m.metric
        WHERE
            csf.control = 'A06-M'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    a15q_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(
                DATE_TRUNC(a15q.billing_task_completed_on, MONTH)
                = DATE_TRUNC(CURRENT_DATE(), MONTH)
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN
            `revenue-assurance-prod.key_control_checklist.a15q_extract` AS a15q
            ON csf.date = DATE(a15q.billing_task_completed_on)
        WHERE
            csf.control = 'A15-Q'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    a17m_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(a17m.metric IN ('Null SAP Net Value', 'Vessel is inside committment period'))
                AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN (
            -- Union two metrics into a single field
            SELECT
                billing_task_completed_on,
                COALESCE(CAST(sap_net_value AS STRING), 'Null SAP Net Value') AS metric
            FROM
                `revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated`
            WHERE
                sap_net_value IS NULL
            UNION ALL
            SELECT
                billing_task_completed_on,
                is_vessel_ooc AS metric
            FROM
                `revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated`
            WHERE
                is_vessel_ooc = 'Vessel is inside committment period'
        ) AS a17m
            ON csf.date = CAST(a17m.billing_task_completed_on AS DATE)
            AND csf.metric = a17m.metric
        WHERE
            csf.control = 'A17-M'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    chv_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(
                chv.check_for_charterer_plan_billied = 'Review for charges - Not found in billing'
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN
            `revenue-assurance-prod.key_control_checklist.chv_extract` AS chv
            ON csf.date = DATE(chv.charterer_start_date)
        WHERE
            csf.control = 'CH-V'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    e05w_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(e05w.review_required = 'Review') AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN
            `revenue-assurance-prod.control_e05w.output_data` AS e05w
            ON csf.date = DATE(e05w.activation_date)
        WHERE
            csf.control = 'E05-W'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    f01m_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNT(
                DISTINCT IF(
                    f01m.billing_status IN (
                        'No billing available - needs review', 'Old billing - needs review'
                    )
                    AND f01m.project_status != 'Stopped'
                    AND f01m.billed_arrears = 'Not billed in arrears',
                    f01m.project_task_order,
                    NULL
                )
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN
            `revenue-assurance-prod.control_f01_m_pulse_projects_reconciliation.control_monthly_data`
                AS f01m
            ON csf.date = f01m.project_implementation_confirmed_date
            AND csf.metric_detail = CAST(f01m.project_type AS STRING)
        WHERE
            csf.control = 'F01-M'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    f12m_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COALESCE(f12m.countoferrors, 0) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN `revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary` AS f12m
            ON csf.date = f12m.chargestartdate
            AND csf.metric_detail = CAST(f12m.errormessageid AS STRING)
        WHERE
            csf.control = 'F12-M'
    ),

    fc01q_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(
                fc01q.pulse_vs_nuda_category_1
                = 'Review needed - no charges matching with Vessel ID'
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN
            `revenue-assurance-prod.key_control_checklist.fc01q_extract` AS fc01q
            ON csf.date = DATE(fc01q.commissioning_confirm_date)
        WHERE
            csf.control = 'FC01-Q'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    gx4_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(
                gx4.control_group = 'Usage'
                AND gx4.control_name = 'DAL vs BTP Usage by SSPC'
                AND gx4.exception_type = 'Difference greater than 2.5%'
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN
            `revenue-assurance-prod.control_gx4.output_control_outcomes` AS gx4
            ON csf.date = DATE(gx4.control_date)
        WHERE
            csf.control = 'GX4-JX'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    ime01w_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COALESCE(ime01w.countoferrors, 0) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN `revenue-assurance-prod.ime_suspense.IME_Tableau_Summary` AS ime01w
            ON csf.date = ime01w.chargestartdate
            AND csf.metric_detail = ime01w.errormessageid
        WHERE
            csf.control = 'IME01-W'
    ),

    ime02w_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            -- If the count of incidents is null due to being missing from the data replace with zero
            COALESCE(SUM(ime02w.control_count), 0) AS control_count
        FROM
            control_scaffold AS csf
            -- Unpivot three metrics into a single field
        LEFT JOIN (
            SELECT
                ime_ime_file_date,
                traffic_type,
                ime_acquisitionportal,
                metric,
                control_count
            FROM
                `revenue-assurance-prod.control_ime_sv.IME_SV_Summary` UNPIVOT (
                control_count
                FOR metric IN (files_collected, ime_totrecsrecvd, ime_v_sv_difference)
            )
        ) AS ime02w
            ON csf.date = ime02w.ime_ime_file_date
            AND csf.metric_detail = CONCAT(ime02w.traffic_type, ' - ', ime02w.ime_acquisitionportal)
            AND csf.metric = ime02w.metric
        WHERE
            csf.control = 'IME02-W'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    var1_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(
                (
                    var1.metric = 'category_1'
                    AND var1.metric_detail = 'Review needed?'
                )
                OR (
                    var1.metric = 'Billed_in_SV_category'
                    AND var1.metric_detail = 'HP - billed in last 3 months'
                )
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN (
            -- Unpivot multiple metric/metric_details into a single field
            SELECT
                order_date,
                metric,
                metric_detail
            FROM
                `revenue-assurance-prod.control_var_01_leases.monthly_control_output_for_review2`
            UNPIVOT (
                metric_detail
                FOR metric IN (category_1, billed_in_sv_category)
            )
            WHERE
                (
                    metric = 'category_1'
                    AND metric_detail = 'Review needed?'
                )
                OR (
                    metric = 'Billed_in_SV_category'
                    AND metric_detail = 'HP - billed in last 3 months'
                )
        ) AS var1
            ON csf.date = CAST(var1.order_date AS DATE)
            AND csf.metric = var1.metric
            AND csf.metric_detail = var1.metric_detail
        WHERE
            csf.control = 'VAR-1'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    x01b_data AS (
        SELECT
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail,
            COUNTIF(
                x01b.category1 IN (
                    'Review - active temp stop vessel, why billed with original charges',
                    'Review - why billed with suspended charges although they are reactivated'
                )
            ) AS control_count
        FROM
            control_scaffold AS csf
        LEFT JOIN
            `revenue-assurance-prod.control_x01b_retail_fx_temprarary_stopped_vessels_review.control_output_data_temp_stop_vessels`
                AS x01b
            ON csf.date = x01b.stopped_confirmed_date
            AND csf.metric_detail = CAST(x01b.category1 AS STRING)
        WHERE
            csf.control = 'X01-B'
        GROUP BY
            csf.control,
            csf.date,
            csf.metric,
            csf.metric_detail
    ),

    combined_data AS (
        SELECT
            *
        FROM
            a02q_data
        UNION ALL
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
            a15q_data
        UNION ALL
        SELECT
            *
        FROM
            a17m_data
        UNION ALL
        SELECT
            *
        FROM
            chv_data
        UNION ALL
        SELECT
            *
        FROM
            e05w_data
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
            fc01q_data
        UNION ALL
        SELECT
            *
        FROM
            gx4_data
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
        UNION ALL
        SELECT
            *
        FROM
            var1_data
        UNION ALL
        SELECT
            *
        FROM
            x01b_data
    ),

    daily_freq_change AS (
        SELECT
            *,
            control_count - LAG(control_count) OVER (
                PARTITION BY
                    control,
                    metric,
                    metric_detail
                ORDER BY
                    `date`
            ) AS daily_absolute_change,
            SAFE_DIVIDE(
                control_count - LAG(control_count) OVER (
                    PARTITION BY
                        control,
                        metric,
                        metric_detail
                    ORDER BY
                        `date`
                ),
                LAG(control_count) OVER (
                    PARTITION BY
                        control,
                        metric,
                        metric_detail
                    ORDER BY
                        `date`
                )
            ) * 100 AS daily_percent_change
        FROM
            combined_data
    ),

    final_data_with_refresh_times AS (
        SELECT
            dfc.*,
            ref.last_refresh_time
        FROM
            daily_freq_change AS dfc
        LEFT JOIN
            `revenue-assurance-prod.key_control_checklist.control_refresh_times` AS ref
            ON dfc.control = ref.control
    )

    SELECT
        control,
        `date`,
        metric,
        metric_detail,
        control_count,
        daily_absolute_change,
        daily_percent_change,
        last_refresh_time
    FROM
        final_data_with_refresh_times
);
