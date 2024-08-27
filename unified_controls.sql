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
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(
               a02q.category1 IN ('Review for charges', 'Timing issue - active billing billing task - check in the next control run')
            ) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.control_a02_fx_completeness.output_fx_completeness_snb_control_monthly_data` a02q ON scf.date = a02q.current_commissioning_confirmed_date
            AND scf.metric_detail = a02q.category1
         WHERE
            scf.control = 'A02-Q'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      a04q_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(a04q.sap_exception IN ('Exception, SAP data found but totals mismatch', 'SAP data not found')) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.control_a04q_rebill.alteryx_output` a04q ON scf.date = CAST(a04q.crc_created_on AS DATE)
            AND scf.metric_detail = a04q.sap_exception
         WHERE
            scf.control = 'A04-Q'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      a06m_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(a06m.metric IN ('Not Billed as Planned', 'Unpriced Lease')) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN (
               -- Union two metrics into a single field
               SELECT
                  contract_start_date,
                  IF(billed_as_expected = TRUE, 'Billed as Planned', 'Not Billed as Planned') AS metric
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
            ) a06m ON scf.date = CAST(a06m.contract_start_date AS DATE)
            AND scf.metric = a06m.metric
         WHERE
            scf.control = 'A06-M'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      a15q_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(DATE_TRUNC(a15q.billing_task_completed_on, MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.key_control_checklist.a15q_extract` a15q ON scf.date = DATE(a15q.billing_task_completed_on)
         WHERE
            scf.control = 'A15-Q'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      a17m_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(a17m.metric IN ('Null SAP Net Value', 'Vessel is inside committment period')) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN (
               -- Union two metrics into a single field
               SELECT
                  billing_task_completed_on,
                  IFNULL(CAST(sap_net_value AS STRING), 'Null SAP Net Value') AS metric
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
            ) a17m ON scf.date = CAST(a17m.billing_task_completed_on AS DATE)
            AND scf.metric = a17m.metric
         WHERE
            scf.control = 'A17-M'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      chv_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(chv.check_for_charterer_plan_billied = 'Review for charges - Not found in billing') AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.key_control_checklist.chv_extract` chv ON scf.date = DATE(chv.charterer_start_date)
         WHERE
            scf.control = 'CH-V'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      e05w_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(e05w.review_required = 'Review') AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.control_e05w.output_data` e05w ON scf.date = DATE(e05w.activation_date)
         WHERE
            scf.control = 'E05-W'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      f01m_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNT(
               DISTINCT IF(
                  f01m.billing_status IN ('No billing available - needs review', 'Old billing - needs review')
                  AND f01m.project_status != 'Stopped'
                  AND f01m.billed_arrears = 'Not billed in arrears',
                  project_task_order,
                  NULL
               )
            ) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.control_f01_m_pulse_projects_reconciliation.control_monthly_data` f01m ON scf.date = f01m.project_implementation_confirmed_date
            AND scf.metric_detail = CAST(f01m.project_type AS STRING)
         WHERE
            scf.control = 'F01-M'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      f12m_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            IFNULL(f12m.countoferrors, 0) AS control_count,
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary` f12m ON scf.date = f12m.chargestartdate
            AND scf.metric_detail = CAST(f12m.errormessageid AS STRING)
         WHERE
            scf.control = 'F12-M'
      ),
      fc01q_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(fc01q.pulse_vs_nuda_category_1 = 'Review needed - no charges matching with Vessel ID') AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.key_control_checklist.fc01q_extract` fc01q ON scf.date = DATE(fc01q.commissioning_confirm_date)
         WHERE
            scf.control = 'FC01-Q'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      gx4_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(
               gx4.control_group = 'Usage'
               AND gx4.control_name = 'DAL vs BTP Usage by SSPC'
               AND gx4.exception_type = 'Difference greater than 2.5%'
            ) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.control_gx4.output_control_outcomes` gx4 ON scf.date = DATE(gx4.control_date)
         WHERE
            scf.control = 'GX4-JX'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      ime01w_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            IFNULL(ime01w.countoferrors, 0) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.ime_suspense.IME_Tableau_Summary` ime01w ON scf.date = ime01w.chargestartdate
            AND scf.metric_detail = ime01w.errormessageid
         WHERE
            scf.control = 'IME01-W'
      ),
      ime02w_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            -- If the count of incidents is null due to being missing from the data replace with zero
            IFNULL(SUM(ime02w.control_count), 0) AS control_count
         FROM
            control_scaffold scf
            -- Unpivot three metrics into a single field
            LEFT JOIN (
               SELECT
                  ime_ime_file_date,
                  traffic_type,
                  ime_acquisitionportal,
                  metric,
                  control_count
               FROM
                  `revenue-assurance-prod.control_ime_sv.IME_SV_Summary` UNPIVOT(
                     control_count
                     FOR metric IN (files_collected, ime_totrecsrecvd, ime_v_sv_difference)
                  )
            ) ime02w ON scf.date = ime02w.ime_ime_file_date
            AND scf.metric_detail = CONCAT(ime02w.traffic_type, ' - ', ime02w.ime_acquisitionportal)
            AND scf.metric = ime02w.metric
         WHERE
            scf.control = 'IME02-W'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      var1_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
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
            control_scaffold scf
            LEFT JOIN (
               -- Unpivot multiple metric/metric_details into a single field
               SELECT
                  order_date,
                  metric,
                  metric_detail
               FROM
                  `revenue-assurance-prod.control_var_01_leases.monthly_control_output_for_review2` UNPIVOT(
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
            ) var1 ON scf.date = CAST(var1.order_date AS DATE)
            AND scf.metric = var1.metric
            AND scf.metric_detail = var1.metric_detail
         WHERE
            scf.control = 'VAR-1'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
      ),
      x01b_data AS (
         SELECT
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail,
            COUNTIF(
               x01b.category1 IN (
                  'Review - active temp stop vessel, why billed with original charges',
                  'Review - why billed with suspended charges although they are reactivated'
               )
            ) AS control_count
         FROM
            control_scaffold scf
            LEFT JOIN `revenue-assurance-prod.control_x01b_retail_fx_temprarary_stopped_vessels_review.control_output_data_temp_stop_vessels` x01b ON scf.date = x01b.stopped_confirmed_date
            AND scf.metric_detail = CAST(x01b.category1 AS STRING)
         WHERE
            scf.control = 'X01-B'
         GROUP BY
            scf.control,
            scf.date,
            scf.metric,
            scf.metric_detail
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
      calculate_daily_freq_change AS (
         SELECT
            *,
            control_count - LAG(control_count) OVER (
               PARTITION BY
                  control,
                  metric,
                  metric_detail
               ORDER BY
                  `date`
            ) AS absolute_change_vs_day_before,
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
            ) * 100 AS pct_change_vs_day_before
         FROM
            combined_data
      ),
      add_control_last_refresh_times AS (
         SELECT
            dfc.*,
            ref.last_refresh_dttm
         FROM
            calculate_daily_freq_change dfc
            LEFT JOIN `revenue-assurance-prod.key_control_checklist.control_refresh_times` ref ON dfc.control = ref.control
      )
   SELECT
      control,
      `date`,
      metric,
      metric_detail,
      control_count,
      absolute_change_vs_day_before,
      pct_change_vs_day_before,
      last_refresh_dttm
   FROM
      add_control_last_refresh_times
);
