/*-----------------------------------------------------------------------------------*
| Name:      refresh_unified_controls_every_six_hours                                |
| Author:    Tom Dobson (The Information Lab)                                        |
| Date:      30-04-2024                                                              |
|------------------------------------------------------------------------------------|
| Purpose:   This script calculates the incident frequency for the last five days    |
|            for controls tracked in Revenue Assurance's Key Control Checklist.      |
|            Each control's data is joined to a scaffold (control_scaffold) to       |
|            ensure that days without any incidents have a row with zero ocurrences  |
|            rather than no data, enabling day-to-day comparisons. The data for      |
|            each control is minimised, combined into a unified structure, and       |
|            stored in the table 'unified_controls', which feeds the Key Control     |
|            Checklist Dashboard.                                                    |
|------------------------------------------------------------------------------------|
| Modifications:                                                                     |
'-----------------------------------------------------------------------------------*/
CREATE OR REPLACE TABLE
   revenue-assurance-prod.key_control_checklist.unified_controls AS (
      -- Fetches the last modified time from the metadata of 
      -- the parent table of each control.
      WITH
         last_refresh_times AS (
            SELECT
               table_id,
               TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
            FROM
               `revenue-assurance-prod.pulse.__TABLES__`
            WHERE
               table_id = 'vw_project_tasks'
            UNION ALL
            SELECT
               table_id,
               TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
            FROM
               `revenue-assurance-prod.control_a04q_rebill.__TABLES__`
            WHERE
               table_id = 'alteryx_output'
            UNION ALL
            SELECT
               table_id,
               TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
            FROM
               `revenue-assurance-prod.control_a06m_leases.__TABLES__`
            WHERE
               table_id = 'dim_lease_history'
            UNION ALL
            SELECT
               table_id,
               TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
            FROM
               `revenue-assurance-prod.control_f12m_btp_suspense.__TABLES__`
            WHERE
               table_id = 'sim_tracker'
            UNION ALL
            SELECT
               table_id,
               TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
            FROM
               `revenue-assurance-prod.ime_suspense.__TABLES__`
            WHERE
               table_id = 'EPS_Suspense'
            UNION ALL
            SELECT
               table_id,
               TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
            FROM
               `revenue-assurance-prod.control_ime_sv.__TABLES__`
            WHERE
               table_id = 'ime_summary'
            UNION ALL
            SELECT
               table_id,
               TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
            FROM
               `revenue-assurance-prod.control_var_01_leases.__TABLES__`
            WHERE
               table_id = 'output_var_leases_alteryx_data'
            UNION ALL
            SELECT
               table_id,
               TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
            FROM
               `revenue-assurance-prod.pulse_src.__TABLES__`
            WHERE
               table_id = 'vessel'
         ),
         a02q_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
               COUNTIF(
                  a02q.category1 IN (
                     'Review for charges',
                     'Timing issue - active billing billing task - check in the next control run'
                  )
               ) AS control_count
            FROM
               revenue-assurance-prod.key_control_checklist.control_scaffold scaf
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'vw_project_tasks'
               LEFT JOIN revenue-assurance-prod.control_a02_fx_completeness.output_fx_completeness_snb_control_monthly_data a02q ON scaf.scafdate = a02q.current_commissioning_confirmed_date
               AND scaf.scafbreakdown = a02q.category1
            WHERE
               scaf.control = 'A02-Q'
            GROUP BY
               scaf.control,
               scaf.scafdate,
               scaf.scafmetric,
               scaf.scafbreakdown
         ),
         a04q_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
               COUNTIF(
                  a04q.sap_exception IN (
                     'Exception, SAP data found but totals mismatch',
                     'SAP data not found'
                  )
               ) AS control_count
            FROM
               revenue-assurance-prod.key_control_checklist.control_scaffold scaf
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'alteryx_output'
               LEFT JOIN revenue-assurance-prod.control_a04q_rebill.alteryx_output a04q ON scaf.scafdate = CAST(a04q.crc_created_on AS DATE)
               AND scaf.scafbreakdown = a04q.sap_exception
            WHERE
               scaf.control = 'A04-Q'
            GROUP BY
               scaf.control,
               scaf.scafdate,
               scaf.scafmetric,
               scaf.scafbreakdown
         ),
         a06m_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
               -- Count the number of incidents from the original data source rather than
               -- from the scaffold.
               COUNTIF(
                  a06m.metric IN ('Not Billed as Planned', 'Unpriced Lease')
               ) AS control_count
            FROM
               revenue-assurance-prod.key_control_checklist.control_scaffold scaf
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'dim_lease_history'
               LEFT JOIN (
                  -- Union the two metrics into a single field and perform a left join to the scaffold
                  -- to get a row for every day and for every metric.
                  SELECT
                     contract_start_date,
                     IF(
                        billed_as_expected = TRUE,
                        'Billed as Planned',
                        'Not Billed as Planned'
                     ) AS metric
                  FROM
                     revenue-assurance-prod.control_a06m_leases.vw_control_monthly
                  WHERE
                     billed_as_expected = FALSE
                     AND lease_cancelled = FALSE
                     AND wholesale_part_of_retail_lease = FALSE
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
                     AND lease_cancelled = FALSE
                     AND wholesale_part_of_retail_lease = FALSE
                     AND (
                        contract_value IS NULL
                        OR contract_value = 0
                     )
               ) a06m ON scaf.scafdate = CAST(a06m.contract_start_date AS DATE)
               AND scaf.scafmetric = a06m.metric
            WHERE
               scaf.control = 'A06-M'
            GROUP BY
               scaf.control,
               scaf.scafdate,
               scaf.scafmetric,
               scaf.scafbreakdown
         ),
         a17m_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
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
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'vw_project_tasks'
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
               scaf.control = 'A17-M'
            GROUP BY
               scaf.control,
               scaf.scafdate,
               scaf.scafmetric,
               scaf.scafbreakdown
         ),
         f01m_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
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
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'vw_project_tasks'
               LEFT JOIN revenue-assurance-prod.control_f01_m_pulse_projects_reconciliation.control_monthly_data f01m ON scaf.scafdate = f01m.project_implementation_confirmed_date
               AND scaf.scafbreakdown = CAST(f01m.project_type AS STRING)
            WHERE
               scaf.control = 'F01-M'
            GROUP BY
               scaf.control,
               scaf.scafdate,
               scaf.scafmetric,
               scaf.scafbreakdown
         ),
         f12m_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
               IFNULL(f12m.CountOfErrors, 0) AS control_count
            FROM
               revenue-assurance-prod.key_control_checklist.control_scaffold scaf
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'sim_tracker'
               LEFT JOIN revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary f12m ON scaf.scafdate = f12m.ChargeStartDate
               AND scaf.scafbreakdown = CAST(f12m.ErrorMessageID AS STRING)
            WHERE
               scaf.control = 'F12-M'
         ),
         ime01w_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
               IFNULL(ime01w.CountOfErrors, 0) AS control_count
            FROM
               revenue-assurance-prod.key_control_checklist.control_scaffold scaf
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'EPS_Suspense'
               LEFT JOIN revenue-assurance-prod.ime_suspense.IME_Tableau_Summary ime01w ON scaf.scafdate = ime01w.ChargeStartDate
               AND scaf.scafbreakdown = ime01w.ErrorMessageID
            WHERE
               scaf.control = 'IME01-W'
         ),
         ime02w_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
               -- If the count of incidents is null (due to being missing from the main data and brought in via scaffolding) replace with zero.
               IFNULL(SUM(ime02w.control_count), 0) AS control_count
            FROM
               revenue-assurance-prod.key_control_checklist.control_scaffold scaf
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'ime_summary'
               -- Left join main data to the scaffold to bring in any days/control combinations missing from the data.
               LEFT JOIN (
                  -- Unpivot multiple metrics into a single field that can be used in the unified format.
                  SELECT
                     ime_ime_file_date,
                     traffic_type,
                     IME_AcquisitionPortal,
                     metric,
                     control_count
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
               scaf.control,
               scaf.scafdate,
               scaf.scafmetric,
               scaf.scafbreakdown
         ),
         var1_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
               COUNTIF(
                  (
                     var1.metric = 'category_1'
                     AND var1.breakdown = 'Review needed?'
                  )
                  OR (
                     var1.metric = 'Billed_in_SV_category'
                     AND var1.breakdown = 'HP - billed in last 3 months'
                  )
               ) AS control_count
            FROM
               revenue-assurance-prod.key_control_checklist.control_scaffold scaf
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'output_var_leases_alteryx_data'
               -- Left join main data to the scaffold to bring in any days/control combinations missing from the data.
               LEFT JOIN (
                  -- Unpivot multiple metrics into a single field that can be used in the unified format.
                  SELECT
                     order_date,
                     metric,
                     breakdown
                  FROM
                     revenue-assurance-prod.control_var_01_leases.monthly_control_output_for_review2 UNPIVOT(
                        breakdown
                        FOR metric IN (category_1, Billed_in_SV_category)
                     )
                  WHERE
                     (
                        metric = 'category_1'
                        AND breakdown = 'Review needed?'
                     )
                     OR (
                        metric = 'Billed_in_SV_category'
                        AND breakdown = 'HP - billed in last 3 months'
                     )
               ) var1 ON scaf.scafdate = CAST(var1.order_date AS DATE)
               AND scaf.scafmetric = var1.metric
               AND scaf.scafbreakdown = var1.breakdown
            WHERE
               scaf.control = 'VAR-1'
            GROUP BY
               scaf.control,
               scaf.scafdate,
               scaf.scafmetric,
               scaf.scafbreakdown
         ),
         x01b_data AS (
            SELECT
               scaf.control,
               scaf.scafdate,
               lr.last_refresh,
               scaf.scafmetric,
               scaf.scafbreakdown,
               COUNTIF(
                  x01b.category1 IN (
                     'Review - active temp stop vessel, why billed with original charges',
                     'Review - why billed with suspended charges although they are reactivated'
                  )
               ) AS control_count
            FROM
               revenue-assurance-prod.key_control_checklist.control_scaffold scaf
               LEFT JOIN last_refresh_times lr ON lr.table_id = 'vessel'
               LEFT JOIN revenue-assurance-prod.control_x01b_retail_fx_temprarary_stopped_vessels_review.control_output_data_temp_stop_vessels x01b ON scaf.scafdate = x01b.stopped_confirmed_date
               AND scaf.scafbreakdown = CAST(x01b.category1 AS STRING)
            WHERE
               scaf.control = 'X01-B'
            GROUP BY
               scaf.control,
               scaf.scafdate,
               scaf.scafmetric,
               scaf.scafbreakdown
         )
         -- Select and format only the required fields to create a unified format.
      SELECT
         control AS `Control`,
         scafdate AS `Date`,
         last_refresh AS `Last Refresh`,
         scafmetric AS `Metric`,
         scafbreakdown AS `Breakdown`,
         control_count AS `Control Count`,
         absolute_diff AS `Absolute Change vs Previous Day`,
         pct_diff AS `Percent Change vs Previous Day`
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
               ) AS absolute_diff,
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
               )
         )
   );