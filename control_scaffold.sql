/*-----------------------------------------------------------------------------------*
| Name:      refresh_control_scaffold_daily                                          |
| Author:    Tom Dobson (The Information Lab)                                        |
| Date:      30-04-2024                                                              |
|------------------------------------------------------------------------------------|
| Purpose:   This script generates a scaffold (control_scaffold) for the last        |
|            five days for each metric/breakdown combination for each of the         |
|            controls tracked in Revenue Assurance's Key Control Checklist.          |
|            This is required to ensure that days without any incidents have a       |
|            row with zero ocurrences rather than no data, enabling                  |
|            day-to-day comparisons.                                                 |
'-----------------------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `revenue-assurance-prod.key_control_checklist.control_scaffold` AS (
   WITH
      date_scaffold AS (
         SELECT
            *
         FROM
            UNNEST (
               GENERATE_DATE_ARRAY(
                  DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY),
                  DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
               )
            ) AS scafdate
      ),
      breakdown_scaffold AS (
         SELECT
            'A02-Q' AS control,
            'Category 1' AS scafmetric,
            category1 AS scafbreakdown
         FROM
            (
               SELECT
                  'Review for charges' AS category1
               UNION ALL
               SELECT
                  'Timing issue - active billing billing task - check in the next control run' AS category1
            )
         UNION ALL
         SELECT
            'A04-Q' AS control,
            'SAP Exception' AS scafmetric,
            sap_exception AS scafbreakdown
         FROM
            (
               SELECT
                  'Exception, SAP data found but totals mismatch' AS sap_exception,
               UNION ALL
               SELECT
                  'SAP data not found' AS sap_exception
            )
         UNION ALL
         SELECT
            'A06-M' AS control,
            metric AS scafmetric,
            'None' AS scafbreakdown
         FROM
            (
               SELECT
                  'Not Billed as Planned' AS metric
               UNION ALL
               SELECT
                  'Unpriced Lease' AS metric
            )
         UNION ALL
         SELECT
            'A17-M' AS control,
            metric AS scafmetric,
            'None' AS scafbreakdown
         FROM
            (
               SELECT
                  'Null SAP Net Value' AS metric
               UNION ALL
               SELECT
                  'Vessel is inside committment period' AS metric
            )
         UNION ALL
         SELECT
            'F01-M' AS control,
            'Tasks Remaining' AS scafmetric,
            project_type AS scafbreakdown
         FROM
            (
               SELECT
                  'Contract change' AS project_type
               UNION ALL
               SELECT
                  'New installation' AS project_type
               UNION ALL
               SELECT
                  'Upgrade installation' AS project_type
            )
         UNION ALL
         SELECT DISTINCT
            'F12-M' AS control,
            'ErrorMessageID' AS scafmetric,
            CAST(ErrorMessageID AS STRING) AS scafbreakdown
         FROM
            revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary
         UNION ALL
         SELECT DISTINCT
            'IME01-W' AS control,
            'ErrorMessageID' AS scafmetric,
            ErrorMessageID AS scafbreakdown
         FROM
            revenue-assurance-prod.ime_suspense.IME_Tableau_Summary
         UNION ALL
         SELECT DISTINCT
            'IME02-W' AS control,
            metric AS scafmetric,
            CONCAT(traffic_type, ' - ', IME_AcquisitionPortal) AS scafbreakdown
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
         UNION ALL
         SELECT
            'VAR-1' AS control,
            metric AS scafmetric,
            breakdown AS scafbreakdown
         FROM
            (
               SELECT
                  'category_1' AS metric,
                  'Review needed?' AS breakdown
               UNION ALL
               SELECT
                  'Billed_in_SV_category' AS metric,
                  'HP - billed in last 3 months' AS breakdown
            )
         UNION ALL
         SELECT DISTINCT
            'X01-B' AS control,
            'Category 1' AS scafmetric,
            breakdown AS scafbreakdown
         FROM
            (
               SELECT
                  'Review - active temp stop vessel, why billed with original charges' AS breakdown
               UNION ALL
               SELECT
                  'Review - why billed with suspended charges although they are reactivated' AS breakdown
            )
         UNION ALL
         SELECT
            'GX4-JX' AS control,
            'DAL vs BTP Usage by SSPC' AS scafmetric,
            'Difference Greater than 2.5%' AS scafbreakdown
            -- Table and conditions added for reference
            -- FROM
            --    `revenue-assurance-prod.control_gx4.output_control_outcomes`
            -- WHERE
            --    control_group = 'Usage'
            --    AND control_name = 'DAL vs BTP Usage by SSPC'
            --    AND exception_type = 'Difference greater than 2.5%'
         UNION ALL
         SELECT
            'FC01-Q' AS control,
            'pulse_vs_nuda_category_1' AS scafmetric,
            'Review needed - no charges matching with Vessel ID' AS scafbreakdown
            -- Table and conditions for reference
            -- FROM
            --    `revenue-assurance-prod.key_control_checklist.fc01q_extract`
            -- WHERE
            --    -- Conditions included for reference
            --    pulse_vs_nuda_category_1 = 'Review needed - no charges matching with Vessel ID'
         UNION ALL
         SELECT
            'CH-V' AS control,
            'check_for_Charterer_plan_billied' AS scafmetric,
            'Review for charges - Not found in billing' AS scafbreakdown
            -- Table and conditions for reference
            -- FROM
            --    `revenue-assurance-prod.key_control_checklist.chv_extract`
            -- WHERE
            --    check_for_Charterer_plan_billied = 'Review for charges - Not found in billing'
         UNION ALL
         SELECT
            'A15-Q' AS control,
            'Billing task completed on' AS scafmetric,
            'This month' AS scafbreakdown
            -- Table and conditions for reference
            --       FROM
            --          `revenue-assurance-prod.key_control_checklist.a15q_extract`
            --       WHERE
            --          DATE_TRUNC(billing_task_completed_on, MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
         UNION ALL
         SELECT
            'E05-W' AS control,
            'Review Required' AS scafmetric,
            'Review' AS scafbreakdown
            -- Table and conditions for reference
            --       FROM
            --          `revenue-assurance-prod.control_e05w.output_data`
            --       WHERE
            --          review_required = 'review'
      )
   SELECT
      b.control,
      d.scafdate,
      b.scafmetric,
      b.scafbreakdown
   FROM
      date_scaffold d
      CROSS JOIN breakdown_scaffold b
);