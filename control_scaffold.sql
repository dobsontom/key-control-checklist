/*
 *===============================================================================
 * Title:    Refresh Control Scaffold Daily
 * Author:   Tom Dobson (The Information Lab)
 * Date:     14-08-2024
 *===============================================================================
 * Purpose:  This script generates a scaffold (control_scaffold) for the last 
 *           five days for each metric/breakdown combination for each of the 
 *           controls tracked in Revenue Assurance's Key Control Checklist. 
 *           This ensures that days without any incidents have a row with zero 
 *           occurrences rather than no data, enabling day-to-day comparisons.
 * Docs:     https://bit.ly/key-control-docs
 *===============================================================================
 */
CREATE OR REPLACE TABLE `revenue-assurance-prod.key_control_checklist.control_scaffold` AS (
   WITH
      -- Create a scaffold of dates for the last 5 days
      date_scaffold AS (
         SELECT
            *
         FROM
            UNNEST (GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY), DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY))) AS scafdate
      ),
      a02q_scaffold AS (
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
      ),
      a04q_scaffold AS (
         SELECT
            'A04-Q' AS control,
            'SAP Exception' AS scafmetric,
            sap_exception AS scafbreakdown
         FROM
            (
               SELECT
                  'Exception, SAP data found but totals mismatch' AS sap_exception
               UNION ALL
               SELECT
                  'SAP data not found' AS sap_exception
            )
      ),
      a06m_scaffold AS (
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
      ),
      a15q_scaffold AS (
         SELECT
            'A15-Q' AS control,
            'Billing task completed on' AS scafmetric,
            'This month' AS scafbreakdown
      ),
      a17m_scaffold AS (
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
      ),
      chv_scaffold AS (
         SELECT
            'CH-V' AS control,
            'check_for_Charterer_plan_billied' AS scafmetric,
            'Review for charges - Not found in billing' AS scafbreakdown
      ),
      e05w_scaffold AS (
         SELECT
            'E05-W' AS control,
            'Review Required' AS scafmetric,
            'Review' AS scafbreakdown
      ),
      f01m_scaffold AS (
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
      ),
      f12m_scaffold AS (
         SELECT DISTINCT
            'F12-M' AS control,
            'ErrorMessageID' AS scafmetric,
            CAST(errormessageid AS STRING) AS scafbreakdown
         FROM
            `revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary`
      ),
      fc01q_scaffold AS (
         SELECT
            'FC01-Q' AS control,
            'pulse_vs_nuda_category_1' AS scafmetric,
            'Review needed - no charges matching with Vessel ID' AS scafbreakdown
      ),
      gx4jx_scaffold AS (
         SELECT
            'GX4-JX' AS control,
            'DAL vs BTP Usage by SSPC' AS scafmetric,
            'Difference Greater than 2.5%' AS scafbreakdown
      ),
      ime01w_scaffold AS (
         SELECT DISTINCT
            'IME01-W' AS control,
            'ErrorMessageID' AS scafmetric,
            errormessageid AS scafbreakdown
         FROM
            `revenue-assurance-prod.ime_suspense.IME_Tableau_Summary`
      ),
      ime02w_scaffold AS (
         SELECT DISTINCT
            'IME02-W' AS control,
            metric AS scafmetric,
            CONCAT(traffic_type, ' - ', ime_acquisitionportal) AS scafbreakdown
         FROM
            `revenue-assurance-prod.control_ime_sv.IME_SV_Summary`
            CROSS JOIN (
               SELECT DISTINCT
                  metric
               FROM
                  `revenue-assurance-prod.control_ime_sv.IME_SV_Summary` UNPIVOT(
                     control_count
                     FOR metric IN (files_collected, ime_totrecsrecvd, ime_v_sv_difference)
                  )
            )
      ),
      var1_scaffold AS (
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
      ),
      x01b_scaffold AS (
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
      ),
      -- Union individual control scaffolds
      breakdown_scaffold AS (
         SELECT
            *
         FROM
            a02q_scaffold
         UNION ALL
         SELECT
            *
         FROM
            a04q_scaffold
         UNION ALL
         SELECT
            *
         FROM
            a06m_scaffold
         UNION ALL
         SELECT
            *
         FROM
            a15q_scaffold
         UNION ALL
         SELECT
            *
         FROM
            a17m_scaffold
         UNION ALL
         SELECT
            *
         FROM
            chv_scaffold
         UNION ALL
         SELECT
            *
         FROM
            e05w_scaffold
         UNION ALL
         SELECT
            *
         FROM
            f01m_scaffold
         UNION ALL
         SELECT
            *
         FROM
            f12m_scaffold
         UNION ALL
         SELECT
            *
         FROM
            fc01q_scaffold
         UNION ALL
         SELECT
            *
         FROM
            gx4jx_scaffold
         UNION ALL
         SELECT
            *
         FROM
            ime01w_scaffold
         UNION ALL
         SELECT
            *
         FROM
            ime02w_scaffold
         UNION ALL
         SELECT
            *
         FROM
            var1_scaffold
         UNION ALL
         SELECT
            *
         FROM
            x01b_scaffold
      )
      -- Cross join date and breakdown scaffolds to create the
      -- final control scaffold
   SELECT
      b.control,
      d.scafdate,
      b.scafmetric,
      b.scafbreakdown
   FROM
      date_scaffold d
      CROSS JOIN breakdown_scaffold b
);
