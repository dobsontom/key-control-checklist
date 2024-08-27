/*
 *===============================================================================
 * Title:    Control Refresh Times
 * Author:   Tom Dobson (The Information Lab)
 * Date:     14-08-2024
 *===============================================================================
 * Purpose:  This script generates a view with the last refresh time for each 
 *           control tracked in Revenue Assurance's Key Control Checklist. It 
 *           pulls the last modified time from the closest parent table for
 *           each control. This view is generated as part of the scheduled
 *           query refresh_unified_controls_every_six_hours.
 * Docs:     https://bit.ly/key-control-docs
 *===============================================================================
 */
CREATE OR REPLACE VIEW `revenue-assurance-prod.key_control_checklist.control_refresh_times` AS (
   WITH
      -- Refresh time for A02-Q sourced from pulse.vw_project_tasks
      a02q_refresh AS (
         SELECT
            'A02-Q' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.pulse.__TABLES__`
         WHERE
            table_id = 'vw_project_tasks'
      ),
      -- Refresh time for A04-Q sourced from control_a04q_rebill.alteryx_output
      a04q_refresh AS (
         SELECT
            'A04-Q' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.control_a04q_rebill.__TABLES__`
         WHERE
            table_id = 'alteryx_output'
      ),
      -- Refresh time for A06-M sourced from control_a06m_leases.dim_lease_history
      a06m_refresh AS (
         SELECT
            'A06-M' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.control_a06m_leases.__TABLES__`
         WHERE
            table_id = 'dim_lease_history'
      ),
      -- Refresh time for A15-Q sourced from pulse.vw_project_tasks
      a15q_refresh AS (
         SELECT
            'A15-Q' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.pulse.__TABLES__`
         WHERE
            table_id = 'vw_project_tasks'
      ),
      -- Refresh time for A17-M sourced from pulse.vw_project_tasks
      a17m_refresh AS (
         SELECT
            'A17-M' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.pulse.__TABLES__`
         WHERE
            table_id = 'vw_project_tasks'
      ),
      -- Refresh time for CH-V sourced from billing_src.derived_attribute_array
      chv_refresh AS (
         SELECT
            'CH-V' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.billing_src.__TABLES__`
         WHERE
            table_id = 'derived_attribute_array'
      ),
      -- Refresh time for E05-W sourced from control_e05w.output_data
      e05w_refresh AS (
         SELECT
            'E05-W' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.control_e05w.__TABLES__`
         WHERE
            table_id = 'output_data'
      ),
      -- Refresh time for F01-M sourced from pulse.vw_project_tasks
      f01m_refresh AS (
         SELECT
            'F01-M' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.pulse.__TABLES__`
         WHERE
            table_id = 'vw_project_tasks'
      ),
      -- Refresh time for F12-M sourced from control_f12m_btp_suspense.sim_tracker
      f12m_refresh AS (
         SELECT
            'F12-M' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.control_f12m_btp_suspense.__TABLES__`
         WHERE
            table_id = 'sim_tracker'
      ),
      -- Refresh time for FC01-Q sourced from pulse_src.vessel
      fc01q_refresh AS (
         SELECT
            'FC01-Q' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.pulse_src.__TABLES__`
         WHERE
            table_id = 'vessel'
      ),
      -- Refresh time for GX4-JX sourced from control_gx4.output_control_outcomes
      gx4jx_refresh AS (
         SELECT
            'GX4-JX' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.control_gx4.__TABLES__`
         WHERE
            table_id = 'output_control_outcomes'
      ),
      -- Refresh time for IME01-W sourced from ime_suspense.EPS_Suspense
      ime01w_refresh AS (
         SELECT
            'IME01-W' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.ime_suspense.__TABLES__`
         WHERE
            table_id = 'EPS_Suspense'
      ),
      -- Refresh time for IME02-W sourced from control_ime_sv.ime_summary
      ime02w_refresh AS (
         SELECT
            'IME02-W' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.control_ime_sv.__TABLES__`
         WHERE
            table_id = 'ime_summary'
      ),
      -- Refresh time for VAR-1 sourced from control_var_01_leases.output_var_leases_alteryx_data
      var1_refresh AS (
         SELECT
            'VAR-1' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.control_var_01_leases.__TABLES__`
         WHERE
            table_id = 'output_var_leases_alteryx_data'
      ),
      -- Refresh time for X01-B sourced from pulse_src.vessel
      x01b_refresh AS (
         SELECT
            'X01-B' AS control,
            TIMESTAMP_MILLIS(last_modified_time) AS last_refresh_time
         FROM
            `revenue-assurance-prod.pulse_src.__TABLES__`
         WHERE
            table_id = 'vessel'
      ),
      combined_data AS (
         -- Combine all refresh times into a single view
         SELECT
            *
         FROM
            a02q_refresh
         UNION ALL
         SELECT
            *
         FROM
            a04q_refresh
         UNION ALL
         SELECT
            *
         FROM
            a06m_refresh
         UNION ALL
         SELECT
            *
         FROM
            a15q_refresh
         UNION ALL
         SELECT
            *
         FROM
            a17m_refresh
         UNION ALL
         SELECT
            *
         FROM
            chv_refresh
         UNION ALL
         SELECT
            *
         FROM
            e05w_refresh
         UNION ALL
         SELECT
            *
         FROM
            f01m_refresh
         UNION ALL
         SELECT
            *
         FROM
            f12m_refresh
         UNION ALL
         SELECT
            *
         FROM
            fc01q_refresh
         UNION ALL
         SELECT
            *
         FROM
            gx4jx_refresh
         UNION ALL
         SELECT
            *
         FROM
            ime01w_refresh
         UNION ALL
         SELECT
            *
         FROM
            ime02w_refresh
         UNION ALL
         SELECT
            *
         FROM
            var1_refresh
         UNION ALL
         SELECT
            *
         FROM
            x01b_refresh
      )
   SELECT
      control,
      last_refresh_time
   FROM
      combined_data
);
