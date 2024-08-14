CREATE OR REPLACE VIEW `revenue-assurance-prod.key_control_checklist.control_refresh_times` AS (
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
);