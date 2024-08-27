CREATE OR REPLACE VIEW `revenue-assurance-prod.key_control_checklist.control_refresh_times` AS (
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: A02-Q, A17-M, and F01-M 
      `revenue-assurance-prod.pulse.__TABLES__`
   WHERE
      table_id = 'vw_project_tasks'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: A04-Q
      `revenue-assurance-prod.control_a04q_rebill.__TABLES__`
   WHERE
      table_id = 'alteryx_output'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: A06-M
      `revenue-assurance-prod.control_a06m_leases.__TABLES__`
   WHERE
      table_id = 'dim_lease_history'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: F12-M
      `revenue-assurance-prod.control_f12m_btp_suspense.__TABLES__`
   WHERE
      table_id = 'sim_tracker'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: IMEO1-W
      `revenue-assurance-prod.ime_suspense.__TABLES__`
   WHERE
      table_id = 'EPS_Suspense'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: IMEO2-W
      `revenue-assurance-prod.control_ime_sv.__TABLES__`
   WHERE
      table_id = 'ime_summary'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: VAR-1
      `revenue-assurance-prod.control_var_01_leases.__TABLES__`
   WHERE
      table_id = 'output_var_leases_alteryx_data'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: X01-B
      `revenue-assurance-prod.pulse_src.__TABLES__`
   WHERE
      table_id = 'vessel'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: GX4-JX
      `revenue-assurance-prod.control_gx4.__TABLES__`
   WHERE
      table_id = 'output_control_outcomes'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: FC01-Q
      `revenue-assurance-prod.key_control_checklist.__TABLES__`
   WHERE
      table_id = 'fc01q_extract'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: CH-V
      `revenue-assurance-prod.key_control_checklist.__TABLES__`
   WHERE
      table_id = 'chv_extract'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: A15-Q
      `revenue-assurance-prod.key_control_checklist.__TABLES__`
   WHERE
      table_id = 'a15q_extract'
   UNION ALL
   SELECT
      table_id,
      TIMESTAMP_MILLIS(last_modified_time) AS last_refresh
   FROM
      -- Last refresh source for: E05-W
      `revenue-assurance-prod.control_e05w.__TABLES__`
   WHERE
      table_id = 'output_data'
);