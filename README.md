# Revenue Assurance Key Control Checklist

## Overview
This repository contains scheduled SQL queries used by the Revenue Assurance team to monitor and report the daily frequency of various incident types (controls). The scripts generate and refresh a scaffold table and a unified controls table to ensure consistent day-to-day comparisons. The data is subsequently visualised in a Tableau dashboard.

## Scheduled Queries

| Query                                | Schedule                                           |
|--------------------------------------|----------------------------------------------------|
| `refresh_control_scaffold_daily`     | Daily at 04:00                                     |
| `refresh_unified_controls_every_six_hours` | Four times per day at 05:30, 11:30, 17:30, and 23:30 |

## Tables Used

### Incident Data

Data for each control is sourced from various tables in the `revenue-assurance-prod` dataset.

| Control | Incidents Data Source |
|---------|------------------------|
| AO2-Q   | `control_a02_fx_completeness.output_fx_completeness_snb_control_monthly_data` |
| A04-Q   | `control_a04q_rebill.alteryx_output` |
| A06-M   | `control_a06m_leases.vw_control_monthly` |
| A17-M   | `control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated` |
| F01-M   | `control_f01_m_pulse_projects_reconciliation.control_monthly_data` |
| F12-M   | `control_f12m_btp_suspense.tableau_summary` |
| IME01-W | `ime_suspense.IME_Tableau_Summary` |
| IME02-W | `control_ime_sv.IME_SV_Summary` |
| VAR-1   | `control_var_01_leases.monthly_control_output_for_review2` |
| X01-B   | `control_x01b_retail_fx_temprarary_stopped_vessels_review.control_output_data_temp_stop_vessels` |

### Last Refresh Data

The last refresh time for each control is obtained from the closest parent table's metadata.

| Control | Last Refresh Data Source |
|---------|---------------------------|
| AO2-Q   | `pulse.vw_project_tasks` |
| A04-Q   | `control_a04q_rebill.alteryx_output` |
| A06-M   | `control_a06m_leases.dim_lease_history` |
| A17-M   | `pulse.vw_project_tasks` |
| F01-M   | `pulse.vw_project_tasks` |
| F12-M   | `control_f12m_btp_suspense.sim_tracker` |
| IME01-W | `ime_suspense.eps_Suspense` |
| IME02-W | `control_ime_sv.ime_summary` |
| VAR-1   | `control_var_01_leases.output_var_leases_alteryx_data` |
| X01-B   | `pulse_src.Vessel` |

## Tables Created

| Scheduled Query                        | Table Created                                |
|----------------------------------------|----------------------------------------------|
| `refresh_control_scaffold_daily`       | `key_control_checklist.control_scaffold`     |
| `refresh_unified_controls_every_six_hours` | `key_control_checklist.unified_controls` |

## How to Add New Controls

1. **First Query: `refresh_control_scaffold_daily`**
   - Add a new clause to the `breakdown_scaffold` CTE.
   - Ensure the new control outputs a single column with metrics and a single column with breakdowns.
   - Run the query to verify output.

2. **Second Query: `refresh_unified_controls_every_six_hours`**
   - Add a clause to the `last_refresh_times` CTE for the new control.
   - Create a new CTE to count incidents for the new control.
   - Join to the `control_scaffold` and `last_refresh_times`.
   - Add the new control to the final `SELECT` statement.
   - Run the query to verify output.

3. **Tableau Dashboard**
   - Add parameters and calculations for the new control.
   - Duplicate existing sheets and update filters for the new control.
   - Add new control to the dashboard.

---
