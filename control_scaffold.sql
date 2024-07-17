/*-------------------------------------------------------------------------------------*
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
   |------------------------------------------------------------------------------------|
   | Modifications:                                                                     |
   *-------------------------------------------------------------------------------------
*/

CREATE OR REPLACE TABLE
    revenue-assurance-prod.key_control_checklist.control_scaffold AS (
        WITH
            date_scaffold AS (
                SELECT
                    *
                FROM
                    UNNEST (
                        GENERATE_DATE_ARRAY(
                            DATE_ADD(CURRENT_DATE, INTERVAL - 5 DAY),
                            DATE_ADD(CURRENT_DATE, INTERVAL - 1 DAY)
                        )
                    ) AS scafdate
            ),
            breakdown_scaffold AS (
                SELECT DISTINCT
                    'A02-Q' AS control,
                    'Category 1' AS scafmetric,
                    category1 AS scafbreakdown,
                FROM
                    revenue-assurance-prod.control_a02_fx_completeness.output_fx_completeness_snb_control_monthly_data
                WHERE
                    category1 IN (
                        'Review for charges',
                        'Timing issue - active billing billing task - check in the next control run'
                    )
                UNION DISTINCT
                SELECT DISTINCT
                    'A04-Q' AS control,
                    'SAP Exception' AS scafmetric,
                    sap_exception AS scafbreakdown,
                FROM
                    revenue-assurance-prod.control_a04q_rebill.alteryx_output
                WHERE
                    sap_exception IN (
                        'Exception, SAP data found but totals mismatch',
                        'SAP data not found'
                    )
                UNION DISTINCT
                SELECT DISTINCT
                    'A06-M' AS control,
                    metric AS scafmetric,
                    'None' AS scafbreakdown,
                FROM
                    revenue-assurance-prod.control_a06m_leases.vw_control_monthly
                    CROSS JOIN (
                        SELECT
                            metric
                            -- Need to create unified metric column via union rather than unpivot, as can't unpivot
                            -- fields with different data types into the same field.
                        FROM
                            (
                                SELECT DISTINCT
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
                                SELECT DISTINCT
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
                            )
                    )
                UNION DISTINCT
                -- A17-M has two metrics which need to be merged into a single metric
                -- field to create full scaffolding in the unified format.
                SELECT DISTINCT
                    'A17-M' AS control,
                    metric AS scafmetric,
                    'None' AS scafbreakdown
                FROM
                    revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
                    CROSS JOIN (
                        SELECT
                            metric
                            -- Need to create unified metric column via unions rather than unpivot, as can't unpivot
                            -- fields with different data types, even when casting as string.
                        FROM
                            (
                                SELECT DISTINCT
                                    IFNULL(
                                        CAST(sap_net_value AS STRING),
                                        'Null SAP Net Value'
                                    ) AS metric
                                FROM
                                    revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
                                WHERE
                                    sap_net_value IS NULL
                                UNION ALL
                                SELECT DISTINCT
                                    is_vessel_ooc AS metric
                                FROM
                                    revenue-assurance-prod.control_a17_m_fx_retail_early_terminations_fees.ETF_control_pulse_and_sdp_fees_calculated
                                WHERE
                                    is_vessel_ooc = 'Vessel is inside committment period'
                            )
                    )
                UNION DISTINCT
                SELECT DISTINCT
                    'F01-M' AS control,
                    'Tasks Remaining' AS scafmetric,
                    project_type AS scafbreakdown
                FROM
                    revenue-assurance-prod.control_f01_m_pulse_projects_reconciliation.control_monthly_data
                WHERE
                    project_type IN (
                        'Contract change',
                        'New installation',
                        'Upgrade installation'
                    )
                UNION DISTINCT
                SELECT DISTINCT
                    'F12-M' AS control,
                    'ErrorMessageID' AS scafmetric,
                    CAST(ErrorMessageID AS STRING) AS scafbreakdown
                FROM
                    revenue-assurance-prod.control_f12m_btp_suspense.tableau_summary
                UNION DISTINCT
                SELECT DISTINCT
                    'IME01-W' AS control,
                    'ErrorMessageID' AS scafmetric,
                    ErrorMessageID AS scafbreakdown
                FROM
                    revenue-assurance-prod.ime_suspense.IME_Tableau_Summary
                UNION DISTINCT
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
                UNION DISTINCT
                SELECT DISTINCT
                    'VAR-1' AS control,
                    metric AS scafmetric,
                    value AS scafbreakdown
                FROM
                    revenue-assurance-prod.control_var_01_leases.monthly_control_output_for_review2 UNPIVOT(
                        value
                        FOR metric IN (category_1, Billed_in_SV_category)
                    )
                WHERE
                    (
                        metric = 'category_1'
                        AND value = 'Review needed?'
                    )
                    OR (
                        metric = 'Billed_in_SV_category'
                        AND value = 'HP - billed in last 3 months'
                    )
                UNION DISTINCT
                SELECT DISTINCT
                    'X01-B' AS control,
                    'Category 1' AS scafmetric,
                    category1 AS scafbreakdown,
                FROM
                    revenue-assurance-prod.control_x01b_retail_fx_temprarary_stopped_vessels_review.control_output_data_temp_stop_vessels
                WHERE
                    category1 IN (
                        'Review - active temp stop vessel, why billed with original charges',
                        'Review - why billed with suspended charges although they are reactivated'
                    )
            )
        SELECT
            breakdown_scaffold.control,
            date_scaffold.scafdate,
            breakdown_scaffold.scafmetric,
            breakdown_scaffold.scafbreakdown
        FROM
            date_scaffold
            CROSS JOIN breakdown_scaffold
    );