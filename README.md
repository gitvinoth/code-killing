# code-killing

# Delete rules with operation = 'delete'
delete_statistical_rules(spark, logger, rules_df, "`ccus_dd40badc-44a8-4c24-994a-ab80edc83478_dev_03`.bronze_zone.statistical_rules")


Last execution failed
33
33
12
# Delete rules with operation = 'delete'
delete_statistical_rules(spark, logger, rules_df, "`ccus_dd40badc-44a8-4c24-994a-ab80edc83478_dev_03`.bronze_zone.statistical_rules")
2025-10-04 08:31:12,620~|~Statistical_rules_Test~|~ERROR~|~Error in delete_statistical_rules(): [UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `conditions` is "ARRAY<STRUCT<condition_id: INT, condition_name: STRING, asset_id: ARRAY<STRING>, parameter: ARRAY<STRING>, asset_parameters: MAP<STRING, ARRAY<STRING>>, operator: STRING, class: INT, threshold: DOUBLE, duration: INT, wire: STRING, function: STRING, wire_length_from: INT, wire_length_to: INT, rule_run_frequency: INT, sensor_type: STRING, severity: STRING, baseline_time: INT, threshold_unit: STRING, risk_register_controls: ARRAY<INT>, additional_properties: MAP<STRING, INT>, path: STRING>>". SQLSTATE: 0A000;
Deduplicate [conditions#48, tenant_id#46, rule_id#43, join_condition#47, rule_type#45, operation#49, rule_name#44]
+- Filter (lower(trim(operation#49, None)) = delete)
   +- Project [rule_id#43, rule_name#44, rule_type#45, tenant_id#46, join_condition#47, conditions#48, operation#49]
      +- Filter (row_num#81 = 1)
         +- Window [rule_id#43, rule_name#44, rule_type#45, tenant_id#46, join_condition#47, conditions#48, operation#49, epoch_timestamp#72, row_number() windowspecdefinition(rule_id#43, epoch_timestamp#72 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row_num#81], [rule_id#43], [epoch_timestamp#72 DESC NULLS LAST]
            +- Project [rule_id#43, rule_name#44, rule_type#45, tenant_id#46, join_condition#47, conditions#48, operation#49, epoch_timestamp#72]
               +- Project [rule_id#43, rule_name#44, rule_type#45, tenant_id#46, join_condition#47, conditions#48, operation#49, element_at(split(element_at(split(element_at(split(input_file_name(), /, -1), -1, None, false), \., -1), 1, None, false), _, 2), 1, None, false) AS epoch_timestamp#72]
                  +- LogicalRDD [rule_id#43, rule_name#44, rule_type#45, tenant_id#46, join_condition#47, conditions#48, operation#49], false

[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `conditions` is "ARRAY<STRUCT<condition_id: INT, condition_name: STRING, asset_id: ARRAY<STRING>, parameter: ARRAY<STRING>, asset_parameters: MAP<STRING, ARRAY<STRING>>, operator: STRING, class: INT, threshold: DOUBLE, duration: INT, wire: STRING, function: STRING, wire_length_from: INT, wire_length_to: INT, rule_run_frequency: INT, sensor_type: STRING, severity: STRING, baseline_time: INT, threshold_unit: STRING, risk_register_controls: ARRAY<INT>, additional_properties: MAP<STRING, INT>, path: STRING>>". SQLSTATE: 0A000
