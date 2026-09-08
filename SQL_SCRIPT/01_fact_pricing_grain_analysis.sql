SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT
        CONCAT(
            CAST(procedure_key AS VARCHAR), '|',
            COALESCE(payer_name, ''), '|',
            COALESCE(plan_name, ''), '|',
            COALESCE(source_code_1, ''), '|',
            COALESCE(source_code_1_type, ''), '|',
            COALESCE(source_code_2, ''), '|',
            COALESCE(source_code_2_type, ''), '|',
            COALESCE(source_code_3, ''), '|',
            COALESCE(source_code_3_type, ''), '|',
            COALESCE(source_code_4, ''), '|',
            COALESCE(source_code_4_type, ''), '|'
        )
    ) AS distinct_grain
FROM fact_pricing;
