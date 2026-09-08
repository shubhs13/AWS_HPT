CREATE OR REPLACE VIEW vw_procedure_payer_pricing AS

SELECT
    p.procedure_key,
    p.procedure_code,
    p.procedure_description,

    f.payer_name,
    f.plan_name,

    f.care_setting,

    COUNT(*) AS pricing_record_count,

    MIN(f.negotiated_price) AS min_negotiated_price,

    MAX(f.negotiated_price) AS max_negotiated_price,

    AVG(f.negotiated_price) AS avg_negotiated_price,

    APPROX_PERCENTILE(
        f.negotiated_price,
        0.5
    ) AS median_negotiated_price,

    COUNT(DISTINCT f.negotiated_methodology)
        AS pricing_methodology_count

FROM fact_pricing f

JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key

WHERE f.negotiated_price IS NOT NULL

GROUP BY
    p.procedure_key,
    p.procedure_code,
    p.procedure_description,
    f.payer_name,
    f.plan_name,
    f.care_setting;
