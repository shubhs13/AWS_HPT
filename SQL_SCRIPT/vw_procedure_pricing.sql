CREATE OR REPLACE VIEW vw_procedure_pricing AS

SELECT
    p.procedure_key,
    p.procedure_code,
    p.procedure_description,

    COUNT(*) AS pricing_record_count,

    MIN(f.negotiated_price) AS min_negotiated_price,

    MAX(f.negotiated_price) AS max_negotiated_price,

    AVG(f.negotiated_price) AS avg_negotiated_price,

    APPROX_PERCENTILE(
        f.negotiated_price,
        0.5
    ) AS median_negotiated_price,

    MAX(f.negotiated_price)
        - MIN(f.negotiated_price)
        AS price_range,

    (
        (
            MAX(f.negotiated_price)
            - MIN(f.negotiated_price)
        )
        /
        NULLIF(
            APPROX_PERCENTILE(
                f.negotiated_price,
                0.5
            ),
            0
        )
    ) * 100 AS relative_price_range_pct

FROM fact_pricing f

JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key

WHERE f.negotiated_price IS NOT NULL

GROUP BY
    p.procedure_key,
    p.procedure_code,
    p.procedure_description;
