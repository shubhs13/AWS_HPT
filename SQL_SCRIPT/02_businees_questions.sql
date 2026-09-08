-- ============================================================
-- Business Question Catalog
-- Project: Hospital Price Transparency
-- Hospital: Methodist Hospital For Surgery
-- Dataset: Gold FACT_PRICING
-- ============================================================

-- Purpose:
-- Define the core business questions that the analytical/serving
-- layer must answer for the current Methodist MVP dataset.

-- These questions will drive the design of analytical views
-- and downstream Tableau analysis.

-- Current dataset:
--   FACT_PRICING rows: 166,416
--   Procedures:        11,362
--   Payers:             45
--   Plans:              35
--   Hospitals:           1

-- IMPORTANT:
-- This catalog defines analytical requirements.
-- It does not contain the final analytical queries/views.
-- ============================================================


-- ============================================================
-- BUSINESS QUESTION 1
-- What is the negotiated price for a specific procedure,
-- payer, and plan?
-- ============================================================

-- Business purpose:
-- Allow an analyst to identify the negotiated price associated
-- with a procedure under a particular payer/plan.

-- Key fields:
--   procedure_code
--   procedure_description
--   payer_name
--   plan_name
--   negotiated_price
--   negotiated_methodology
--   care_setting

-- Grain:
-- Individual FACT_PRICING record.

SELECT
    p.procedure_code,
    p.procedure_description,
    f.payer_name,
    f.plan_name,
    f.care_setting,
    f.negotiated_price,
    f.negotiated_methodology,
    f.negotiated_algorithm
FROM fact_pricing f
JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key
WHERE p.procedure_code = '99205'
ORDER BY
    f.payer_name,
    f.plan_name;


-- Important:
-- A procedure + payer + plan can have multiple pricing records,
-- so the analytical layer must not assume a single price exists.


-- ============================================================
-- BUSINESS QUESTION 2
-- What are the lowest, highest, and typical negotiated prices
-- for a procedure?
-- ============================================================

-- Business purpose:
-- Understand the pricing range for a procedure across the
-- available pricing records.

-- Metrics:
--   MIN(negotiated_price)
--   MAX(negotiated_price)
--   AVG(negotiated_price)
--   MEDIAN(negotiated_price)

-- Grain:
-- Procedure.
SELECT
    p.procedure_code,
    p.procedure_description,
    COUNT(*) AS pricing_record_count,
    MIN(f.negotiated_price) AS min_negotiated_price,
    MAX(f.negotiated_price) AS max_negotiated_price,
    AVG(f.negotiated_price) AS avg_negotiated_price,
    APPROX_PERCENTILE(f.negotiated_price, 0.5) AS median_negotiated_price
FROM fact_pricing f
JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key
WHERE f.negotiated_price IS NOT NULL
GROUP BY
    p.procedure_code,
    p.procedure_description
ORDER BY
    p.procedure_code;

-- Null negotiated prices should be excluded from price
-- calculations.


-- ============================================================
-- BUSINESS QUESTION 3
-- Compare gross charge and negotiated price at the procedure level while preserving cases where negotiated price exceeds gross charge.
-- ============================================================

-- Business purpose:
-- Measure the difference between the hospital's gross charge
-- and negotiated pricing.

-- Metrics:
--   gross_charge
--   negotiated_price
--   negotiated_price / gross_charge
--   gross_charge - negotiated_price

-- Potential derived metric:
--   negotiated_vs_gross_pct

-- Formula:
--   (negotiated_price / gross_charge) * 100

-- Gross charges of zero or NULL must be handled to avoid
-- invalid percentage calculations.

SELECT
    p.procedure_code,
    p.procedure_description,
    COUNT(*) AS pricing_record_count,

    AVG(f.gross_charge) AS avg_gross_charge,
    AVG(f.negotiated_price) AS avg_negotiated_price,

    AVG(f.gross_charge - f.negotiated_price) AS avg_price_difference,

    AVG(
        CASE
            WHEN f.gross_charge > 0
            THEN (f.gross_charge - f.negotiated_price)
                 / f.gross_charge * 100
        END
    ) AS avg_discount_percentage

FROM fact_pricing f
JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key

WHERE f.gross_charge IS NOT NULL
  AND f.negotiated_price IS NOT NULL
  AND f.gross_charge > 0

GROUP BY
    p.procedure_code,
    p.procedure_description

ORDER BY
    p.procedure_code;


-- ============================================================
-- BUSINESS QUESTION 4
-- How does the discounted cash price compare with negotiated
-- prices?
-- ============================================================

-- Business purpose:
-- Compare the hospital's self-pay/cash price with negotiated
-- payer prices.

-- Metrics:
--   discounted_cash_price
--   negotiated_price
--   cash_vs_negotiated_difference
--   cash_vs_negotiated_pct

-- This can help identify procedures where the cash price is
-- lower or higher than negotiated payer prices.

SELECT
    p.procedure_code,
    p.procedure_description,
    COUNT(*) AS pricing_record_count,

    AVG(f.discounted_cash_price) AS avg_discounted_cash_price,
    AVG(f.negotiated_price) AS avg_negotiated_price,

    AVG(
        f.discounted_cash_price - f.negotiated_price
    ) AS avg_cash_vs_negotiated_difference,

    AVG(
        CASE
            WHEN f.negotiated_price > 0
            THEN (f.discounted_cash_price - f.negotiated_price)
                 / f.negotiated_price * 100
        END
    ) AS avg_cash_vs_negotiated_percentage

FROM fact_pricing f
JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key

WHERE f.discounted_cash_price IS NOT NULL
  AND f.negotiated_price IS NOT NULL
  AND f.negotiated_price > 0

GROUP BY
    p.procedure_code,
    p.procedure_description

ORDER BY
    p.procedure_code;


-- ============================================================
-- BUSINESS QUESTION 5
-- Which procedures have the largest pricing variation?
-- ============================================================

-- Business purpose:
-- Identify procedures with substantial variation in negotiated
-- prices.

-- Primary metrics:
--   MIN(negotiated_price)
--   MAX(negotiated_price)
--   AVG(negotiated_price)
--   MEDIAN(negotiated_price)

-- Derived metric:
--   price_range = MAX(negotiated_price)
--                 - MIN(negotiated_price)

-- Candidate ranking:
--   ORDER BY price_range DESC

-- Minimum record-count thresholds may be applied later to avoid
-- highlighting procedures with insufficient observations.

SELECT
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
        - MIN(f.negotiated_price) AS price_range,

    (
        (
            MAX(f.negotiated_price)
            - MIN(f.negotiated_price)
        )
        /
        NULLIF(
            APPROX_PERCENTILE(f.negotiated_price, 0.5),
            0
        )
    ) * 100 AS relative_price_range_pct

FROM fact_pricing f
JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key

WHERE f.negotiated_price IS NOT NULL

GROUP BY
    p.procedure_code,
    p.procedure_description

ORDER BY
    relative_price_range_pct DESC;


-- ============================================================
-- BUSINESS QUESTION 6
-- Which payer and plan combinations have the most pricing
-- records?
-- ============================================================

-- Business purpose:
-- Understand payer/plan representation in the Methodist dataset.

-- Metrics:
--   COUNT(*) AS pricing_record_count

-- Dimensions:
--   payer_name
--   plan_name

-- This is a coverage/cardinality analysis and should not be
-- interpreted as market share or utilization.
SELECT
    f.payer_name,
    f.plan_name,
    COUNT(*) AS pricing_record_count
FROM fact_pricing f
WHERE f.payer_name IS NOT NULL
  AND TRIM(f.payer_name) <> ''
  AND f.plan_name IS NOT NULL
  AND TRIM(f.plan_name) <> ''
GROUP BY
    f.payer_name,
    f.plan_name
ORDER BY
    pricing_record_count DESC;

-- ============================================================
-- BUSINESS QUESTION 7
-- What is the average negotiated price by procedure?
-- ============================================================

-- Business purpose:
-- Provide a procedure-level pricing benchmark.

-- Metrics:
--   AVG(negotiated_price)
--   MEDIAN(negotiated_price)
--   MIN(negotiated_price)
--   MAX(negotiated_price)
--   COUNT(*) AS pricing_record_count

-- Grain:
-- One row per procedure.
SELECT
    p.procedure_code,
    p.procedure_description,
    COUNT(*) AS pricing_record_count,
    AVG(f.negotiated_price) AS avg_negotiated_price
FROM fact_pricing f
JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key
WHERE f.negotiated_price IS NOT NULL
GROUP BY
    p.procedure_code,
    p.procedure_description
ORDER BY
    avg_negotiated_price DESC;


-- ============================================================
-- BUSINESS QUESTION 8
-- Which procedures have unusually high gross charges?
-- ============================================================

-- Business purpose:
-- Identify procedures with high listed/gross charges for
-- further investigation.

-- Metrics:
--   AVG(gross_charge)
--   MEDIAN(gross_charge)
--   MAX(gross_charge)

-- The analytical layer should preserve the distinction between
-- gross charge and actual negotiated price.
SELECT
    p.procedure_code,
    p.procedure_description,
    COUNT(*) AS pricing_record_count,
    AVG(f.gross_charge) AS avg_gross_charge,
    APPROX_PERCENTILE(f.gross_charge, 0.5) AS median_gross_charge,
    MAX(f.gross_charge) AS max_gross_charge
FROM fact_pricing f
JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key
WHERE f.gross_charge IS NOT NULL
GROUP BY
    p.procedure_code,
    p.procedure_description
ORDER BY
    avg_gross_charge DESC;

-- ============================================================
-- BUSINESS QUESTION 9
-- How do pricing methodologies differ across pricing records?
-- ============================================================

-- Business purpose:
-- Understand how negotiated prices are represented in the MRF.

-- Dimension:
--   negotiated_methodology

-- Example values observed in the dataset may include:
--   case rate
--   per diem
--   other

-- Metrics:
--   COUNT(*)
--   AVG(negotiated_price)
--   MEDIAN(negotiated_price)
--   MIN(negotiated_price)
--   MAX(negotiated_price)

-- IMPORTANT:
-- Different methodologies may represent fundamentally different
-- pricing arrangements. They should not automatically be treated
-- as directly comparable without business interpretation.
SELECT
    f.negotiated_methodology,
    COUNT(*) AS pricing_record_count,
    COUNT(DISTINCT f.procedure_key) AS distinct_procedures,
    AVG(f.negotiated_price) AS avg_negotiated_price,
    APPROX_PERCENTILE(
        f.negotiated_price,
        0.5
    ) AS median_negotiated_price,
    MIN(f.negotiated_price) AS min_negotiated_price,
    MAX(f.negotiated_price) AS max_negotiated_price
FROM fact_pricing f
WHERE f.negotiated_price IS NOT NULL
GROUP BY
    f.negotiated_methodology
ORDER BY
    pricing_record_count DESC;

-- ============================================================
-- BUSINESS QUESTION 10
-- Which procedures have missing or incomplete payer/plan
-- information?
-- ============================================================

-- Business purpose:
-- Quantify and identify pricing records where payer or plan
-- information is missing or blank.

-- Metrics:
--   COUNT(*) AS incomplete_record_count

-- Dimensions:
--   procedure
--   payer_name
--   plan_name

-- Missing values should include both NULL and blank/whitespace
-- values where applicable.
SELECT
    COUNT(*) AS total_records,

    SUM(
        CASE
            WHEN payer_name IS NULL
              OR TRIM(payer_name) = ''
            THEN 1
            ELSE 0
        END
    ) AS missing_payer_records,

    SUM(
        CASE
            WHEN plan_name IS NULL
              OR TRIM(plan_name) = ''
            THEN 1
            ELSE 0
        END
    ) AS missing_plan_records,

    SUM(
        CASE
            WHEN (
                payer_name IS NULL
                OR TRIM(payer_name) = ''
                OR plan_name IS NULL
                OR TRIM(plan_name) = ''
            )
            THEN 1
            ELSE 0
        END
    ) AS incomplete_payer_plan_records

FROM fact_pricing;


-- ============================================================
-- BUSINESS QUESTION 11
-- How many pricing records exist for each procedure?
-- ============================================================

-- Business purpose:
-- Understand the number of pricing observations available for
-- each procedure.

-- Metrics:
--   COUNT(*) AS pricing_record_count

-- Grain:
-- One row per procedure.

-- This metric is useful for interpreting other procedure-level
-- statistics and identifying procedures with limited observations.
SELECT
    p.procedure_code,
    p.procedure_description,
    COUNT(*) AS pricing_record_count
FROM fact_pricing f
JOIN dim_procedure p
    ON f.procedure_key = p.procedure_key
GROUP BY
    p.procedure_code,
    p.procedure_description
ORDER BY
    pricing_record_count DESC;


-- ============================================================
-- BUSINESS QUESTION 12
-- How do pricing records vary by care setting?
-- ============================================================

-- Business purpose:
-- Determine whether pricing characteristics differ across
-- available care settings.

-- Dimensions:
--   care_setting

-- Metrics:
--   COUNT(*)
--   AVG(negotiated_price)
--   MEDIAN(negotiated_price)
--   MIN(negotiated_price)
--   MAX(negotiated_price)

-- IMPORTANT:
-- care_setting is NOT part of the validated unique grain for
-- the current dataset. It is treated as a descriptive analytical
-- attribute.


-- ============================================================
-- CORE ANALYTICAL OUTPUTS
-- ============================================================

-- The business questions above will be consolidated into a
-- small number of reusable analytical views rather than creating
-- one view per question.

-- Planned analytical views:

-- 1. vw_procedure_pricing
--     Procedure-level pricing statistics.

-- 2. vw_procedure_payer_pricing
--     Procedure + payer + plan pricing analysis.

-- 3. vw_pricing_comparison
--     Gross charge vs cash price vs negotiated price.

-- Additional views will be created only if they provide a clear
-- analytical need.

-- ============================================================
-- END OF BUSINESS QUESTION CATALOG
-- ============================================================
