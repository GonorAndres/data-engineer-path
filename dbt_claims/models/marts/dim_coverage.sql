select
    row_number() over (order by coverage_type) as coverage_key,
    coverage_type,
    coverage_category,
    description
from {{ ref('stg_coverages') }}
