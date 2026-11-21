select
    lower(trim(coverage_type)) as coverage_type,
    lower(trim(coverage_category)) as coverage_category,
    trim(description) as description
from {{ ref('raw_coverages') }}
