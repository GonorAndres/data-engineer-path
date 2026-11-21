{#
  SCD Type 2 history of policies: when a tracked column changes, the old row
  is closed (dbt_valid_to set) and a new row is inserted.
  The source is static seed data, so every policy has exactly one version
  until the source changes.
#}
{% snapshot snap_policies %}
    {{
        config(
            target_schema=target.schema,
            unique_key='policy_id',
            strategy='check',
            check_cols=['status', 'annual_premium', 'coverage_limit', 'deductible', 'expiration_date'],
        )
    }}

    select * from {{ ref('stg_policies') }}
{% endsnapshot %}
