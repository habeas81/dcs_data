{{
    config({
        "post-hook": [
            "{{ primary_key(this, 'campaign_id') }}"
        ]
    })
}}

with

source as (
    select * from {{ ref('stg_sdcs__campaigns') }}
),

final as (
    select *
    from source
    order by campaign_id
)

select * from final
