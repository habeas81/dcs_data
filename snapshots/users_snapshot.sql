{% snapshot users_snapshot %}


{{
    config(
        unique_key='id',
        target_schema='snapshots',
        strategy='timestamp',
        updated_at='last_seen'
    )
}}

select * from {{ source('sdcs', 'user')}}

{% endsnapshot %}
