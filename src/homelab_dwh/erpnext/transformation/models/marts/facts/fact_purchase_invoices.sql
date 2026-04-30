{{ config(
    materialized='incremental',
    unique_key='purchase_invoice_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with purchase_invoice as (

    select
        "name" as purchase_invoice_id,
        title,
        posting_date,
        docstatus,
        modified
    from {{ source('erpnext_raw', 'tab_purchase_invoice') }}

),

purchase_invoice_item as (

    select
        "name" as purchase_invoice_item_id,
        parent as purchase_invoice_id,
        expense_account,
        amount,
        description,
        cost_center,
        project as project_id,
        business_unit
    from {{ source('erpnext_raw', 'tab_purchase_invoice_item') }}

),

final as (

    select
        pii.purchase_invoice_item_id,
        pi.purchase_invoice_id,
        pi.title,
        pi.posting_date,

        pii.expense_account,
        pii.amount,
        pii.description,
        pii.cost_center,
        pii.project_id,
        pii.business_unit,

        pi.modified as purchase_invoice_modified_at

    from purchase_invoice pi
    join purchase_invoice_item pii
        on pii.purchase_invoice_id = pi.purchase_invoice_id

    where pi.docstatus = 1

    {% if is_incremental() %}
        and pi.modified > (
            select coalesce(max(purchase_invoice_modified_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}

)

select *
from final