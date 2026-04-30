{{ config(
    materialized='incremental',
    unique_key='sales_invoice_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with sales_invoice as (

    select
        "name" as sales_invoice_id,
        customer,
        posting_date,
        docstatus,
        modified
    from {{ source('erpnext_raw', 'tab_sales_invoice') }}

),

sales_invoice_item as (

    select
        "name" as sales_invoice_item_id,
        parent as sales_invoice_id,
        modified,
        item_name,
        income_account,
        cost_center,
        project as project_id,
        business_unit,
        amount
    from {{ source('erpnext_raw', 'tab_sales_invoice_item') }}

),

final as (

    select
        sii.sales_invoice_item_id,
        si.sales_invoice_id,
        si.customer,
        si.posting_date,

        sii.item_name,
        sii.income_account,
        sii.cost_center,
        sii.project_id,
        sii.business_unit,
        sii.amount,

        -- revenue tracking audit field
        sii.modified as sales_invoice_item_modified_at

    from sales_invoice si
    join sales_invoice_item sii
        on si.sales_invoice_id = sii.sales_invoice_id

    where si.docstatus = 1

    {% if is_incremental() %}
        and sii.modified > (
            select coalesce(max(sales_invoice_item_modified_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}

)

select *
from final