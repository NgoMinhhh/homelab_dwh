{{ config(
    materialized='incremental',
    unique_key='journal_entry_account_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with journal_entry as (

    select
        "name" as journal_entry_id,
        title,
        posting_date,
        user_remark,
        docstatus,
        modified
    from {{ source('erpnext_raw', 'tab_journal_entry') }}

),

journal_entry_account as (

    select
        "name" as journal_entry_account_id,
        parent as journal_entry_id,
        account,
        cost_center,
        project as project_id,
        "BUSINESS_UNIT" as business_unit,
        debit_in_account_currency,
        credit_in_account_currency
    from {{ source('erpnext_raw', 'tab_journal_entry_account') }}

),

final as (

    select
        jea.journal_entry_account_id,
        je.journal_entry_id,
        je.title,
        je.posting_date,
        je.user_remark,
        jea.account,
        jea.cost_center,
        jea.project_id,
        jea.business_unit,
        jea.debit_in_account_currency,
        jea.credit_in_account_currency,

        case
            when jea.debit_in_account_currency > 0 then 'DR'
            when jea.credit_in_account_currency > 0 then 'CR'
            else 'ZERO'
        end as dr_cr_flag,

        jea.debit_in_account_currency - jea.credit_in_account_currency as signed_amount,

        je.modified as journal_entry_modified_at

    from journal_entry je
    join journal_entry_account jea
        on jea.journal_entry_id = je.journal_entry_id

    where je.docstatus = 1

    {% if is_incremental() %}
        and je.modified > (
            select coalesce(max(journal_entry_modified_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}

)

select *
from final