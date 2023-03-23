def model(dbt, session):
    stg_eq = dbt.ref('stg_eq')
    return stg_eq