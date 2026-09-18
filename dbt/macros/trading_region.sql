{#
    Map an ISO country code to the regulated entity that owns the client relationship.

    The platform operates through separately regulated legal entities, and the entity -- not the desk -- is what
    determines which analysts may see a client's row. This is the row-access-policy key: a UK desk
    analyst may only read `trading_region = 'UK'` rows.

    It lives in a macro rather than being repeated in each model so the row filter and the models that
    carry it can never drift apart -- if the mapping changes, every layer changes together.

      UK    -- UK entity        (FCA)
      EMEA  -- EMEA entity      (BaFin)
      APAC  -- APAC entities    (ASIC / MAS)
      US    -- US entity        (SEC / CFTC)
#}
{% macro trading_region(country_column) %}
    case
        when {{ country_column }} in ('GB') then 'UK'
        when {{ country_column }} in ('IE', 'DE', 'ES', 'FR', 'IT', 'NL') then 'EMEA'
        when {{ country_column }} in ('AU', 'SG', 'JP', 'NZ') then 'APAC'
        when {{ country_column }} in ('US') then 'US'
        else 'OTHER'
    end
{% endmacro %}
