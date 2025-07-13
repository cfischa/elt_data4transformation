-- dbt macros for the political data transformation project

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro get_political_party_color(party_name) -%}
    {%- set party_colors = {
        'CDU': '#000000',
        'SPD': '#E3000F',
        'GRÜNE': '#1AA037',
        'FDP': '#FFED00',
        'DIE LINKE': '#BE3075',
        'AFD': '#0489DB',
        'BSW': '#722F73',
        'CSU': '#008AC5',
        'DEFAULT': '#808080'
    } -%}
    
    {%- set normalized_party = party_name | upper | replace(' ', '') -%}
    {%- if normalized_party in party_colors -%}
        {{ party_colors[normalized_party] }}
    {%- else -%}
        {{ party_colors['DEFAULT'] }}
    {%- endif -%}
{%- endmacro %}

{% macro calculate_poll_trend(poll_table, party_column, date_column, value_column, days=30) -%}
    WITH poll_trend AS (
        SELECT 
            {{ party_column }},
            {{ date_column }},
            {{ value_column }},
            LAG({{ value_column }}, 1) OVER (
                PARTITION BY {{ party_column }} 
                ORDER BY {{ date_column }}
            ) as previous_value,
            LAG({{ date_column }}, 1) OVER (
                PARTITION BY {{ party_column }} 
                ORDER BY {{ date_column }}
            ) as previous_date
        FROM {{ poll_table }}
        WHERE {{ date_column }} >= today() - {{ days }}
    )
    SELECT 
        {{ party_column }},
        {{ date_column }},
        {{ value_column }},
        CASE 
            WHEN previous_value IS NULL THEN 'NEW'
            WHEN {{ value_column }} > previous_value THEN 'UP'
            WHEN {{ value_column }} < previous_value THEN 'DOWN'
            ELSE 'STABLE'
        END as trend_direction,
        COALESCE({{ value_column }} - previous_value, 0) as trend_change,
        COALESCE(dateDiff('day', previous_date, {{ date_column }}), 0) as days_since_last_poll
    FROM poll_trend
{%- endmacro %}

{% macro get_election_cycle_phase(election_date, current_date='today()') -%}
    CASE 
        WHEN dateDiff('day', {{ current_date }}, {{ election_date }}) > 365 THEN 'EARLY'
        WHEN dateDiff('day', {{ current_date }}, {{ election_date }}) > 90 THEN 'CAMPAIGN'
        WHEN dateDiff('day', {{ current_date }}, {{ election_date }}) > 0 THEN 'FINAL_STRETCH'
        ELSE 'POST_ELECTION'
    END
{%- endmacro %}

{% macro generate_poll_quality_score(sample_size, polling_method, institute_rating) -%}
    (
        -- Sample size component (0-40 points)
        CASE 
            WHEN {{ sample_size }} >= 2000 THEN 40
            WHEN {{ sample_size }} >= 1500 THEN 35
            WHEN {{ sample_size }} >= 1000 THEN 30
            WHEN {{ sample_size }} >= 500 THEN 20
            WHEN {{ sample_size }} >= 100 THEN 10
            ELSE 0
        END +
        
        -- Polling method component (0-30 points)
        CASE 
            WHEN {{ polling_method }} = 'TELEPHONE' THEN 30
            WHEN {{ polling_method }} = 'ONLINE' THEN 25
            WHEN {{ polling_method }} = 'MIXED' THEN 20
            ELSE 10
        END +
        
        -- Institute rating component (0-30 points)
        COALESCE({{ institute_rating }}, 15)
    )
{%- endmacro %}

{% macro clean_party_name(party_name_column) -%}
    trim(
        multiIf(
            {{ party_name_column }} = 'Bündnis 90/Die Grünen', 'GRÜNE',
            {{ party_name_column }} = 'Die Linke', 'DIE LINKE',
            {{ party_name_column }} = 'Alternative für Deutschland', 'AFD',
            {{ party_name_column }} = 'Bündnis Sahra Wagenknecht', 'BSW',
            {{ party_name_column }} = 'Freie Demokratische Partei', 'FDP',
            {{ party_name_column }} = 'Sozialdemokratische Partei Deutschlands', 'SPD',
            {{ party_name_column }} = 'Christlich Demokratische Union', 'CDU',
            {{ party_name_column }} = 'Christlich-Soziale Union', 'CSU',
            upper({{ party_name_column }})
        )
    )
{%- endmacro %}

{% macro create_date_spine(start_date, end_date, date_part='day') -%}
    WITH RECURSIVE date_spine AS (
        SELECT toDate('{{ start_date }}') as date_value
        UNION ALL
        SELECT 
            CASE 
                WHEN '{{ date_part }}' = 'day' THEN date_value + INTERVAL 1 DAY
                WHEN '{{ date_part }}' = 'week' THEN date_value + INTERVAL 1 WEEK
                WHEN '{{ date_part }}' = 'month' THEN date_value + INTERVAL 1 MONTH
                ELSE date_value + INTERVAL 1 DAY
            END as date_value
        FROM date_spine
        WHERE date_value < toDate('{{ end_date }}')
    )
    SELECT date_value FROM date_spine
{%- endmacro %}

{% macro test_poll_data_quality(model_name, min_sample_size=100, max_age_days=365) -%}
    -- Test for data quality issues in poll data
    SELECT 
        'SAMPLE_SIZE_TOO_LOW' as issue_type,
        id,
        sample_size,
        '{{ min_sample_size }}' as threshold
    FROM {{ model_name }}
    WHERE sample_size < {{ min_sample_size }}
    
    UNION ALL
    
    SELECT 
        'POLL_TOO_OLD' as issue_type,
        id,
        toString(poll_date) as poll_date,
        '{{ max_age_days }}' as threshold
    FROM {{ model_name }}
    WHERE dateDiff('day', poll_date, today()) > {{ max_age_days }}
    
    UNION ALL
    
    SELECT 
        'INVALID_PERCENTAGE' as issue_type,
        id,
        toString(poll_value) as poll_value,
        '0-100' as threshold
    FROM {{ model_name }}
    WHERE poll_value < 0 OR poll_value > 100
{%- endmacro %}

{% macro get_parliamentary_majority_threshold(parliament_type='bundestag') -%}
    {%- if parliament_type == 'bundestag' -%}
        50.0
    {%- elif parliament_type == 'landtag' -%}
        50.0
    {%- elif parliament_type == 'europaparlament' -%}
        50.0
    {%- else -%}
        50.0
    {%- endif -%}
{%- endmacro %}

{% macro generate_coalition_combinations(parties_array) -%}
    -- Generate all possible coalition combinations from an array of parties
    -- This is a simplified version - in practice, you'd need more complex logic
    
    -- Two-party coalitions
    {% for party1 in parties_array %}
        {% for party2 in parties_array %}
            {% if party1 != party2 %}
                SELECT 
                    '{{ party1 }}' as party1,
                    '{{ party2 }}' as party2,
                    NULL as party3,
                    2 as coalition_size
                {% if not loop.last or not loop.parent.last %}
                UNION ALL
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endfor %}
{%- endmacro %}
