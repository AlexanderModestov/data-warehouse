{{
    config(
        materialized='table'
    )
}}

/*
    Mart: Onboarding Analytics

    Daily aggregate statistics of onboarding survey responses.
    Grain: One row per date + funnel + question + answer

    Use cases:
    - Dashboard visualizations of answer distributions
    - Track trends over time by date
    - Compare responses across funnels
    - All-time stats by aggregating: SUM(response_count) GROUP BY funnel_id, question, answer
*/

with key_questions as (
    -- Define the 15 key onboarding questions to include
    select unnest(array[
        'gender',
        'age',
        'reason',
        'start_period',
        'intensity',
        'overwhelmed_frequency',
        'daily_emotional_experience',
        'impact',
        'emotional_support',
        'worry_about_future',
        'disconnected',
        'emotional_ups_downs',
        'emotionally_alone',
        'sleep_difficulty',
        'overthink_about_past',
        'biggest_challenge',
        'top_priority'
    ]) as question
),

funnels as (
    select
        id as funnel_id,
        title as funnel_title
    from {{ source('raw_funnelfox', 'funnels') }}
),

daily_answers as (
    select
        date_trunc('day', r.created_at)::date as date,
        r.funnel_id,
        r.element_custom_id as question,
        r.value as answer,
        count(distinct r.profile_id) as response_count
    from {{ source('raw_facebook', 'onboarding_replies') }} r
    inner join key_questions kq on r.element_custom_id = kq.question
    where r.value is not null
      and r.value != ''
    group by 1, 2, 3, 4
),

with_totals as (
    select
        date,
        funnel_id,
        question,
        answer,
        response_count,
        sum(response_count) over (
            partition by date, funnel_id, question
        ) as question_total
    from daily_answers
)

select
    wt.date,
    wt.funnel_id,
    f.funnel_title,
    wt.question,
    wt.answer,
    wt.response_count,
    wt.question_total,
    round(wt.response_count::numeric / nullif(wt.question_total, 0), 4) as answer_percentage
from with_totals wt
left join funnels f on wt.funnel_id = f.funnel_id
order by wt.date desc, wt.funnel_id, wt.question, wt.response_count desc
