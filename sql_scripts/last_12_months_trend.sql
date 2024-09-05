with cte as
(
    select * from youtube_analytics_stats.video_data_dim vd where vd.published_date between date_add('year', -365, current_date) and current_date
),
videos_per_year_uploads as (
    select channel_id, count(*) as video_cnt_in_year from cte group by channel_id order by 2 desc
),
videos_per_quarter as (
    select channel_id, concat('Q',cast(quarter(published_date) as varchar)) as quater, count(*) as video_cnt_per_quater from cte group by quarter(published_date), channel_id order by 1,2
),
videos_per_month as (
    select channel_id, 
    CASE WHEN month(published_date) = 1 THEN 'January'
        WHEN month(published_date) = 2 THEN 'Febuary'
        WHEN month(published_date) = 3 THEN 'March'
        WHEN month(published_date) = 4 THEN 'April'
        WHEN month(published_date) = 5 THEN 'May'
        WHEN month(published_date) = 6 THEN 'June'
        WHEN month(published_date) = 7 THEN 'July'
        WHEN month(published_date) = 8 THEN 'August'
        WHEN month(published_date) = 9 THEN 'September'
        WHEN month(published_date) = 10 THEN 'October'
        WHEN month(published_date) = 11 THEN 'November'
        ELSE 'December' END as month_name,
    count(*) as video_cnt_per_month from cte group by channel_id, month(published_date) order by 1,2
)

select * from videos_per_year_uploads

select * from videos_per_quarter

select * from videos_per_month