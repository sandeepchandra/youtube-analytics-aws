-- select chnl_dim.channel_id, chnl_dim.title, chnl_fact.subscribers_count from youtube_analytics_db.channel_data_dim chnl_dim join youtube_analytics_db.channel_data_fact chnl_fact 
-- on chnl_dim.channel_id = chnl_fact.channel_id 
-- and chnl_fact.snapshot_date = (select max(snapshot_date) from youtube_analytics_db.channel_data_fact);

-- select chnl_dim.channel_id, chnl_dim.title, chnl_fact.videos_count from youtube_analytics_db.channel_data_dim chnl_dim join youtube_analytics_db.channel_data_fact chnl_fact 
-- on chnl_dim.channel_id = chnl_fact.channel_id 
-- and chnl_fact.snapshot_date = (select max(snapshot_date) from youtube_analytics_db.channel_data_fact);


-- with cte as (
--     select *, dense_rank() over(order by views_count desc) as views_rank from youtube_analytics_db.video_data_fact where snapshot_date = (select max(snapshot_date) from youtube_analytics_db.video_data_fact)
-- )

-- select vid_dim.title, vid_dim.video_id, vid_dim.channel_id, c.views_count from youtube_analytics_db.video_data_dim vid_dim join (select video_id, views_count from cte where views_rank <= 10) c on vid_dim.video_id = c.video_id order by 4 desc
