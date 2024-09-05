select chnl_dim.channel_id, chnl_dim.title, chnl_fact.videos_count from youtube_analytics_stats.channel_data_dim chnl_dim join youtube_analytics_stats.channel_data_fact chnl_fact 
on chnl_dim.channel_id = chnl_fact.channel_id 
and chnl_fact.snapshot_date = (select max(snapshot_date) from youtube_analytics_stats.channel_data_fact);