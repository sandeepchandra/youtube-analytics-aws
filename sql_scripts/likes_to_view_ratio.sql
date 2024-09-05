select 
    v.video_id, v.title, v.channel_id, v.published_date, vs.views_count, vs.likes_count, vs.dislikes_count, (vs.likes_count * 1.0 / nullif(vs.views_count, 0)) as like_to_view_ratio, (vs.comments_count * 1.0 / nullif(vs.views_count, 0)) as comment_to_view_ratio
from 
    video_data_dim v
join 
    video_data_fact vs 
    on v.video_id = vs.video_id 
    and vs.snapshot_date = (
        select max(snapshot_date) 
        from video_data_fact 
    )
order by 
    like_to_view_ratio desc;