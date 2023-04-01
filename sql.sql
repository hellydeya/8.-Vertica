drop table if exists --- hello ---.group_log;
create table --- hello ---.group_log(
group_id bigint primary key, 
user_id int, 
user_id_from int, 
event varchar(20), 
datetime datetime
)
order by group_id, user_id
segmented by HASH(group_id, user_id) all NODES
partition by datetime::date
group by calendar_hierarchy_day(datetime::date, 3, 2);


drop table if exists --- hello ---__DWH.l_user_group_activity;
create table --- hello ---__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id  bigint not null constraint fk_l_user_group_activity_user references --- hello ---__DWH.h_users (hk_user_id),
hk_group_id bigint not null constraint fk_l_user_group_activity_group references --- hello ---__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2); 


insert into --- hello ---.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select distinct
hash(hk_group_id, hk_user_id),
hk_user_id,
hk_group_id,
now(),
's3'
from --- hello ---__STAGING.group_log as gl
left join --- hello ---__DWH.h_users as hu on gl.user_id = hu.user_id
left join --- hello ---__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from --- hello ---__DWH.l_user_group_activity);

drop table if exists --- hello ---__DWH.s_auth_history;
create table --- hello ---__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null constraint fk_s_auth_hostory_l_user_group_activity references --- hello ---__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(20),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by event_dt
segmented by hk_l_user_group_activity all nodes
partition by event_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);


insert into --- hello ---__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
select 
hk_l_user_group_activity,
user_id_from,
event,
datetime,
now(),
's3'
from --- hello ---__STAGING.group_log gl
left join --- hello ---__DWH.h_groups hg on gl.group_id = hg.group_id
left join --- hello ---__DWH.h_users hu on gl.user_id = hu.user_id
left join --- hello ---__DWH.l_user_group_activity lug on hg.hk_group_id = lug.hk_group_id and hu.hk_user_id = lug.hk_user_id;




with t1 as(
	select hk_group_id, count(distinct hk_user_id) as cnt_users_in_group_with_messages
	from --- hello ---__DWH.l_user_message as lum
	left join --- hello ---__DWH.l_groups_dialogs as lgd on lum.hk_message_id = lgd.hk_message_id
	left join --- hello ---__DWH.h_dialogs hd on hd.hk_message_id = lgd.hk_message_id
	left join --- hello ---__DWH.s_dialog_info sdi on sdi.hk_message_id = hd.hk_message_id
	where sdi.message is not null
	group by 1),
t2 as (
	select lgd.hk_group_id, count(distinct lum.hk_user_id) as cnt_added_users
	from --- hello ---__DWH.l_user_message as lum
	left join --- hello ---__DWH.l_groups_dialogs as lgd on lum.hk_message_id = lgd.hk_message_id
	left join--- hello ---__DWH.h_users hu on hu.hk_user_id = lum.hk_user_id 
	left join --- hello ---__DWH.l_user_group_activity luga on luga.hk_user_id = hu.hk_user_id
	left join --- hello ---__DWH.s_auth_history sah on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
	where sah.event = 'add'
	group by 1)
select t1.hk_group_id, cnt_added_users, cnt_users_in_group_with_messages, (cnt_users_in_group_with_messages / cnt_added_users) as group_conversion 
from t1 
join t2 on t2.hk_group_id = t1.hk_group_id
ORDER by group_conversion;
