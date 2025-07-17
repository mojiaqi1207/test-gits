from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("info")
date = "20250717"

spark.sql("use gmall_dwd;")

#------------------------------------------------ dwd_trade_cart_add_inc
spark.sql(f"""
    insert into dwd_trade_cart_add_inc partition (dt='20250717')
select
    id,
    user_id,
    sku_id,
    substr(create_time,1,7),
    create_time,
    sku_num
from gmall.ods_cart_info
where dt={date};
""").show()

#------------------------------------------------ dwd_trade_order_detail_inc
spark.sql(f"""
insert overwrite table dwd_trade_order_detail_inc partition (dt={date})
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
    (
        select
            id,
            order_id,
            sku_id,
            create_time,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from gmall.ods_order_detail
        where dt={date}
    ) od
        left join
    (
        select
            id,
            user_id,
            province_id
        from gmall.ods_order_info
        where dt={date}
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from gmall.ods_order_detail_activity
        where dt={date}
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from gmall.ods_order_detail_coupon
        where dt={date}
    ) cou
    on od.id = cou.order_detail_id;
""").show()

#------------------------------------------------ dwd_trade_pay_detail_suc_inc

spark.sql(f"""
    insert into dwd_trade_pay_detail_suc_inc partition (dt={date})
select
    od.id,
    pay.order_id,
    pay.user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    dic_name,
    substr(pay.create_time,1,7),
    callback_time,
    sku_num,
    (split_activity_amount+split_coupon_amount+split_total_amount),
    split_activity_amount,
    split_coupon_amount,
    split_total_amount
from
    (
        select
            id,
            order_id,
            sku_id,
            create_time,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from gmall.ods_order_detail
        where dt={date}
    ) od
        left join
    (
        select
            id,
            user_id,
            province_id
        from gmall.ods_order_info
        where dt={date}
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from gmall.ods_order_detail_activity
        where dt={date}
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from gmall.ods_order_detail_coupon
        where dt={date}
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select * from gmall.ods_payment_info
        where dt={date}
          and payment_status='1602'
    )pay
    on pay.order_id=oi.id
        left join
    (
        select * from gmall.ods_base_dic
        where dt={date}
          and parent_code='11'
    )dic
    on dic.dic_code=pay.payment_type;
""").show()


#------------------------------------------------ dwd_trade_cart_full

spark.sql(f"""
insert into dwd_trade_cart_full partition (dt="20250717")
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from gmall.ods_cart_info
where dt={date}
  and is_ordered=0;
""").show()


#------------------------------------------------ dwd_trade_trade_flow_acc

spark.sql(
    f"""
    with oi as (
    select
        *
    from gmall.ods_order_info
    where dt={date}
),
     pay as(
         select
             *
         from gmall.ods_payment_info
         where dt={date}
           and payment_status='1602'
     ),
     log as (
         select
             *
         from gmall.ods_order_status_log
         where dt={date}
           and order_status='1004'
     )
insert into dwd_trade_trade_flow_acc partition (dt={date})
select
    oi.id,
    oi.user_id,
    oi.province_id,
    substr(oi.create_time,1,7),
    oi.create_time,
    substr(pay.callback_time,1,7),
    pay.callback_time,
    substr(log.create_time,1,7),
    log.create_time,
    (oi.total_amount+oi.activity_reduce_amount+oi.coupon_reduce_amount),
    oi.activity_reduce_amount,
    oi.coupon_reduce_amount,
    oi.total_amount,
    oi.total_amount
from oi
         left join pay on pay.order_id=oi.id
         left join log on log.order_id=oi.id;
    """
).show()

#------------------------------------------------ dwd_tool_coupon_used_inc

spark.sql(f"""
    insert into dwd_tool_coupon_used_inc partition (dt='20250717')
select
    id,
    coupon_id,
    user_id,
    order_id,
    substr(used_time,1,7),
    used_time
from gmall.ods_coupon_use
where dt={date}
  and used_time is not null;
""").show()


#------------------------------------------------ dwd_interaction_favor_add_inc

spark.sql(f"""
    insert into dwd_interaction_favor_add_inc partition (dt={date})
select
    id,
    user_id,
    sku_id,
    substr(create_time,1,7),
    create_time
from gmall.ods_favor_info
where is_cancel='0';
""").show()

#------------------------------------------------ dwd_traffic_page_view_inc

spark.sql(f"""
    insert overwrite table dwd_traffic_page_view_inc partition (dt={date})
SELECT
    get_json_object(log, '$.common.ar') AS province_id,
    get_json_object(log, '$.common.ba') AS brand,
    get_json_object(log, '$.common.ch') AS channel,
    get_json_object(log, '$.common.is_new') AS is_new,
    get_json_object(log, '$.common.md') AS model,
    get_json_object(log, '$.common.mid') AS mid_id,
    get_json_object(log, '$.common.os') AS operate_system,
    get_json_object(log, '$.common.uid') AS user_id,
    get_json_object(log, '$.common.vc') AS version_code,
    get_json_object(log, '$.page.item') AS page_item,
    get_json_object(log, '$.page.item_type') AS page_item_type,
    get_json_object(log, '$.page.last_page_id') AS last_page_id,
    get_json_object(log, '$.page.page_id') AS page_id,
    get_json_object(log, '$.page.from_pos_id') AS from_pos_id,
    get_json_object(log, '$.page.from_pos_seq') AS from_pos_seq,
    get_json_object(log, '$.page.refer_id') AS refer_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), "UTC"), 'yyyy-MM-dd') AS date_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), "UTC"), 'yyyy-MM-dd HH:mm:ss') AS view_time,
    get_json_object(log, '$.common.sid') AS session_id,
    -- 将字符串转换为长整型，使用cast函数处理类型转换
    cast(get_json_object(log, '$.page.during_time') as bigint) AS during_time
FROM gmall.ods_z_log
WHERE dt = {date}
  AND get_json_object(log, '$.page') IS NOT NULL;
    """).show()


#------------------------------------------------ dwd_user_register_inc

spark.sql(f"""
    with use_r as (
    select
        *
    from gmall.ods_user_info
    where dt={date}
),
     log as (
         select
             get_json_object(log,'$.common.ch') channel,
             get_json_object(log,'$.common.ar') province_id,
             get_json_object(log,'$.common.vc') version_code,
             get_json_object(log,'$.common.mid') mid_id,
             get_json_object(log,'$.common.ba') brand,
             get_json_object(log,'$.common.md') model,
             get_json_object(log,'$.common.os') operate_system,
             get_json_object(log,'$.common.uid') uid
         from gmall.ods_z_log
         WHERE dt = {date}
           AND get_json_object(log, '$.page') IS NOT NULL
     )
insert into dwd_user_register_inc partition (dt={date})
select
    use_r.id,
    substr(create_time,1,7),
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,model,
    operate_system
from use_r
         left join log on use_r.id=log.uid;
""").show()


#------------------------------------------------ dwd_user_login_inc
spark.sql(f"""
    insert overwrite table dwd_user_login_inc partition (dt = {date})
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select user_id,
                channel,
                province_id,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts
         from (select get_json_object(log,'$.common.uid') user_id,
                      get_json_object(log,'$.common.ch')  channel,
                      get_json_object(log,'$.common.ar')  province_id,
                      get_json_object(log,'$.common.vc')  version_code,
                      get_json_object(log,'$.common.mid') mid_id,
                      get_json_object(log,'$.common.ba')  brand,
                      get_json_object(log,'$.common.md')  model,
                      get_json_object(log,'$.common.os')  operate_system,
                      get_json_object(log,'$.ts') ts,
                      row_number() over (partition by get_json_object(log,'$.common.sid') order by get_json_object(log,'$.ts')) rn
               from gmall.ods_z_log
               where dt = {date}
                 and get_json_object(log,'$.page') is not null
                 and get_json_object(log,'$.common.uid') is not null) t1
         where rn = 1
     ) t2;
""").show()