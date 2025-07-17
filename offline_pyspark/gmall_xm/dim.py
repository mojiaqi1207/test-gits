

from pyspark.sql import SparkSession

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Gmall ODS Layer") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置配置参数
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("hive.exec.mode.local.auto", "true")

# 创建数据库
spark.sql("CREATE DATABASE IF NOT EXISTS gmall")
spark.sql("USE gmall")


# ====================================== #
# 全量表定义和数据加载
# ====================================== #

def create_full_tables():
    """创建所有全量ODS表并加载数据"""

    # 1. 活动信息表
    spark.sql("""
    DROP TABLE IF EXISTS ods_activity_info_full;
    CREATE EXTERNAL TABLE ods_activity_info_full(
        `id`            STRING COMMENT '活动id',
        `activity_name` STRING COMMENT '活动名称',
        `activity_type` STRING COMMENT '活动类型',
        `activity_desc` STRING COMMENT '活动描述',
        `start_time`    STRING COMMENT '开始时间',
        `end_time`      STRING COMMENT '结束时间',
        `create_time`   STRING COMMENT '创建时间',
        `operate_time`  STRING COMMENT '修改时间'
    ) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    NULL DEFINED AS ''
    LOCATION '/user/hive/warehouse/gmall.db/ods/ods_activity_info_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
    """)

    # 加载活动信息数据
    spark.sql("""
    INSERT OVERWRITE TABLE ods_activity_info_full PARTITION (dt)
    SELECT
        id,
        activity_name,
        activity_type,
        activity_desc,
        start_time,
        end_time,
        create_time,
        operate_time,
        substr(create_time, 1, 10) AS dt
    FROM activity_info;
    """)

    # 2. 活动规则表（其他表类似结构，以下省略重复注释）
    spark.sql("""
    DROP TABLE IF EXISTS ods_activity_rule_full;
    CREATE EXTERNAL TABLE ods_activity_rule_full(
        `id`               STRING COMMENT '编号',
        `activity_id`      STRING COMMENT '活动ID',
        `activity_type`    STRING COMMENT '活动类型',
        `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',
        `condition_num`    BIGINT COMMENT '满减件数',
        `benefit_amount`   DECIMAL(16, 2) COMMENT '优惠金额',
        `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣',
        `benefit_level`    STRING COMMENT '优惠级别'
    ) 
    PARTITIONED BY (`dt` STRING)
    LOCATION '/user/hive/warehouse/gmall.db/ods/ods_activity_rule_full/'
    TBLPROPERTIES ('compression.codec'='gzip');
    """)
    spark.sql("""
    INSERT OVERWRITE TABLE ods_activity_rule_full PARTITION (dt)
    SELECT *, substr(create_time, 1, 10) AS dt 
    FROM activity_rule;
    """)

    # 3. 商品分类表（1-3级）
    category_tables = [
        ("ods_base_category1_full", "一级品类表"),
        ("ods_base_category2_full", "二级品类表"),
        ("ods_base_category3_full", "三级品类表")
    ]

    for table, comment in category_tables:
        spark.sql(f"""
        DROP TABLE IF EXISTS {table};
        CREATE EXTERNAL TABLE {table}(
            `id`           STRING COMMENT '编号',
            `name`         STRING COMMENT '分类名称',
            `parent_id`    STRING COMMENT '上级分类ID',  -- 仅2/3级表有此字段
            `create_time`  STRING COMMENT '创建时间',
            `operate_time` STRING COMMENT '修改时间'
        ) COMMENT '{comment}'
        PARTITIONED BY (`dt` STRING)
        LOCATION '/user/hive/warehouse/gmall.db/ods/{table}/'
        TBLPROPERTIES ('compression.codec'='gzip');
        """)

        # 数据加载（根据实际表名调整字段）
        spark.sql(f"""
        INSERT OVERWRITE TABLE {table} PARTITION (dt)
        SELECT 
            id, 
            name,
            {'category1_id, ' if '2' in table else ''}
            {'category2_id, ' if '3' in table else ''}
            create_time,
            operate_time,
            substr(create_time, 1, 10) AS dt
        FROM base_category{table.split('_')[2][0]};
        """)

    # 4. 基础字典表
    spark.sql("""
    CREATE EXTERNAL TABLE ods_base_dic_full(
        `dic_code`     STRING COMMENT '编号',
        `dic_name`     STRING COMMENT '编码名称',
        `parent_code`  STRING COMMENT '父编号'
    ) COMMENT '编码字典表'
    PARTITIONED BY (`dt` STRING)
    LOCATION '/user/hive/warehouse/gmall.db/ods/ods_base_dic_full/'
    TBLPROPERTIES ('compression.codec'='gzip');
    """)

    # 5. 省份地区表
    location_tables = [
        ("ods_base_province_full", "省份表",
         "id, name, region_id, area_code, iso_code, iso_3166_2"),
        ("ods_base_region_full", "地区表", "id, region_name")
    ]

    for table, comment, fields in location_tables:
        spark.sql(f"""
        CREATE EXTERNAL TABLE {table}(
            {fields.split(', ')[0]} STRING COMMENT 'ID',
            {fields.split(', ')[1]} STRING COMMENT '名称',
            {', '.join([f + ' STRING' for f in fields.split(', ')[2:]])}
        ) COMMENT '{comment}'
        PARTITIONED BY (`dt` STRING)
        LOCATION '/user/hive/warehouse/gmall.db/ods/{table}/'
        TBLPROPERTIES ('compression.codec'='gzip');
        """)

    # 6. 品牌表
    spark.sql("""
    CREATE EXTERNAL TABLE ods_base_trademark_full(
        `id`        STRING COMMENT '编号',
        `tm_name`   STRING COMMENT '品牌名称',
        `logo_url`  STRING COMMENT 'LOGO路径'
    ) COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    LOCATION '/user/hive/warehouse/gmall.db/ods/ods_base_trademark_full/'
    TBLPROPERTIES ('compression.codec'='gzip');
    """)

    # 7. 购物车表
    spark.sql("""
    CREATE EXTERNAL TABLE ods_cart_info_full(
        `id`            STRING COMMENT '编号',
        `user_id`       STRING COMMENT '用户ID',
        `sku_id`        STRING COMMENT 'SKU_ID',
        `cart_price`    DECIMAL(16,2) COMMENT '加入时价格',
        `sku_num`       BIGINT COMMENT '数量',
        `is_checked`    STRING COMMENT '是否选中',
        `create_time`   STRING COMMENT '创建时间',
        `operate_time`  STRING COMMENT '修改时间',
        `is_ordered`    STRING COMMENT '是否下单',
        `order_time`    STRING COMMENT '下单时间'
    ) COMMENT '购物车全量表'
    PARTITIONED BY (`dt` STRING)
    LOCATION '/user/hive/warehouse/gmall.db/ods/ods_cart_info_full/'
    TBLPROPERTIES ('compression.codec'='gzip');
    """)

    # 8. 优惠券表
    spark.sql("""
    CREATE EXTERNAL TABLE ods_coupon_info_full(
        `id`                 STRING COMMENT '购物券编号',
        `coupon_name`        STRING COMMENT '购物券名称',
        `coupon_type`        STRING COMMENT '购物券类型',
        `condition_amount`   DECIMAL(16,2) COMMENT '满额数',
        `condition_num`      BIGINT COMMENT '满件数',
        `benefit_amount`     DECIMAL(16,2) COMMENT '减免金额',
        `benefit_discount`   DECIMAL(16,2) COMMENT '折扣',
        `range_type`         STRING COMMENT '范围类型',
        `limit_num`          BIGINT COMMENT '最多领用次数',
        `taken_count`        BIGINT COMMENT '已领用次数',
        `start_time`         STRING COMMENT '开始领取时间',
        `end_time`           STRING COMMENT '结束领取时间',
        `expire_time`        STRING COMMENT '过期时间'
    ) COMMENT '优惠券信息表'
    PARTITIONED BY (`dt` STRING)
    LOCATION '/user/hive/warehouse/gmall.db/ods/ods_coupon_info_full/'
    TBLPROPERTIES ('compression.codec'='gzip');
    """)

    # 9. 商品相关表
    product_tables = [
        ("ods_sku_info_full", "商品表",
         "id, spu_id, price, sku_name, weight, tm_id, category3_id"),
        ("ods_sku_attr_value_full", "商品平台属性表",
         "id, attr_id, value_id, sku_id"),
        ("ods_sku_sale_attr_value_full", "商品销售属性值表",
         "id, sku_id, spu_id, sale_attr_value_id, sale_attr_id"),
        ("ods_spu_info_full", "SPU表",
         "id, spu_name, description, category3_id, tm_id")
    ]

    for table, comment, fields in product_tables:
        spark.sql(f"""
        CREATE EXTERNAL TABLE {table}(
            {', '.join([f.split()[0] + ' STRING' for f in fields.split(',')])}
        ) COMMENT '{comment}'
        PARTITIONED BY (`dt` STRING)
        LOCATION '/user/hive/warehouse/gmall.db/ods/{table}/'
        TBLPROPERTIES ('compression.codec'='gzip');
        """)

    # 10. 营销表
    marketing_tables = [
        ("ods_promotion_pos_full", "营销坑位表",
         "id, pos_location, pos_type, promotion_type"),
        ("ods_promotion_refer_full", "营销渠道表",
         "id, refer_name")
    ]

    for table, comment, fields in marketing_tables:
        spark.sql(f"""
        CREATE EXTERNAL TABLE {table}(
            {', '.join([f.split()[0] + ' STRING' for f in fields.split(',')])}
        ) COMMENT '{comment}'
        PARTITIONED BY (`dt` STRING)
        LOCATION '/user/hive/warehouse/gmall.db/ods/{table}/'
        TBLPROPERTIES ('compression.codec'='gzip');
        """)


# ====================================== #
# 增量表定义（使用JSON格式）
# ====================================== #

def create_inc_tables():
    """创建所有增量ODS表（JSON格式）"""

    # 1. 购物车增量表
    spark.sql("""
    DROP TABLE IF EXISTS ods_cart_info_inc;
    CREATE EXTERNAL TABLE ods_cart_info_inc(
        `type` STRING COMMENT '变动类型',
        `ts`   BIGINT COMMENT '变动时间',
        `data` STRUCT<
            id: STRING,
            user_id: STRING,
            sku_id: STRING,
            cart_price: DECIMAL(16,2),
            sku_num: BIGINT,
            sku_name: STRING,
            is_checked: STRING,
            create_time: STRING,
            operate_time: STRING,
            is_ordered: STRING,
            order_time: STRING
        > COMMENT '数据',
        `old` MAP<STRING,STRING> COMMENT '旧值'
    ) COMMENT '购物车增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/user/hive/warehouse/gmall.db/ods/ods_cart_info_inc/'
    TBLPROPERTIES ('compression.codec'='gzip');
    """)

    # 2. 其他增量表结构（按同样模式创建）
    inc_tables = [
        ("ods_comment_info_inc", "评论表", """
            id: STRING,
            user_id: STRING,
            sku_id: STRING,
            order_id: STRING,
            appraise: STRING,
            create_time: STRING
        """),
        ("ods_coupon_use_inc", "优惠券领用表", """
            id: STRING,
            coupon_id: STRING,
            user_id: STRING,
            order_id: STRING,
            coupon_status: STRING,
            get_time: STRING,
            using_time: STRING,
            used_time: STRING
        """),
        ("ods_order_info_inc", "订单表", """
            id: STRING,
            consignee: STRING,
            total_amount: DECIMAL(16,2),
            order_status: STRING,
            user_id: STRING,
            payment_way: STRING,
            out_trade_no: STRING,
            create_time: STRING,
            operate_time: STRING
        """)
    ]

    for table, comment, fields in inc_tables:
        spark.sql(f"""
        CREATE EXTERNAL TABLE {table}(
            `type` STRING,
            `ts`   BIGINT,
            `data` STRUCT<{fields}>,
            `old`  MAP<STRING,STRING>
        ) COMMENT '{comment}'
        PARTITIONED BY (`dt` STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
        LOCATION '/user/hive/warehouse/gmall.db/ods/{table}/'
        TBLPROPERTIES ('compression.codec'='gzip');
        """)

    # 3. 加载增量数据示例（购物车）
    spark.sql("""
    INSERT INTO TABLE ods_cart_info_inc PARTITION(dt='2025-06-27')
    SELECT
        'insert' AS `type`,
        unix_timestamp() * 1000 AS `ts`,
        named_struct(
            'id', id,
            'user_id', user_id,
            'sku_id', sku_id,
            'cart_price', cart_price,
            'sku_num', sku_num,
            'sku_name', sku_name,
            'is_checked', is_checked,
            'create_time', create_time,
            'operate_time', operate_time,
            'is_ordered', is_ordered,
            'order_time', order_time
        ) AS `data`,
        map() AS `old`
    FROM cart_info;
    """)


# ====================================== #
# 执行创建和加载
# ====================================== #
create_full_tables()
create_inc_tables()

# 验证表数据
tables_to_check = [
    "ods_activity_info_full",
    "ods_cart_info_inc",
    "ods_user_info_inc"
]

for table in tables_to_check:
    spark.sql(f"SELECT * FROM {table} LIMIT 5").show()



# 停止Spark会话
spark.stop()