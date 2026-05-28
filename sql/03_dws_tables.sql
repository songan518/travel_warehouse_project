--dws层-用户月度出行主题宽表
create external table if not exists dws.dws_user_travel_month(
    --维度
    blog_id                 string comment "匿名用户id",
    --总量指标
    trip_total_cnt          int comment "本月总出行次数",
    total_spot_cnt          int comment "本月累计游玩景点数",
    --分布于偏好指标
    city_visit_map          map<string, int> comment "本月每个城市的访问次数",
    category_visit_map      map<string, int> comment "本月每个景点分类的访问次数",
    partner_visit_map       map<string, int> comment "本月每种同伴类型的访问次数",
    --时间特征
    first_trip_date         string comment "本月首次出行日期",
    last_trip_date          string comment "本月末次出行日期"
)comment "dws层-用户月度出行主题宽表"
partitioned by (travel_year int, travel_month int)
stored as parquet;

--dws层-景点热度主题表
create external table if not exists dws.dws_attraction_travel_quarter(
    --维度
    poi_id               string comment "景点ID",
    name                 string comment "景点名称",
    city                 string comment "所在城市",
    category             string comment "景点分类",
    --热度指标
    visit_total_cnt      int comment "本季度该景点被游玩的总人次",
    --游客画像指标
    partner_map          map<string, int> comment "游客同伴类型分布",
    --环比变化指标
    visit_cnt_growth_rate double comment "对比上季度总人次增长率",
    is_hot_rising        string comment "是否热度上升：是/否/无上期数据"
)comment "dws层-景点热度主题表"
partitioned by (travel_year int, travel_quarter int)
stored as parquet;

--dws层-城市季度热度主题表
create external table if not exists dws.dws_city_travel_quarter(
    --维度
    city                    string comment "城市名称",
    --热度指标
    visit_trip_cnt          int comment "本季度该城市被访问的总次数",
    unique_attraction_cnt   int comment "本季度该城市被游玩的不重复景点数",
    --游客特征
    partner_map             map<string, int> comment "游客同伴类型分布",
    --环比变化指标
    visit_cnt_growth_rate    double comment "对比上季度用户数增长率",
    is_hot_rising           string comment "是否热度上升：是/否/无上期数据"
)comment "dws层-城市季度热度主题表"
partitioned by (travel_year int, travel_quarter int)
stored as parquet;
