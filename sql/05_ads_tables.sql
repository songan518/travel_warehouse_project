create external table if not exists ads.ads_user_portrait(
    blog_id            string comment "用户id",
    total_trips        int comment "总出行次数",
    total_spots        int comment "总游览景点数",
    favorite_city      array<string> comment "最常去的城市",
    favorite_category  array<string> comment "最偏好的景点类型",
    favorite_partner   array<string> comment "最爱的同伴类型",
    user_level         string comment "活跃等级：高频/中频/低频",
    user_status        string comment "用户状态：活跃/流失"
)comment "ads层-用户画像表"
stored as parquet;

--ads层-热门景点排行榜
create external table if not exists ads.ads_top50_attractions_last_year(
    year_quarter         string comment "年份季度",
    poi_id               string comment "景点id",
    name                 string comment "景点名称",
    city                 string comment "所在城市",
    category             string comment "景点分类",
    visit_total_cnt      int comment "访问总人次",
    top_partner          array<string> comment "最热门的出游方式",
    rank                 int comment "季度热度排名",
    is_hot_rising        string comment "是否热度上升",
    user_cnt_growth_rate double comment "环比增长率"
)comment "ads层-最近一年热门景点top50"
stored as parquet;

--ads层-城市热度top10
create external table if not exists ads.ads_city_top10_last_year(
    year_quarter            string comment "年份季度",
    city                    string comment "城市名称",
    visit_trip_cnt          int comment "访问次数",
    unique_attraction_cnt   int comment "被访问的不同景点数",
    top_partner             array<string> comment "最热门的出游方式",
    rank                    int comment "季度热度排名",
    is_hot_rising           string comment "是否热度上升"
)comment "ads层-最近一年城市热度top10"
stored as parquet;
