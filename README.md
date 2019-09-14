### API接口

#### Search

`search?query=<keyword>`

query是字符串，也就是用户在网站上的搜索框输入的字儿

返回一个Array，里面是结果，每个结果包含UP主信息

#### info

`info/<uid>`

id是数字id，直接返回该UP主的信息

#### videos

`videos/<uid>`

id是UP主数字id，返回一个Array里面是这个UP主的所有视频，av号，标题，发布时间戳

#### chart

`chart/<uid>?field=<field>&dataType=<sum|inc|pre>`

* field可以是`FanNum`, `PlayNum`, `ChargeNum`, 也就是这是关注，播放，充电

返回Array，里面有timestamp和那时的数据

#### videoQuality

`videoQuality/<uid>`

Array，最近30个视频

包含质量，发布时间，标题

#### trackedVideos

`trackedVideos/<uid>`

返回一个Array，包含av号和title，也就是有“7天统计”的视频

#### videoInfo

`videoInfo/<uid>/<aid>`

返回视频的基本信息，包括：

* 播放
* 赞
* 评论
* 硬币
* 收藏

#### videoChart

`videoChart/<uid>/<aid>?field=<field>&dataType=<sum|inc>`

field可以是`View` ,`Like`, `Coin`, `Save`, `Comment`, 也就是播放，赞，硬币，收藏，评论

返回Array，里面有timestamp和当时的数据，也就是这个视频在发布后7天的变化

#### rank

`rank/<category>?field=<field>&offset=<offset>&count=<count>&order=<asc|dec>`

* field可以是
* `FanNum`:粉丝总量
* `FanIncIndex`粉丝增长指数
* `ChargesMonthly`本月充电数
* `ViewsWeekly`本周新增播放量
* `WorkIndex` 作品指数
* `SummaryIndex` 总指数
* `AvgQuality`: 平均质量

category可以是 global, advertisement, dance, fashion, game, mic, muisc, technology, animation, digit, fun, life, movie, otomad

返回Array，里面是每个up主的排行