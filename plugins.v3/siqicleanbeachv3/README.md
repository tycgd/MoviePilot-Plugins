# 思齐炼魔丸（SiQiCleanBeachV3）

MoviePilot V3 插件：思齐站点自动搬砖、清理沙滩拾取物品、按库存炼造魔丸。

## 功能
- 自动搬砖：每日服务端上限 50 块，可设置单次上限。
- 清理沙滩：自动进场拾取垃圾，更新库存。
- 炼造魔丸：按配方与库存递归规划，可设置目标魔丸数量。
- 通知、定时（interval/cron）与手动触发。

## 使用
1. 安装并启用插件。
2. 配置页确认/填写 `si-qi.xyz` 的 Cookie（留空则沿用站点管理）。
3. 选择调度方式；或点详情页「立即运行」。
4. 远程命令：`/siqi_clean_beach`

## 主要配置
| 配置 | 说明 |
| --- | --- |
| enabled | 启用插件 |
| notify | 发送完成通知 |
| schedule_mode / cron / interval_minutes | 调度方式 |
| brick_enabled / brick_max_per_run | 搬砖开关与单次上限（≤50） |
| craft_enabled / craft_target_pills | 炼造开关与目标魔丸数 |

## 说明
- 运行数据保存在插件数据目录，不写入插件源码目录。
- 站点 Cookie 过期会提示认证失败，更新 Cookie 即可。
