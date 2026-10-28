# __init__.py - 思齐自动收垃圾（MoviePilot V3）
import asyncio
import json
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx2
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.db.oper.site import SiteOper
from app.schemas.types import EventType, MessageType
from app.sdk import scheduler as scheduler_sdk
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.network import AsyncRequestUtils
from app.sdk.plugin import _PluginBase


class SiQiCleanBeachV3(_PluginBase):
    """思齐站点自动清理沙滩并收集垃圾的 V3 插件。"""

    plugin_name = "思齐自动收垃圾"
    plugin_desc = "自动在思齐站点清理沙滩并收集垃圾，支持Cron和间隔定时。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.1.0"
    plugin_author = "bfjy"
    author_url = "https://bfjy2024.github.io/bfjy"
    plugin_config_prefix = "siqicleanbeachv3_"
    plugin_order = 30
    auth_level = 2

    SITE_DOMAIN = "si-qi.xyz"
    BASE_URL = "https://si-qi.xyz"
    MOWAN_URL = f"{BASE_URL}/mowan.php"
    UA = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
    )

    _enabled = False
    _cookie = ""
    _schedule_mode = "interval"
    _cron = "1 */2 * * *"
    _interval_minutes = 121
    _notify = True

    # 搬砖：服务端每日上限写死为 50，单次任务最多搬 50 块
    BRICK_MAX_DAILY = 50
    BRICK_MOVE_INTERVAL = 0.4

    _brick_enabled = True
    _brick_max_per_run = 50

    # 一次性调度任务的固定 ID，停用时需全部移除
    ONCE_JOB_IDS = ("run_once", "api_run_once", "command_run_once", "brick_run_once")

    # 运行态：并发锁按需创建并缓存的单实例对象（宿主当前为单事件循环模型），
    # 状态通过基类 save_data 持久化
    _lock: Optional[asyncio.Lock] = None
    _bricks: Dict[str, Any] = {
        "daily": 0,
        "pocket": 0,
        "remaining": BRICK_MAX_DAILY,
        "last_update": None,
        "next_reset": 0
    }
    _inventory: Dict[str, Any] = {}
    _stats: Dict[str, Any] = {
        "last_update": None,
        "last_result": "",
        "total_items_count": 0,
        "history": []
    }

    ITEM_ICONS = {
        "木材": "🪵",
        "塑料袋": "🛍️",
        "瓶子": "🧴",
        "螺丝": "🔩",
        "旧电池": "🔋",
        "破铜片": "🪙",
        "木工件": "🪚",
        "塑料件": "🪣",
        "简易工具": "🛠️",
        "能量碎片": "⚡",
        "魔丸胚胎": "🥚",
        "魔丸": "⚗️",
        "蚯蚓": "🪱"
    }

    ITEM_COLORS = {
        "木材": "#f39c12",
        "塑料袋": "#3498db",
        "瓶子": "#1abc9c",
        "螺丝": "#95a5a6",
        "旧电池": "#2c3e50",
        "破铜片": "#e67e22",
        "木工件": "#d4a574",
        "塑料件": "#5dade2",
        "简易工具": "#7f8c8d",
        "能量碎片": "#f1c40f",
        "魔丸胚胎": "#d7bde2",
        "魔丸": "#8e44ad",
        "蚯蚓": "#e67e22"
    }

    def init_plugin(self, config: dict = None):
        """读取配置与持久化状态；允许重复调用，不在此处发起阻塞请求。"""
        config = config or {}
        site_cookie = self.__get_site_cookie()

        self._enabled = bool(config.get("enabled", False))
        self._cookie = (config.get("cookie") or site_cookie or "").strip()
        self._schedule_mode = config.get("schedule_mode", "interval")
        self._cron = (config.get("cron") or "1 */2 * * *").strip()
        self._interval_minutes = self.__safe_int(config.get("interval_minutes"), 121, min_value=1)
        self._notify = bool(config.get("notify", True))
        self._brick_enabled = bool(config.get("brick_enabled", True))
        self._brick_max_per_run = min(
            self.BRICK_MAX_DAILY,
            self.__safe_int(
                config.get("brick_max_per_run"), self.BRICK_MAX_DAILY, min_value=1
            )
        )

        self._bricks = {
            "daily": 0,
            "pocket": 0,
            "remaining": self.BRICK_MAX_DAILY,
            "last_update": None,
            "next_reset": 0
        }
        saved_bricks = self.get_data("bricks")
        if isinstance(saved_bricks, dict):
            self._bricks["daily"] = min(
                self.BRICK_MAX_DAILY, self.__safe_int(saved_bricks.get("daily"), 0)
            )
            self._bricks["pocket"] = self.__safe_int(saved_bricks.get("pocket"), 0)
            self._bricks["remaining"] = min(
                self.BRICK_MAX_DAILY,
                max(0, self.__safe_int(
                    saved_bricks.get("remaining"),
                    self.BRICK_MAX_DAILY - self._bricks["daily"]
                ))
            )
            self._bricks["last_update"] = saved_bricks.get("last_update")
            self._bricks["next_reset"] = self.__safe_int(
                saved_bricks.get("next_reset"), 0
            )
            next_reset = self._bricks["next_reset"]
            if next_reset and time.time() >= next_reset:
                self._bricks["daily"] = 0
                self._bricks["remaining"] = self.BRICK_MAX_DAILY

        self._stats = {
            "last_update": None,
            "last_result": "",
            "total_items_count": 0,
            "history": []
        }
        saved_stats = self.get_data("stats")
        if isinstance(saved_stats, dict):
            self._stats.update(saved_stats)
            self._stats["history"] = list(self._stats.get("history") or [])[-20:]

        self._inventory = {}
        saved_inventory = self.get_data("inventory")
        if isinstance(saved_inventory, dict):
            self._inventory = {
                k: v for k, v in saved_inventory.items() if k != "砖块"
            }

        logger.info(
            f"思齐自动收垃圾插件初始化完成：enabled={self._enabled}, "
            f"schedule_mode={self._schedule_mode}, interval_minutes={self._interval_minutes}, "
            f"brick_enabled={self._brick_enabled}, brick_max_per_run={self._brick_max_per_run}"
        )

        if config.get("run_once"):
            self.update_config({**config, "run_once": False})
            logger.info("收到配置页立即运行请求，提交一次性调度任务")
            scheduler_sdk.add_plugin_once_job(
                self.__class__.__name__,
                "run_once",
                self.run_clean_task,
                "思齐自动收垃圾立即运行",
                delay_seconds=3,
            )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/siqi_clean_beach",
                "event": EventType.PluginAction,
                "desc": "立即清理思齐沙滩",
                "category": "站点",
                "data": {
                    "action": "siqi_clean_beach"
                }
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """注册立即执行、手动搬砖与统计数据查询接口。"""
        return [
            {
                "path": "/run",
                "endpoint": self.run_once_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即清理思齐沙滩",
                "description": "按当前插件配置立即执行一次清理沙滩任务。"
            },
            {
                "path": "/bricks",
                "endpoint": self.move_bricks_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "手动搬运砖块",
                "description": "按当前额度提交一次搬砖任务，不阻塞请求。"
            },
            {
                "path": "/stats",
                "endpoint": self.get_stats_api,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取收垃圾统计数据",
                "description": "获取思齐收垃圾的统计数据。"
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """按配置注册间隔或 Cron 周期任务。"""
        if not self._enabled:
            return []

        if self._schedule_mode == "interval":
            try:
                trigger = IntervalTrigger(minutes=self._interval_minutes)
                logger.info(f"使用间隔触发器：每{self._interval_minutes}分钟执行一次")
            except Exception as e:
                logger.warning(f"间隔触发器创建失败：{e}")
                return []
        else:
            if not self._cron:
                return []
            try:
                trigger = CronTrigger.from_crontab(self._cron)
                logger.info(f"使用Cron触发器：{self._cron}")
            except ValueError as e:
                logger.warning(f"Cron配置无效：{e}")
                return []

        return [{
            "id": self.__class__.__name__,
            "name": "思齐自动收垃圾",
            "trigger": trigger,
            "func": self.run_clean_task,
            "kwargs": {}
        }]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        site_cookie = self.__get_site_cookie()
        cookie_value = self._cookie or site_cookie or ""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "notify", "label": "发送通知"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "run_once",
                                            "label": "立即运行一次",
                                            "hint": "保存配置后执行，并自动关闭"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "schedule_mode",
                                            "label": "定时模式",
                                            "items": [
                                                {"title": "Cron表达式", "value": "cron"},
                                                {"title": "固定间隔(分钟)", "value": "interval"},
                                            ],
                                            "hint": "选择定时执行方式"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "Cron表达式",
                                            "placeholder": "5位 Cron 表达式",
                                            "hint": "Cron模式下生效"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "interval_minutes",
                                            "label": "间隔分钟数",
                                            "type": "number",
                                            "min": 1,
                                            "hint": "间隔模式下生效，例如 121",
                                            "value": 121
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "brick_enabled",
                                            "label": "自动搬砖",
                                            "hint": "清理沙滩前自动搬满每日额度"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "brick_max_per_run",
                                            "label": "单次搬砖上限",
                                            "type": "number",
                                            "min": 1,
                                            "max": 50,
                                            "hint": "1-50，服务端每日上限为50块",
                                            "value": 50
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "cookie",
                                            "label": "思齐 Cookie",
                                            "rows": 3,
                                            "placeholder": "填写包含 c_secure_pass 的完整 Cookie",
                                            "hint": "留空时读取站点管理中的思齐 Cookie"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": self._enabled,
            "cookie": cookie_value,
            "schedule_mode": self._schedule_mode,
            "cron": self._cron,
            "interval_minutes": self._interval_minutes,
            "notify": self._notify,
            "brick_enabled": self._brick_enabled,
            "brick_max_per_run": self._brick_max_per_run,
            "run_once": False
        }

    def get_page(self) -> List[dict]:
        """返回详情页：统计卡片、物品清单与历史记录。"""
        inventory = self._inventory
        stats = self._stats
        
        last_update = stats.get("last_update", "")
        if last_update:
            try:
                last_update = datetime.fromtimestamp(last_update).strftime("%Y-%m-%d %H:%M:%S")
            except:
                last_update = str(last_update)

        filtered_inventory = {k: v for k, v in inventory.items() if k != "砖块"}
        
        total_items = len(filtered_inventory)
        total_count = sum(item.get("count", 0) for item in filtered_inventory.values())
        has_items = {k: v for k, v in filtered_inventory.items() if v.get("count", 0) > 0}
        has_items_count = len(has_items)

        bricks = self._bricks or {}
        brick_daily = min(
            self.BRICK_MAX_DAILY, self.__safe_int(bricks.get("daily"), 0)
        )
        brick_pocket = self.__safe_int(bricks.get("pocket"), 0)

        sorted_items = sorted(
            has_items.items(),
            key=lambda x: x[1].get("count", 0),
            reverse=True
        )

        history = stats.get("history", [])[-5:]

        if self._schedule_mode == "interval":
            schedule_info = f"每{self._interval_minutes}分钟"
        else:
            schedule_info = f"Cron: {self._cron}"

        return [
            {
                "component": "VCard",
                "props": {"variant": "tonal", "class": "mb-4"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "text": "🏖️ 思齐收垃圾数据面板"
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "props": {"class": "mb-4"},
                                "content": [
                                    self.__stat_card("📦", "物品总数", f"{total_count:,}", "个", "primary"),
                                    self.__stat_card("📊", "物品种类", f"{total_items}", "种", "info"),
                                    self.__stat_card("✅", "有物品", f"{has_items_count}", "种", "success"),
                                    self.__stat_card("🕐", "最后更新", last_update or "-", "", "warning"),
                                ]
                            },
                            {
                                "component": "VRow",
                                "props": {"class": "mb-4"},
                                "content": [
                                    self.__stat_card(
                                        "🧱", "今日搬砖",
                                        f"{brick_daily}/{self.BRICK_MAX_DAILY}", "", "deep-purple"
                                    ),
                                    self.__stat_card("👝", "口袋砖块", f"{brick_pocket:,}", "块", "secondary"),
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    self.__info_col("⏰ 定时模式", schedule_info),
                                    self.__info_col("📝 最后结果", stats.get("last_result", "-")[:25]),
                                    self.__info_col("🔔 通知", "开启" if self._notify else "关闭"),
                                    self.__info_col("🍪 Cookie", "已配置" if self._cookie else "未配置"),
                                ]
                            }
                        ]
                    }
                ]
            },
            {
                "component": "VCard",
                "props": {"variant": "tonal", "class": "mb-4"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "text": "🎯 物品清单"
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {
                                                    "style": "display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 16px;"
                                                },
                                                "content": self.__inventory_items(sorted_items) if sorted_items else [
                                                    {
                                                        "component": "div",
                                                        "props": {"class": "text-center text-medium-emphasis pa-4", "style": "grid-column: 1 / -1; color: #999;"},
                                                        "text": "🏝️ 暂无物品数据"
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            {
                "component": "VCard",
                "props": {"variant": "tonal"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "text": "📋 执行记录"
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VTable",
                                "props": {"hover": True, "dense": True},
                                "content": [
                                    {
                                        "component": "thead",
                                        "content": [
                                            {
                                                "component": "tr",
                                                "content": [
                                                    {"component": "th", "text": "时间"},
                                                    {"component": "th", "text": "结果"},
                                                    {"component": "th", "text": "物品总数"},
                                                    {"component": "th", "text": "状态"},
                                                ]
                                            }
                                        ]
                                    },
                                    {
                                        "component": "tbody",
                                        "content": self.__history_rows(history) if history else [
                                            {
                                                "component": "tr",
                                                "content": [
                                                    {
                                                        "component": "td",
                                                        "props": {"colspan": 4, "class": "text-center text-medium-emphasis"},
                                                        "text": "暂无执行记录"
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

    def stop_service(self):
        """移除全部一次性调度任务并释放并发锁，允许重复调用。"""
        for job_id in self.ONCE_JOB_IDS:
            try:
                scheduler_sdk.remove_plugin_once_job(
                    self.__class__.__name__, job_id
                )
            except Exception as e:
                logger.debug(f"移除一次性任务 {job_id} 失败：{e}")
        self._lock = None

    def __inventory_items(self, sorted_items: list) -> List[Dict[str, Any]]:
        """生成物品卡片网格 - 优化文字颜色对比度"""
        items = []
        for name, data in sorted_items:
            icon = self.ITEM_ICONS.get(name, "📦")
            color = self.ITEM_COLORS.get(name, "#3498db")
            count = data.get("count", 0)
            
            # 根据颜色亮度决定文字颜色
            # 对于亮色背景使用深色文字，暗色背景使用亮色文字
            dark_colors = ["#2c3e50", "#7f8c8d", "#8e44ad", "#d4a574"]
            text_color = "#ffffff" if color in dark_colors else "#2c3e50"
            
            items.append({
                "component": "div",
                "props": {
                    "style": f"""
                        background: linear-gradient(145deg, {color}25, {color}08);
                        border-radius: 16px;
                        padding: 20px 16px;
                        text-align: center;
                        border: 1px solid {color}30;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                        transition: transform 0.2s, box-shadow 0.2s;
                    """
                },
                "content": [
                    {
                        "component": "div",
                        "props": {"style": f"font-size: 40px; margin-bottom: 6px; display: block;"},
                        "text": icon
                    },
                    {
                        "component": "div",
                        "props": {"style": f"font-size: 14px; font-weight: 600; color: {text_color}; margin-bottom: 4px; letter-spacing: 0.3px;"},
                        "text": name
                    },
                    {
                        "component": "div",
                        "props": {"style": f"font-size: 24px; font-weight: bold; color: {color}; text-shadow: 0 1px 4px rgba(0,0,0,0.1);"},
                        "text": f"{count:,}"
                    },
                    {
                        "component": "div",
                        "props": {
                            "style": f"width: 100%; height: 4px; background: {color}20; border-radius: 4px; margin-top: 10px; overflow: hidden;"
                        },
                        "content": [
                            {
                                "component": "div",
                                "props": {
                                    "style": f"width: {min(100, count / 10)}%; height: 100%; background: linear-gradient(90deg, {color}, {color}80); border-radius: 4px;"
                                }
                            }
                        ]
                    }
                ]
            })
        return items

    @staticmethod
    def __stat_card(icon: str, label: str, value: str, suffix: str, color: str) -> Dict[str, Any]:
        return {
            "component": "VCol",
            "props": {"cols": 6, "md": 3},
            "content": [
                {
                    "component": "VCard",
                    "props": {
                        "color": color,
                        "variant": "tonal",
                        "class": "pa-3"
                    },
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "d-flex align-center"},
                            "content": [
                                {
                                    "component": "div",
                                    "props": {"class": "text-h4 mr-2"},
                                    "text": icon
                                },
                                {
                                    "component": "div",
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {"class": "text-caption text-medium-emphasis"},
                                            "text": label
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "text-h6 font-weight-bold"},
                                            "text": f"{value}{suffix}"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    @staticmethod
    def __info_col(label: str, value: Any) -> Dict[str, Any]:
        return {
            "component": "VCol",
            "props": {"cols": 6, "md": 3},
            "content": [
                {
                    "component": "div",
                    "props": {"class": "text-caption text-medium-emphasis"},
                    "text": label
                },
                {
                    "component": "div",
                    "props": {"class": "text-h6"},
                    "text": str(value or "-")
                }
            ]
        }

    def __history_rows(self, history: list) -> List[Dict[str, Any]]:
        rows = []
        for record in reversed(history):
            time_str = record.get("time", "")
            if isinstance(time_str, (int, float)):
                try:
                    time_str = datetime.fromtimestamp(time_str).strftime("%Y-%m-%d %H:%M:%S")
                except:
                    time_str = str(time_str)
            
            status_color = "success" if record.get("status") == "completed" else "warning"
            status_text = "✅ 成功" if record.get("status") == "completed" else "⚠️ 失败"
            result_text = record.get("result", "-")
            total_count = record.get("total_count", 0)
            
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": time_str},
                    {"component": "td", "text": result_text[:30]},
                    {"component": "td", "text": f"{total_count:,}" if total_count > 0 else "-"},
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "VChip",
                                "props": {
                                    "color": status_color,
                                    "size": "small",
                                    "variant": "tonal"
                                },
                                "text": status_text
                            }
                        ]
                    }
                ]
            })
        return rows

    def __get_lock(self) -> asyncio.Lock:
        """返回按需创建并缓存的单实例并发锁（宿主当前为单事件循环模型）。"""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def run_once_api(self) -> Dict[str, Any]:
        """API：提交一次清理任务，不阻塞请求。"""
        if self.__get_lock().locked():
            return {"success": False, "message": "已有收垃圾任务正在执行"}
        logger.info("收到 API 立即执行请求，提交一次性调度任务")
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "api_run_once",
            self.run_clean_task,
            "思齐自动收垃圾API立即运行",
            delay_seconds=1,
        )
        return {"success": True, "message": "任务已开始，完成后会发送通知"}

    async def get_stats_api(self) -> Dict[str, Any]:
        """API：返回统计与物品数据。"""
        return {
            "success": True,
            "data": {
                "stats": self._stats,
                "inventory": self._inventory
            }
        }

    async def move_bricks_api(self) -> Dict[str, Any]:
        """API：提交一次搬砖任务，不阻塞请求，返回当前搬砖状态。"""
        if self.__get_lock().locked():
            return self.__brick_result(success=False, message="已有任务正在执行")
        logger.info("收到 API 手动搬砖请求，提交一次性调度任务")
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "brick_run_once",
            self.__run_brick_task,
            "思齐手动搬砖",
            delay_seconds=1,
        )
        return self.__brick_result(
            success=True, message="搬砖任务已提交，完成后会发送通知"
        )

    async def __run_brick_task(self):
        """一次性任务：手动搬砖并记录结果，与清理任务共用并发锁。"""
        lock = self.__get_lock()
        if lock.locked():
            logger.warning("已有任务正在执行，跳过手动搬砖")
            return
        async with lock:
            cookie = (self._cookie or await self.__get_site_cookie_async() or "").strip()
            if not cookie or "c_secure_pass=" not in cookie:
                logger.warning("手动搬砖失败：缺少包含 c_secure_pass 的思齐 Cookie")
                return

            result = await self.__move_bricks(cookie)
            moved = self.__safe_int(result.get("moved"), 0)
            daily = self.__safe_int(result.get("daily_bricks"), 0)
            logger.info(f"手动搬砖结束：moved={moved}, daily={daily}")

            self._stats["last_result"] = (
                f"手动搬砖{moved}块，今日{daily}/{self.BRICK_MAX_DAILY}"
            )
            try:
                self.save_data("stats", self._stats)
            except Exception as e:
                logger.debug(f"保存统计数据失败：{e}")

            if self._notify:
                self.post_message(
                    mtype=MessageType.Plugin,
                    title="🏖️【思齐自动收垃圾插件】",
                    text=(
                        f"🧱 手动搬砖：{moved} 块\n"
                        f"📅 今日累计：{daily}/{self.BRICK_MAX_DAILY}\n"
                        f"📝 {result.get('message') or '-'}"
                    )
                )

    async def __move_bricks(self, cookie: str) -> Dict[str, Any]:
        """同步游戏状态后按剩余额度逐块搬砖，返回本次搬砖明细。"""
        if not cookie or "c_secure_pass=" not in cookie:
            return self.__brick_result(
                success=False, message="缺少包含 c_secure_pass 的思齐 Cookie"
            )

        state = await self.__sync_brick_state(cookie)
        if not state.get("success"):
            return self.__brick_result(
                success=False, message=state.get("message") or "同步游戏状态失败"
            )

        daily_bricks = self.__safe_int(state.get("daily_bricks"), 0)
        next_reset = self.__safe_int(state.get("next_brick_reset_ts"), 0)
        remaining = max(0, self.BRICK_MAX_DAILY - daily_bricks)
        self.__save_brick_state(
            daily=daily_bricks, remaining=remaining, next_reset=next_reset
        )

        if remaining <= 0:
            logger.info("今日搬砖已达上限，跳过搬砖")
            return self.__brick_result(
                success=True,
                message=f"今日搬砖已达上限（{self.BRICK_MAX_DAILY}块）",
                daily=daily_bricks,
                remaining=0
            )

        limit = min(
            self.BRICK_MAX_DAILY,
            max(1, self.__safe_int(self._brick_max_per_run, self.BRICK_MAX_DAILY))
        )
        target = min(remaining, limit)
        moved = 0
        pocket: Optional[int] = None
        failure = ""

        for index in range(target):
            if index:
                await asyncio.sleep(self.BRICK_MOVE_INTERVAL)
            move = await self.__move_brick_once(cookie)
            if move.get("success"):
                moved += 1
                daily_bricks = self.__safe_int(
                    move.get("daily_bricks"), daily_bricks + 1
                )
                if move.get("pocket") is not None:
                    pocket = self.__safe_int(move.get("pocket"), 0)
                continue
            failure = move.get("message") or "搬砖失败"
            logger.warning(f"第{moved + 1}次搬砖失败：{failure}")
            break

        remaining = max(0, self.BRICK_MAX_DAILY - daily_bricks)
        self.__save_brick_state(daily=daily_bricks, remaining=remaining, pocket=pocket)

        if failure:
            message = failure
        else:
            message = f"本次搬砖{moved}块，今日累计{daily_bricks}/{self.BRICK_MAX_DAILY}"
        logger.info(f"搬砖结束：{message}")
        return self.__brick_result(
            success=not failure,
            message=message,
            moved=moved,
            daily=daily_bricks,
            remaining=remaining
        )

    async def __sync_brick_state(self, cookie: str) -> Dict[str, Any]:
        """读取服务端每日搬砖计数与下次重置时间。"""
        payload = await self.__game_action(cookie, "sync_game_state")
        if not self.__game_ok(payload):
            return {
                "success": False,
                "message": payload.get("message") or "同步游戏状态失败"
            }
        return {
            "success": True,
            "daily_bricks": self.__safe_int(payload.get("daily_bricks"), 0),
            "next_brick_reset_ts": self.__safe_int(
                payload.get("next_brick_reset_ts"), 0
            )
        }

    async def __move_brick_once(self, cookie: str) -> Dict[str, Any]:
        """搬运一块砖，返回是否成功、服务端累计与口袋数量。"""
        payload = await self.__game_action(cookie, "move_brick")
        if not self.__game_ok(payload):
            return {
                "success": False,
                "message": payload.get("message") or "搬砖失败"
            }
        return {
            "success": True,
            "message": payload.get("message") or "",
            "daily_bricks": payload.get("daily_bricks"),
            "pocket": payload.get("pocket")
        }

    async def __game_action(
        self, cookie: str, action: str, timeout: int = 10
    ) -> Dict[str, Any]:
        """请求 mowan.php 表单接口并返回解析后的 JSON 字典。"""
        try:
            request = self.__build_client(cookie, self.__game_headers(), timeout=timeout)
            response = await request.post_res(
                self.MOWAN_URL, data={"action": action}, raise_exception=True
            )
            if response.status_code != 200:
                return {"success": False, "message": f"HTTP {response.status_code}"}
            payload = response.json()
            if not isinstance(payload, dict):
                return {"success": False, "message": "返回数据格式异常"}
            return payload
        except httpx2.TimeoutException:
            logger.error(f"{action} 请求超时")
            return {"success": False, "message": "请求超时"}
        except httpx2.RequestError as e:
            logger.error(f"{action} 请求失败：{e}")
            return {"success": False, "message": str(e)}
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"{action} 返回非JSON响应：{e}")
            return {"success": False, "message": "返回非JSON响应"}
        except Exception as e:
            logger.error(f"{action} 请求异常：{e}")
            return {"success": False, "message": str(e)}

    @staticmethod
    def __game_ok(payload: Dict[str, Any]) -> bool:
        """判断游戏接口响应是否表示成功。"""
        if not isinstance(payload, dict):
            return False
        return bool(payload.get("success")) or payload.get("status") == "success"

    def __game_headers(self) -> Dict[str, str]:
        """构造游戏表单接口使用的请求头。"""
        return {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "content-type": "application/x-www-form-urlencoded",
            "origin": self.BASE_URL,
            "referer": self.MOWAN_URL,
            "user-agent": self.UA
        }

    def __brick_result(
        self,
        success: bool,
        message: str,
        moved: int = 0,
        daily: Optional[int] = None,
        remaining: Optional[int] = None
    ) -> Dict[str, Any]:
        """构造搬砖结果结构，缺省值取自当前持久化状态。"""
        bricks = self._bricks or {}
        daily_value = (
            self.__safe_int(bricks.get("daily"), 0) if daily is None else daily
        )
        daily_value = max(0, min(self.BRICK_MAX_DAILY, daily_value))
        remaining_value = (
            max(0, self.BRICK_MAX_DAILY - daily_value)
            if remaining is None else remaining
        )
        return {
            "success": success,
            "moved": moved,
            "daily_bricks": daily_value,
            "remaining": max(0, remaining_value),
            "message": message
        }

    def __brick_suffix(self, brick_result: Dict[str, Any]) -> str:
        """生成追加到 last_result 的搬砖摘要，未执行时返回空串。"""
        if not brick_result:
            return ""
        moved = self.__safe_int(brick_result.get("moved"), 0)
        if moved > 0:
            return f"，搬砖{moved}块"
        if not brick_result.get("success"):
            return "，搬砖失败"
        return ""

    def __save_brick_state(
        self,
        daily: Optional[int] = None,
        pocket: Optional[int] = None,
        remaining: Optional[int] = None,
        next_reset: Optional[int] = None
    ) -> Dict[str, Any]:
        """合并并持久化搬砖状态，允许部分字段更新。"""
        bricks = dict(self._bricks or {})
        bricks.setdefault("daily", 0)
        bricks.setdefault("pocket", 0)
        bricks.setdefault("next_reset", 0)
        if daily is not None:
            bricks["daily"] = max(
                0, min(self.BRICK_MAX_DAILY, self.__safe_int(daily, 0))
            )
        if pocket is not None:
            bricks["pocket"] = max(0, self.__safe_int(pocket, 0))
        if next_reset is not None:
            bricks["next_reset"] = max(0, self.__safe_int(next_reset, 0))
        if remaining is None:
            remaining = self.BRICK_MAX_DAILY - self.__safe_int(bricks.get("daily"), 0)
        bricks["remaining"] = max(0, self.__safe_int(remaining, 0))
        bricks["last_update"] = time.time()
        self._bricks = bricks
        try:
            self.save_data("bricks", bricks)
        except Exception as e:
            logger.debug(f"保存搬砖状态失败：{e}")
        return bricks

    @eventmanager.register(EventType.PluginAction)
    async def run_once_command(self, event: Event = None):
        """远程命令入口：只处理本插件的 action。"""
        event_data = event.event_data if event else {}
        if not event_data or event_data.get("action") != "siqi_clean_beach":
            return
        channel = event_data.get("channel")
        userid = event_data.get("user")
        if self.__get_lock().locked():
            self.post_message(
                channel=channel,
                userid=userid,
                mtype=MessageType.Plugin,
                title="【思齐自动收垃圾插件】",
                text="已有收垃圾任务正在执行，请等待当前任务结束。"
            )
            return
        logger.info("收到远程命令立即执行请求，提交一次性调度任务")
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "command_run_once",
            self.run_clean_task,
            "思齐自动收垃圾命令立即运行",
            delay_seconds=1,
        )
        self.post_message(
            channel=channel,
            userid=userid,
            mtype=MessageType.Plugin,
            title="【思齐自动收垃圾插件】",
            text="任务已开始，完成后会发送通知。"
        )

    async def run_clean_task(self) -> Dict[str, Any]:
        """执行一次清理任务：搬砖、进沙滩、收垃圾、刷新物品并通知。"""
        lock = self.__get_lock()
        if lock.locked():
            return {"status": "running", "message": "已有收垃圾任务正在执行"}

        async with lock:
            try:
                cookie = (self._cookie or await self.__get_site_cookie_async() or "").strip()
                if not cookie or "c_secure_pass=" not in cookie:
                    result = self.__new_result(status="auth_failed", message="缺少包含 c_secure_pass 的思齐 Cookie")
                    self.__finish_task(result)
                    return result

                logger.info("开始执行收垃圾任务")

                # 搬砖失败不影响后续清理流程，仅记录摘要
                bricks_moved = 0
                brick_result: Dict[str, Any] = {}
                if self._brick_enabled:
                    try:
                        brick_result = await self.__move_bricks(cookie)
                    except Exception as e:
                        logger.error(f"搬砖异常：{e}")
                        brick_result = {"success": False, "moved": 0, "message": str(e)}
                    bricks_moved = self.__safe_int(brick_result.get("moved"), 0)
                    if brick_result.get("success"):
                        logger.info(f"搬砖完成：{brick_result.get('message')}")
                    else:
                        logger.warning(f"搬砖未完成：{brick_result.get('message')}")
                brick_suffix = self.__brick_suffix(brick_result)

                enter_result = await self.__enter_beach(cookie)
                collect_success = False
                trash_count = 0

                if enter_result.get("success"):
                    logger.info("成功进入沙滩")

                    collect_result = await self.__collect_trash(cookie)
                    if collect_result.get("success"):
                        trash_count = collect_result.get("trash", 0)
                        collect_success = True
                        logger.info(f"收集垃圾成功：{trash_count} 个")
                    else:
                        logger.warning(f"收集垃圾失败：{collect_result.get('message')}")
                else:
                    msg = enter_result.get("message", "")
                    logger.warning(f"进入沙滩失败：{msg}")
                    if "冷却" in msg:
                        self._stats["last_result"] = "沙滩冷却中，当前物品待刷新"
                    else:
                        result = self.__new_result(status="failed", message=f"进入沙滩失败：{msg}")
                        result["data"] = {"bricks": bricks_moved}
                        await self.__fetch_and_update_inventory(cookie)
                        self.__finish_task(result)
                        return result

                inventory_data = await self.__fetch_inventory(cookie)
                if inventory_data:
                    filtered_data = {k: v for k, v in inventory_data.items() if k != "砖块"}
                    self._inventory = filtered_data
                    total_count = sum(item.get("count", 0) for item in filtered_data.values())
                    logger.info(f"更新物品数据：{len(filtered_data)} 种物品，共 {total_count} 个")
                else:
                    total_count = sum(item.get("count", 0) for item in self._inventory.values())
                    logger.warning(f"获取物品数据失败，使用缓存数据：共 {total_count} 个")

                self._stats["last_update"] = time.time()
                self._stats["total_items_count"] = total_count

                if collect_success:
                    self._stats["last_result"] = f"收集了{trash_count}个垃圾，共{total_count}个物品{brick_suffix}"
                elif "冷却" in enter_result.get("message", ""):
                    self._stats["last_result"] = f"沙滩冷却中，共{total_count}个物品{brick_suffix}"
                else:
                    self._stats["last_result"] = f"未收集垃圾，共{total_count}个物品{brick_suffix}"

                history = self._stats.get("history", [])
                if collect_success:
                    result_text = f"收集{trash_count}个垃圾"
                    status = "completed"
                elif "冷却" in enter_result.get("message", ""):
                    result_text = "沙滩冷却中"
                    status = "completed"
                else:
                    result_text = enter_result.get("message", "未收集垃圾")
                    status = "failed"

                history.append({
                    "time": time.time(),
                    "result": result_text,
                    "total_count": total_count,
                    "status": status
                })
                if len(history) > 20:
                    history = history[-20:]
                self._stats["history"] = history

                if collect_success:
                    result = self.__new_result(
                        status="completed",
                        message=f"成功清理沙滩，收集了{trash_count}个垃圾，当前共{total_count}个物品"
                    )
                elif "冷却" in enter_result.get("message", ""):
                    result = self.__new_result(
                        status="completed",
                        message=f"沙滩冷却中，当前共{total_count}个物品"
                    )
                else:
                    result = self.__new_result(
                        status="partial",
                        message=f"未能收集垃圾，当前共{total_count}个物品"
                    )
                result["data"] = {
                    "trash": trash_count,
                    "total_count": total_count,
                    "bricks": bricks_moved
                }

                self.__finish_task(result)
                return result

            except Exception as e:
                logger.error(f"收垃圾任务异常：{e}")
                import traceback
                logger.error(traceback.format_exc())
                try:
                    cookie = (self._cookie or await self.__get_site_cookie_async() or "").strip()
                    if cookie and "c_secure_pass=" in cookie:
                        await self.__fetch_and_update_inventory(cookie)
                except Exception:
                    pass
                result = self.__new_result(status="error", message=str(e))
                self.__finish_task(result)
                return result

    async def __fetch_and_update_inventory(self, cookie: str):
        """异常兜底：尽力刷新一次物品数据。"""
        try:
            inventory_data = await self.__fetch_inventory(cookie)
            if inventory_data:
                filtered_data = {k: v for k, v in inventory_data.items() if k != "砖块"}
                self._inventory = filtered_data
                total_count = sum(item.get("count", 0) for item in filtered_data.values())
                self._stats["total_items_count"] = total_count
                self._stats["last_update"] = time.time()
                logger.info(f"物品数据已更新：{len(filtered_data)} 种，共 {total_count} 个")
            else:
                logger.warning("获取物品数据失败")
        except Exception as e:
            logger.error(f"获取物品数据异常：{e}")

    async def __enter_beach(self, cookie: str) -> Dict[str, Any]:
        """请求进入沙滩，返回是否成功与失败原因。"""
        try:
            headers = {
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                "content-type": "application/x-www-form-urlencoded",
                "origin": self.BASE_URL,
                "referer": f"{self.MOWAN_URL}",
                "user-agent": self.UA
            }

            data = {"action": "enter_beach"}

            logger.info("发送进入沙滩请求...")
            request = self.__build_client(cookie, headers, timeout=10)
            response = await request.post_res(
                self.MOWAN_URL, data=data, raise_exception=True
            )

            logger.info(f"进入沙滩响应状态码：{response.status_code}")

            if response.status_code != 200:
                return {"success": False, "message": f"HTTP {response.status_code}"}

            try:
                result = response.json()
                logger.info(f"进入沙滩响应：{result}")
                if result.get("success") or result.get("status") == "success":
                    return {"success": True}
                return {"success": False, "message": result.get("message", "未知错误")}
            except (json.JSONDecodeError, ValueError):
                html = response.text
                if "登录" in html or "cookie" in html.lower():
                    return {"success": False, "message": "Cookie失效，请重新登录"}
                if "成功" in html:
                    return {"success": True}
                return {"success": False, "message": "返回非JSON响应"}

        except httpx2.TimeoutException:
            logger.error("进入沙滩请求超时")
            return {"success": False, "message": "请求超时"}
        except httpx2.RequestError as e:
            logger.error(f"进入沙滩请求失败：{e}")
            return {"success": False, "message": str(e)}
        except Exception as e:
            logger.error(f"进入沙滩异常：{e}")
            return {"success": False, "message": str(e)}

    async def __collect_trash(self, cookie: str) -> Dict[str, Any]:
        """请求一键收集垃圾，返回是否成功与收集数量。"""
        try:
            headers = {
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                "content-type": "application/x-www-form-urlencoded",
                "origin": self.BASE_URL,
                "referer": f"{self.MOWAN_URL}",
                "user-agent": self.UA
            }

            data = {"action": "collect_all_trash"}

            logger.info("发送一键收集请求...")
            request = self.__build_client(cookie, headers, timeout=15)
            response = await request.post_res(
                self.MOWAN_URL, data=data, raise_exception=True
            )

            logger.info(f"收集垃圾响应状态码：{response.status_code}")

            if response.status_code != 200:
                return {"success": False, "message": f"HTTP {response.status_code}"}

            try:
                result = response.json()
                logger.info(f"收集垃圾响应：{result}")
                trash_count = 0
                if isinstance(result, dict):
                    if "data" in result and isinstance(result["data"], dict):
                        trash_count = result["data"].get("collected", 0)
                    elif "collected" in result:
                        trash_count = result.get("collected", 0)

                    if trash_count == 0 and "message" in result:
                        msg = str(result["message"])
                        numbers = re.findall(r'\d+', msg)
                        if numbers:
                            trash_count = int(numbers[0])

                if result.get("success") or result.get("status") == "success" or trash_count > 0:
                    return {"success": True, "trash": trash_count}
                return {"success": False, "message": result.get("message", "未知错误")}
            except (json.JSONDecodeError, ValueError):
                html = response.text
                if "登录" in html or "cookie" in html.lower():
                    return {"success": False, "message": "Cookie失效，请重新登录"}

                trash_match = re.search(r'收集了?\s*(\d+)\s*个?垃圾', html)
                if trash_match:
                    return {"success": True, "trash": int(trash_match.group(1))}

                if "成功" in html:
                    return {"success": True, "trash": 0}
                return {"success": False, "message": "返回非JSON响应"}

        except httpx2.TimeoutException:
            logger.error("收集垃圾请求超时")
            return {"success": False, "message": "请求超时"}
        except httpx2.RequestError as e:
            logger.error(f"收集垃圾请求失败：{e}")
            return {"success": False, "message": str(e)}
        except Exception as e:
            logger.error(f"收集垃圾异常：{e}")
            return {"success": False, "message": str(e)}

    async def __fetch_inventory(self, cookie: str) -> Optional[Dict[str, Any]]:
        """抓取并解析物品清单页面，剔除砖块。"""
        try:
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                "cache-control": "max-age=0",
                "user-agent": self.UA
            }

            logger.info("开始获取物品数据...")
            request = self.__build_client(cookie, headers, timeout=15)
            response = await request.get_res(
                self.MOWAN_URL, raise_exception=True
            )

            logger.info(f"获取物品数据响应状态码：{response.status_code}")

            if response.status_code != 200:
                return None

            html = response.text
            inventory = {}

            item_pattern = r'<div[^>]*class="[^"]*inventory-item[^"]*"[^>]*>(.*?)</div>\s*(?:<div|</div>|$)'
            item_blocks = re.findall(item_pattern, html, re.DOTALL)

            for item_block in item_blocks:
                name_match = re.search(r'<div[^>]*class="[^"]*item-name[^"]*"[^>]*>([^<]+)</div>', item_block)
                if not name_match:
                    continue
                name = name_match.group(1).strip()
                
                count_match = re.search(r'<div[^>]*class="[^"]*item-count[^"]*"[^>]*>([^<]+)</div>', item_block)
                if not count_match:
                    continue
                try:
                    count = int(count_match.group(1).strip())
                except ValueError:
                    count = 0
                
                icon_match = re.search(r'<span[^>]*class="[^"]*item-icon[^"]*"[^>]*>([^<]+)</span>', item_block)
                icon = icon_match.group(1).strip() if icon_match else "📦"
                
                if name and name not in inventory:
                    inventory[name] = {
                        "name": name,
                        "count": count,
                        "icon": icon
                    }

            if len(inventory) == 0:
                grid_match = re.search(r'<div[^>]*class="[^"]*inventory-grid[^"]*"[^>]*>(.*?)</div>\s*</div>', html, re.DOTALL)
                if grid_match:
                    grid_html = grid_match.group(1)
                    parts = re.split(r'<div[^>]*class="[^"]*inventory-item[^"]*"[^>]*>', grid_html)
                    for part in parts[1:]:
                        name_match = re.search(r'<div[^>]*class="[^"]*item-name[^"]*"[^>]*>([^<]+)</div>', part)
                        if not name_match:
                            continue
                        name = name_match.group(1).strip()
                        
                        count_match = re.search(r'<div[^>]*class="[^"]*item-count[^"]*"[^>]*>([^<]+)</div>', part)
                        count = int(count_match.group(1).strip()) if count_match else 0
                        
                        icon_match = re.search(r'<span[^>]*class="[^"]*item-icon[^"]*"[^>]*>([^<]+)</span>', part)
                        icon = icon_match.group(1).strip() if icon_match else "📦"
                        
                        if name and name not in inventory:
                            inventory[name] = {
                                "name": name,
                                "count": count,
                                "icon": icon
                            }

            if "砖块" in inventory:
                brick_entry = inventory.pop("砖块")
                self.__save_brick_state(
                    pocket=self.__safe_int(brick_entry.get("count"), 0)
                )
                logger.info("已过滤砖块")

            logger.info(f"解析到 {len(inventory)} 种物品")
            return inventory

        except httpx2.TimeoutException:
            logger.error("获取物品数据请求超时")
            return None
        except httpx2.RequestError as e:
            logger.error(f"获取物品数据请求失败：{e}")
            return None
        except Exception as e:
            logger.error(f"获取物品数据异常：{e}")
            return None

    def __build_client(
        self, cookie: str, headers: Dict[str, str], timeout: int
    ) -> AsyncRequestUtils:
        """构造携带站点 Cookie 的异步请求客户端（默认不校验证书）。"""
        return AsyncRequestUtils(
            headers=headers,
            cookies=self.__cookie_to_dict(cookie),
            timeout=timeout,
        )

    def __get_site_cookie(self) -> str:
        """同步读取思齐站点 Cookie（仅用于初始化与页面渲染）。"""
        site_cookie = ""
        try:
            site = SiteOper().get_by_domain(self.SITE_DOMAIN)
            if site and site.cookie:
                site_cookie = site.cookie
        except Exception as e:
            logger.error(f"获取站点 Cookie 异常：{e}")
        return site_cookie

    async def __get_site_cookie_async(self) -> str:
        """异步读取思齐站点 Cookie，避免在事件循环里阻塞。"""
        try:
            site = await SiteOper().async_get_by_domain(self.SITE_DOMAIN)
            if site and site.cookie:
                return site.cookie
        except Exception as e:
            logger.error(f"获取站点 Cookie 异常：{e}")
        return ""

    @staticmethod
    def __cookie_to_dict(cookie: str) -> Dict[str, str]:
        """把 Cookie 字符串解析为字典。"""
        cookies = {}
        for item in (cookie or "").split(";"):
            if "=" not in item:
                continue
            key, value = item.split("=", 1)
            key = key.strip()
            if key:
                cookies[key] = value.strip()
        return cookies

    @staticmethod
    def __safe_int(value: Any, default: int, min_value: int = None) -> int:
        """把任意输入转换为不小于 min_value 的整数。"""
        try:
            result = int(value) if value is not None else default
        except (ValueError, TypeError):
            result = default
        if min_value is not None:
            result = max(result, min_value)
        return result

    @staticmethod
    def __new_result(status: str = "running", message: str = "") -> Dict[str, Any]:
        """构造统一的任务结果结构。"""
        return {
            "status": status,
            "message": message,
            "timestamp": time.time()
        }

    def __finish_task(self, result: Dict[str, Any]):
        """任务收尾：按配置通知并持久化状态。"""
        if self._notify:
            self.__send_notification(result)
        self._update_stats()

    def _update_stats(self):
        """把统计与物品数据写入插件数据存储，不污染插件配置。"""
        try:
            self.save_data("stats", self._stats)
            self.save_data("inventory", self._inventory)
        except Exception as e:
            logger.error(f"更新统计数据失败：{e}")

    def __send_notification(self, result: Dict[str, Any]):
        title = "🏖️【思齐自动收垃圾插件】"
        status_map = {
            "completed": "✅ 成功",
            "partial": "⚠️ 部分成功",
            "failed": "❌ 失败",
            "auth_failed": "🔐 认证失败",
            "error": "💥 异常"
        }
        status_text = status_map.get(result.get("status"), result.get("status", "未知"))
        text = f"状态：{status_text}\n消息：{result.get('message')}"

        if result.get("data"):
            data = result["data"]
            if data.get("trash"):
                text += f"\n\n🗑️ 收集垃圾：{data['trash']} 个"
            if data.get("total_count"):
                text += f"\n📦 当前物品总数：{data['total_count']:,} 个"
            if data.get("bricks"):
                text += f"\n\n🧱 搬砖：{data['bricks']} 块"

        total_count = sum(item.get("count", 0) for item in self._inventory.values())
        text += f"\n\n📊 物品种类：{len(self._inventory)} 种"
        text += f"\n📦 物品总数：{total_count:,} 个"

        self.post_message(
            mtype=MessageType.Plugin,
            title=title,
            text=text
        )
