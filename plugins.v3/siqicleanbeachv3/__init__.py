# __init__.py - 思齐炼魔丸（MoviePilot V3）
import asyncio
import json
import math
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
    """思齐站点自动搬砖、清理沙滩并炼造魔丸的 V3 插件。"""

    plugin_name = "思齐炼魔丸"
    plugin_desc = "自动搬砖、清理沙滩并炼造魔丸，支持定时与手动触发。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.3.0"
    plugin_author = "tycgd"
    author_url = "https://github.com/tycgd"
    plugin_config_prefix = "siqicleanbeachv3_"
    plugin_order = 30
    auth_level = 1

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

    # 炼造：单次请求上限与规划约束
    CRAFT_INTERVAL = 0.4
    CRAFT_MAX_QUANTITY = 100
    CRAFT_MAX_PILLS = 10
    CRAFT_MAX_DEPTH = 8
    CRAFT_MAX_STEPS = 200
    CRAFT_RESULT_ITEM = "魔丸"

    _craft_enabled = True
    _craft_target_pills = 1

    # 一次性调度任务的固定 ID，停用时需全部移除
    ONCE_JOB_IDS = (
        "run_once",
        "api_run_once",
        "command_run_once",
        "brick_run_once",
        "craft_run_once"
    )

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
    _craft: Dict[str, Any] = {
        "last_craft": [],
        "pills_total": 0,
        "last_message": "",
        "last_update": None
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
        self._craft_enabled = bool(config.get("craft_enabled", True))
        self._craft_target_pills = min(
            self.CRAFT_MAX_PILLS,
            self.__safe_int(config.get("craft_target_pills"), 1, min_value=1)
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

        self._craft = {
            "last_craft": [],
            "pills_total": 0,
            "last_message": "",
            "last_update": None
        }
        saved_craft = self.get_data("craft")
        if isinstance(saved_craft, dict):
            self._craft["last_craft"] = list(saved_craft.get("last_craft") or [])
            self._craft["pills_total"] = self.__safe_int(
                saved_craft.get("pills_total"), 0
            )
            self._craft["last_message"] = saved_craft.get("last_message") or ""
            self._craft["last_update"] = saved_craft.get("last_update")

        logger.info(
            f"思齐炼魔丸插件初始化完成：enabled={self._enabled}, "
            f"schedule_mode={self._schedule_mode}, interval_minutes={self._interval_minutes}, "
            f"brick_enabled={self._brick_enabled}, brick_max_per_run={self._brick_max_per_run}, "
            f"craft_enabled={self._craft_enabled}, craft_target_pills={self._craft_target_pills}"
        )

        if config.get("run_once"):
            self.update_config({**config, "run_once": False})
            logger.info("收到配置页立即运行请求，提交一次性调度任务")
            scheduler_sdk.add_plugin_once_job(
                self.__class__.__name__,
                "run_once",
                self.run_clean_task,
                "思齐炼魔丸立即运行",
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
        """注册立即执行、手动搬砖、手动炼造与统计数据查询接口。"""
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
                "path": "/craft",
                "endpoint": self.craft_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "手动炼造魔丸",
                "description": "按当前库存提交一次炼造任务，不阻塞请求。"
            },
            {
                "path": "/stats",
                "endpoint": self.get_stats_api,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取清理沙滩统计数据",
                "description": "获取思齐炼魔丸的清理与炼造统计数据。"
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
            "name": "思齐炼魔丸",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "craft_enabled",
                                            "label": "自动炼造",
                                            "hint": "清理结束后按库存自动炼造魔丸"
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
                                            "model": "craft_target_pills",
                                            "label": "目标魔丸数",
                                            "type": "number",
                                            "min": 1,
                                            "max": 10,
                                            "hint": "每次任务的目标魔丸数量（1-10）",
                                            "value": 1
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
            "craft_enabled": self._craft_enabled,
            "craft_target_pills": self._craft_target_pills,
            "run_once": False
        }

    def get_page(self) -> List[dict]:
        """返回详情页：Hero 概览、搬砖进度、统计瓦片、物品清单与执行记录。"""
        inventory = self._inventory or {}
        stats = self._stats or {}

        last_update = stats.get("last_update", "")
        if last_update:
            try:
                last_update = datetime.fromtimestamp(float(last_update)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except Exception:
                last_update = str(last_update)
        else:
            last_update = ""

        filtered_inventory = {
            k: v
            for k, v in inventory.items()
            if k != "砖块" and isinstance(v, dict)
        }

        total_items = len(filtered_inventory)
        total_count = sum(
            max(0, self.__safe_int(item.get("count"), 0))
            for item in filtered_inventory.values()
        )
        has_items = {
            k: v
            for k, v in filtered_inventory.items()
            if max(0, self.__safe_int(v.get("count"), 0)) > 0
        }
        has_items_count = len(has_items)

        bricks = self._bricks or {}
        brick_daily = min(
            self.BRICK_MAX_DAILY, self.__safe_int(bricks.get("daily"), 0)
        )
        brick_pocket = max(0, self.__safe_int(bricks.get("pocket"), 0))

        sorted_items = sorted(
            has_items.items(),
            key=lambda x: max(0, self.__safe_int(x[1].get("count"), 0)),
            reverse=True
        )

        history = list(stats.get("history") or [])[-5:]

        if self._schedule_mode == "interval":
            schedule_info = f"每 {self._interval_minutes} 分钟"
        else:
            schedule_info = f"Cron: {self._cron}"

        return [
            self.__hero_card(
                self.__item_count("魔丸"),
                schedule_info,
                last_update,
                stats.get("last_result", ""),
            ),
            self.__brick_progress_card(brick_daily),
            {
                "component": "VRow",
                "props": {"class": "mb-4"},
                "content": [
                    self.__tile("👝", f"{brick_pocket:,}块", "口袋砖块"),
                    self.__tile(
                        "🥚", f"{self.__item_count('魔丸胚胎'):,}个", "魔丸胚胎"
                    ),
                    self.__tile("🎒", f"{total_items}种", "物品种类"),
                    self.__tile("📦", f"{total_count:,}个", "物品总数"),
                ]
            },
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-4"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center"},
                        "content": [
                            {"component": "span", "text": "🎒 物品清单"},
                            {"component": "VSpacer"},
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "small",
                                    "variant": "tonal",
                                    "color": "deep-purple"
                                },
                                "text": f"{has_items_count} 种"
                            }
                        ]
                    },
                    {
                        "component": "VCardText",
                        "content": (
                            [
                                {
                                    "component": "VAlert",
                                    "props": {"color": "info", "variant": "tonal"},
                                    "text": "🏝️ 暂无物品数据，执行一次任务后即可看到库存。"
                                }
                            ]
                            if not sorted_items
                            else [
                                {
                                    "component": "div",
                                    "props": {
                                        "style": "display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px;"
                                    },
                                    "content": self.__inventory_items(sorted_items)
                                }
                            ]
                        )
                    }
                ]
            },
            {
                "component": "VCard",
                "props": {"variant": "outlined"},
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

    def __hero_card(
        self,
        pill_count: int,
        schedule_info: str,
        last_update: str,
        last_result: str,
    ) -> Dict[str, Any]:
        """生成顶部渐变 Hero 卡片：魔丸库存、状态、定时与上次执行摘要。"""
        status_text = "⚡ 已启用" if self._enabled else "💤 已停用"
        summary = str(last_result or "").strip()[:40] or "暂无执行记录"
        return {
            "component": "VCard",
            "props": {
                "class": "mb-4",
                "style": "background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #ffffff;"
            },
            "content": [
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VRow",
                            "props": {"align": "center"},
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 7},
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {
                                                "style": "font-size: 13px; opacity: 0.85; letter-spacing: 1px;"
                                            },
                                            "text": "魔丸库存"
                                        },
                                        {
                                            "component": "div",
                                            "props": {
                                                "style": "font-size: 42px; font-weight: 700; line-height: 1.1;"
                                            },
                                            "text": f"{max(0, pill_count):,}颗"
                                        },
                                        {
                                            "component": "div",
                                            "props": {"style": "font-size: 14px; opacity: 0.9;"},
                                            "text": "魔丸"
                                        }
                                    ]
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 5},
                                    "content": [
                                        {
                                            "component": "VChip",
                                            "props": {
                                                "color": "white",
                                                "variant": "flat",
                                                "size": "small",
                                                "class": "mr-2 mb-2"
                                            },
                                            "text": status_text
                                        },
                                        {
                                            "component": "VChip",
                                            "props": {
                                                "color": "white",
                                                "variant": "outlined",
                                                "size": "small",
                                                "class": "mb-2"
                                            },
                                            "text": f"⏱ 定时：{schedule_info}"
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "component": "div",
                            "props": {
                                "style": "font-size: 12px; opacity: 0.85; margin-top: 4px;"
                            },
                            "text": f"🕐 上次执行：{last_update or '尚未执行'}　｜　📝 结果：{summary}"
                        }
                    ]
                }
            ]
        }

    def __brick_progress_card(self, daily: int) -> Dict[str, Any]:
        """生成今日搬砖进度卡片，进度条用 div 宽度百分比实现。"""
        total = max(1, self.BRICK_MAX_DAILY)
        daily = min(total, max(0, daily))
        percent = min(100.0, daily / total * 100)
        full = daily >= total
        bar_color = (
            "linear-gradient(90deg, #2e7d32, #66bb6a)"
            if full
            else "linear-gradient(90deg, #667eea, #764ba2)"
        )
        return {
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VRow",
                            "props": {"align": "center", "class": "mb-1"},
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 8},
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {
                                                "style": "font-weight: 600; color: #4527a0;"
                                            },
                                            "text": "🧱 今日搬砖"
                                        }
                                    ]
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 4, "class": "text-right"},
                                    "content": [
                                        {
                                            "component": "span",
                                            "props": {
                                                "style": "font-weight: 700; color: #5e35b1;"
                                            },
                                            "text": f"{daily}/{total}"
                                        },
                                        {
                                            "component": "span",
                                            "props": {
                                                "style": "font-size: 12px; color: "
                                                + ("#2e7d32" if full else "#7e57c2")
                                                + "; margin-left: 6px;"
                                            },
                                            "text": "已满" if full else f"还差 {total - daily} 块"
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "component": "div",
                            "props": {
                                "style": "height: 14px; background: #ede7f6; border-radius: 999px; overflow: hidden;"
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "props": {
                                        "style": f"width: {percent:.1f}%; height: 100%; "
                                        f"background: {bar_color}; border-radius: 999px; "
                                        "transition: width 0.3s ease;"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    @staticmethod
    def __tile(icon: str, value: str, label: str) -> Dict[str, Any]:
        """生成柔和高圆角统计瓦片。"""
        return {
            "component": "VCol",
            "props": {"cols": 6, "md": 3},
            "content": [
                {
                    "component": "div",
                    "props": {
                        "style": "background: #f5f3ff; border-radius: 12px; "
                        "padding: 14px; height: 100%;"
                    },
                    "content": [
                        {
                            "component": "div",
                            "props": {"style": "font-size: 20px;"},
                            "text": icon
                        },
                        {
                            "component": "div",
                            "props": {
                                "style": "font-size: 22px; font-weight: 700; "
                                "color: #5e35b1; line-height: 1.3;"
                            },
                            "text": value
                        },
                        {
                            "component": "div",
                            "props": {"style": "font-size: 12px; color: #7e57c2;"},
                            "text": label
                        }
                    ]
                }
            ]
        }

    def __inventory_items(self, sorted_items: list) -> List[Dict[str, Any]]:
        """生成物品卡片网格：左侧 emoji 圆形图标 + 右下角数量徽标。"""
        items = []
        for name, data in sorted_items:
            icon = self.ITEM_ICONS.get(name, "📦")
            color = self.ITEM_COLORS.get(name, "#3498db")
            count = max(0, self.__safe_int((data or {}).get("count"), 0))
            items.append({
                "component": "div",
                "props": {
                    "style": (
                        "position: relative; display: flex; align-items: center; "
                        "gap: 10px; background: #fafafa; border: 1px solid #eeeeee; "
                        f"border-left: 4px solid {color}; border-radius: 12px; "
                        "padding: 12px 14px; min-height: 76px;"
                    )
                },
                "content": [
                    {
                        "component": "div",
                        "props": {
                            "style": (
                                "width: 40px; height: 40px; flex: 0 0 40px; "
                                "border-radius: 50%; display: flex; "
                                "align-items: center; justify-content: center; "
                                f"font-size: 20px; background: {color}1f;"
                            )
                        },
                        "text": icon
                    },
                    {
                        "component": "div",
                        "props": {"style": "min-width: 0; flex: 1;"},
                        "content": [
                            {
                                "component": "div",
                                "props": {
                                    "style": "font-size: 13px; font-weight: 600; "
                                    "color: #424242; white-space: nowrap; "
                                    "overflow: hidden; text-overflow: ellipsis;"
                                },
                                "text": name
                            },
                            {
                                "component": "div",
                                "props": {"style": "font-size: 12px; color: #9e9e9e;"},
                                "text": "库存"
                            }
                        ]
                    },
                    {
                        "component": "div",
                        "props": {
                            "style": (
                                "position: absolute; right: 8px; bottom: 8px; "
                                f"background: {color}; color: #ffffff; "
                                "border-radius: 999px; padding: 0 8px; "
                                "font-size: 11px; font-weight: 600; line-height: 18px;"
                            )
                        },
                        "text": f"×{count:,}"
                    }
                ]
            })
        return items

    def __item_count(self, name: str) -> int:
        """读取库存中某个物品的数量，兼容字典与整数两种存储形式。"""
        entry = (self._inventory or {}).get(name)
        if isinstance(entry, dict):
            return max(0, self.__safe_int(entry.get("count"), 0))
        return max(0, self.__safe_int(entry, 0))

    def __history_rows(self, history: list) -> List[Dict[str, Any]]:
        """生成执行记录表格行，状态以彩色 VChip 展示。"""
        rows = []
        for record in reversed(history):
            record = record or {}
            time_str = record.get("time", "")
            if isinstance(time_str, (int, float)):
                try:
                    time_str = datetime.fromtimestamp(time_str).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                except Exception:
                    time_str = str(time_str)

            status = str(record.get("status") or "")
            result_text = str(record.get("result") or "-")
            if status == "partial":
                chip_color, chip_text = "warning", "⚠️ 部分成功"
            elif status in ("failed", "error", "auth_failed"):
                chip_color, chip_text = "error", "❌ 失败"
            elif "冷却" in result_text:
                chip_color, chip_text = "success", "🧊 冷却中"
            else:
                chip_color, chip_text = "success", "✅ 成功"

            total_count = max(0, self.__safe_int(record.get("total_count"), 0))
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": time_str},
                    {"component": "td", "text": result_text[:30]},
                    {
                        "component": "td",
                        "text": f"{total_count:,}" if total_count > 0 else "-"
                    },
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "VChip",
                                "props": {
                                    "color": chip_color,
                                    "size": "small",
                                    "variant": "tonal"
                                },
                                "text": chip_text
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
            return {"success": False, "message": "已有清理任务正在执行"}
        logger.info("收到 API 立即执行请求，提交一次性调度任务")
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "api_run_once",
            self.run_clean_task,
            "思齐炼魔丸API立即运行",
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

    async def craft_api(self) -> Dict[str, Any]:
        """API：提交一次炼造任务，不阻塞请求。"""
        if self.__get_lock().locked():
            return {"success": False, "message": "已有任务正在执行"}
        logger.info("收到 API 手动炼造请求，提交一次性调度任务")
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "craft_run_once",
            self.__run_craft_task,
            "思齐手动炼造",
            delay_seconds=1,
        )
        return {"success": True, "message": "炼造任务已提交，完成后会发送通知"}

    async def __run_craft_task(self):
        """一次性任务：手动炼造并记录结果，与清理任务共用并发锁。"""
        lock = self.__get_lock()
        if lock.locked():
            logger.warning("已有任务正在执行，跳过手动炼造")
            return
        async with lock:
            cookie = (self._cookie or await self.__get_site_cookie_async() or "").strip()
            if not cookie or "c_secure_pass=" not in cookie:
                logger.warning("手动炼造失败：缺少包含 c_secure_pass 的思齐 Cookie")
                return

            result = await self.__execute_craft(cookie)
            logger.info(f"手动炼造结束：{self.__craft_summary(result)}")

            self._stats["last_result"] = self.__craft_summary(result)
            try:
                self.save_data("stats", self._stats)
            except Exception as e:
                logger.debug(f"保存统计数据失败：{e}")

            if self._notify:
                crafted = result.get("crafted") or []
                pills = self.__safe_int(result.get("pills"), 0)
                self.post_message(
                    mtype=MessageType.Plugin,
                    title="⚗️【思齐炼魔丸】",
                    text=(
                        f"⚗️ 炼造物品：{len(crafted)} 项\n"
                        f"🧪 本次魔丸：{pills} 颗\n"
                        f"📝 {result.get('message') or '-'}"
                    )
                )

    async def __execute_craft(self, cookie: str) -> Dict[str, Any]:
        """执行炼造、持久化炼造状态，有产出后刷新一次库存。"""
        try:
            result = await self.__craft_items(cookie)
        except Exception as e:
            logger.error(f"炼造异常：{e}")
            result = {
                "success": False,
                "crafted": [],
                "pills": 0,
                "message": str(e)
            }
        self.__save_craft_state(result)
        if result.get("crafted"):
            await self.__fetch_and_update_inventory(cookie)
        return result

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
                    title="🏖️【思齐炼魔丸】",
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

    async def __craft_items(self, cookie: str) -> Dict[str, Any]:
        """抓取库存与配方，规划后逐条执行炼造并返回明细。"""
        result: Dict[str, Any] = {
            "success": False,
            "crafted": [],
            "pills": 0,
            "message": ""
        }
        if not cookie or "c_secure_pass=" not in cookie:
            result["message"] = "缺少包含 c_secure_pass 的思齐 Cookie"
            return result

        try:
            html = await self.__fetch_mowan_html(cookie)
            if not html:
                result["message"] = "获取炼造页面失败"
                return result

            inventory = self.__parse_inventory_html(html)
            if not inventory:
                result["message"] = "解析库存失败，跳过炼造"
                logger.warning(result["message"])
                return result

            recipes = self.__parse_recipes(html)
            if not recipes:
                result["message"] = "解析炼造配方失败，跳过炼造"
                logger.warning(result["message"])
                return result

            plan = self.__plan_craft(inventory, recipes, self._craft_target_pills)
            if not plan:
                result["success"] = True
                result["message"] = "材料不足或无需炼造"
                logger.info(f"炼造规划为空：{result['message']}")
                return result

            by_id = {recipe["recipe_id"]: recipe for recipe in recipes}
            crafted: List[Dict[str, Any]] = []
            pills = 0
            failure = ""

            for index, (recipe_id, quantity) in enumerate(plan):
                if index:
                    await asyncio.sleep(self.CRAFT_INTERVAL)
                recipe = by_id.get(recipe_id) or {}
                payload = await self.__craft_once(cookie, recipe_id, quantity)
                if not payload.get("success"):
                    failure = payload.get("message") or "炼造失败"
                    logger.warning(
                        f"炼造 {recipe.get('result_name') or recipe_id} 失败：{failure}"
                    )
                    break
                produced = quantity * self.__safe_int(
                    recipe.get("result_quantity"), 1
                )
                name = recipe.get("result_name") or str(recipe_id)
                crafted.append({"name": name, "qty": produced})
                if name == self.CRAFT_RESULT_ITEM:
                    pills += produced

            result["crafted"] = crafted
            result["pills"] = pills
            result["success"] = not failure
            if failure:
                result["message"] = failure
            else:
                total = sum(item.get("qty", 0) for item in crafted)
                result["message"] = (
                    f"炼造{len(crafted)}项共{total}件，魔丸{pills}颗"
                )
            logger.info(f"炼造结束：{result['message']}")
            return result
        except httpx2.TimeoutException:
            logger.error("炼造请求超时")
            result["message"] = "请求超时"
            return result
        except httpx2.RequestError as e:
            logger.error(f"炼造请求失败：{e}")
            result["message"] = str(e)
            return result
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"炼造数据解析失败：{e}")
            result["message"] = "返回数据解析失败"
            return result

    async def __craft_once(
        self, cookie: str, recipe_id: int, quantity: int
    ) -> Dict[str, Any]:
        """请求一次 craft_item，返回是否成功与服务端消息。"""
        payload = await self.__game_action(
            cookie,
            "craft_item",
            timeout=15,
            extra={"recipe_id": recipe_id, "quantity": quantity}
        )
        if self.__game_ok(payload):
            return {"success": True, "message": payload.get("message") or ""}
        return {"success": False, "message": payload.get("message") or "炼造失败"}

    @staticmethod
    def __parse_recipes(html: str) -> List[Dict[str, Any]]:
        """从 mowan.php 内联 JS 的 const recipes 中解析配方表。"""
        if not html:
            return []
        match = re.search(
            r"const\s+recipes\s*=\s*(\[.*?\])\s*;", html, re.DOTALL
        )
        if not match:
            return []
        try:
            raw = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"炼造配方 JSON 解析失败：{e}")
            return []
        if not isinstance(raw, list):
            return []

        recipes: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                recipe_id = int(item.get("recipe_id"))
                result_quantity = max(1, int(item.get("result_quantity") or 1))
            except (TypeError, ValueError):
                continue
            result_name = str(item.get("result_name") or "").strip()
            ingredients: List[Dict[str, Any]] = []
            for ingredient in item.get("ingredients") or []:
                if not isinstance(ingredient, dict):
                    continue
                name = str(ingredient.get("name") or "").strip()
                try:
                    quantity = int(ingredient.get("quantity") or 0)
                except (TypeError, ValueError):
                    continue
                if name and quantity > 0:
                    ingredients.append({"name": name, "quantity": quantity})
            if not result_name or not ingredients:
                continue
            recipes.append({
                "recipe_id": recipe_id,
                "result_name": result_name,
                "result_quantity": result_quantity,
                "ingredients": ingredients
            })
        recipes.sort(key=lambda recipe: recipe["recipe_id"])
        return recipes

    @staticmethod
    def __parse_inventory_html(html: str) -> Dict[str, Dict[str, Any]]:
        """解析 mowan.php 页面库存网格，返回包含砖块的完整物品清单。"""
        inventory: Dict[str, Dict[str, Any]] = {}
        if not html:
            return inventory

        grid_match = re.search(
            r'<div[^>]*class="[^"]*inventory-grid[^"]*"[^>]*>', html
        )
        segment = html[grid_match.start():] if grid_match else html
        end_match = re.search(
            r'<div[^>]*class="[^"]*recipe-grid[^"]*"[^>]*>', segment
        )
        if end_match:
            segment = segment[:end_match.start()]

        parts = re.split(
            r'<div[^>]*class="[^"]*inventory-item[^"]*"[^>]*>', segment
        )
        for part in parts[1:]:
            name_match = re.search(
                r'<div[^>]*class="[^"]*item-name[^"]*"[^>]*>([^<]+)</div>', part
            )
            if not name_match:
                continue
            name = name_match.group(1).strip()
            if not name or name in inventory:
                continue
            count_match = re.search(
                r'<div[^>]*class="[^"]*item-count[^"]*"[^>]*>([^<]+)</div>', part
            )
            try:
                count = int(count_match.group(1).strip()) if count_match else 0
            except ValueError:
                count = 0
            icon_match = re.search(
                r'<span[^>]*class="[^"]*item-icon[^"]*"[^>]*>([^<]+)</span>', part
            )
            inventory[name] = {
                "name": name,
                "count": count,
                "icon": icon_match.group(1).strip() if icon_match else "📦"
            }
        return inventory

    def __plan_craft(
        self,
        inventory: Dict[str, Any],
        recipes: List[Dict[str, Any]],
        target_pills: int
    ) -> List[Tuple[int, int]]:
        """需求驱动的递归炼造规划，只补缺口材料并返回配方升序的执行步骤。"""
        sim: Dict[str, int] = {
            name: self.__inventory_value(entry)
            for name, entry in (inventory or {}).items()
        }
        by_result = {recipe["result_name"]: recipe for recipe in recipes}
        plan: List[Tuple[int, int]] = []
        steps = 0

        def craftable(recipe: Dict[str, Any]) -> int:
            caps = [
                sim.get(ingredient["name"], 0) // max(1, ingredient["quantity"])
                for ingredient in recipe["ingredients"]
            ]
            return min(caps) if caps else 0

        def expand(item: str, quantity: int, depth: int) -> None:
            nonlocal steps
            if steps >= self.CRAFT_MAX_STEPS:
                logger.warning(
                    f"炼造规划步数超过上限 {self.CRAFT_MAX_STEPS}，停止展开"
                )
                return
            if depth > self.CRAFT_MAX_DEPTH:
                logger.warning(
                    f"炼造规划递归超过深度 {self.CRAFT_MAX_DEPTH}，停止展开"
                )
                return
            missing = quantity - sim.get(item, 0)
            if missing <= 0:
                return
            recipe = by_result.get(item)
            if recipe is None:
                return

            capacity = craftable(recipe)
            if capacity < missing:
                for ingredient in recipe["ingredients"]:
                    expand(
                        ingredient["name"],
                        ingredient["quantity"] * missing,
                        depth + 1
                    )
                capacity = craftable(recipe)

            result_quantity = max(
                1, self.__safe_int(recipe.get("result_quantity"), 1)
            )
            make = min(
                math.ceil(missing / result_quantity),
                capacity,
                self.CRAFT_MAX_QUANTITY
            )
            if make <= 0:
                return
            for ingredient in recipe["ingredients"]:
                sim[ingredient["name"]] = (
                    sim.get(ingredient["name"], 0)
                    - ingredient["quantity"] * make
                )
            sim[item] = sim.get(item, 0) + make * result_quantity
            plan.append((recipe["recipe_id"], make))
            steps += 1

        target = min(self.CRAFT_MAX_PILLS, max(1, self.__safe_int(target_pills, 1)))
        before_pills = sim.get(self.CRAFT_RESULT_ITEM, 0)
        expand(self.CRAFT_RESULT_ITEM, target, 0)
        produced_pills = sim.get(self.CRAFT_RESULT_ITEM, 0) - before_pills
        if plan and produced_pills <= 0:
            # 只补中间件却炼不出魔丸会白吃砖块，直接放弃本次规划
            logger.warning("库存不足以炼出魔丸，丢弃仅含中间件的炼造规划")
            return []
        return plan

    @staticmethod
    def __inventory_value(entry: Any) -> int:
        """读取库存条目数量，兼容 {'count': n} 与整数两种形式。"""
        if isinstance(entry, dict):
            entry = entry.get("count")
        try:
            return max(0, int(entry or 0))
        except (TypeError, ValueError):
            return 0

    async def __game_action(
        self,
        cookie: str,
        action: str,
        timeout: int = 10,
        extra: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """请求 mowan.php 表单接口并返回解析后的 JSON 字典。"""
        data = {"action": action}
        if extra:
            data.update(extra)
        try:
            request = self.__build_client(cookie, self.__game_headers(), timeout=timeout)
            response = await request.post_res(
                self.MOWAN_URL, data=data, raise_exception=True
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

    def __craft_summary(self, craft_result: Dict[str, Any]) -> str:
        """生成炼造结果的一句话摘要。"""
        if not craft_result:
            return "未执行炼造"
        pills = self.__safe_int(craft_result.get("pills"), 0)
        crafted = craft_result.get("crafted") or []
        total = sum(self.__safe_int(item.get("qty"), 0) for item in crafted)
        if pills > 0:
            return f"炼造魔丸{pills}颗"
        if total > 0:
            return f"炼造{total}件物品"
        return craft_result.get("message") or "未炼造"

    def __craft_suffix(self, craft_result: Dict[str, Any]) -> str:
        """生成追加到 last_result 的炼造摘要，未炼出物品时返回空串。"""
        if not craft_result:
            return ""
        pills = self.__safe_int(craft_result.get("pills"), 0)
        if pills > 0:
            return f"，炼造魔丸{pills}颗"
        crafted = craft_result.get("crafted") or []
        total = sum(self.__safe_int(item.get("qty"), 0) for item in crafted)
        if total > 0:
            return f"，炼造{total}件物品"
        if not craft_result.get("success"):
            return "，炼造失败"
        return ""

    def __save_craft_state(self, craft_result: Dict[str, Any]) -> Dict[str, Any]:
        """合并并持久化炼造状态，可重复调用。"""
        craft = dict(self._craft or {})
        craft.setdefault("last_craft", [])
        craft.setdefault("pills_total", 0)
        craft.setdefault("last_message", "")
        craft.setdefault("last_update", None)
        craft["last_craft"] = list(craft_result.get("crafted") or [])
        craft["pills_total"] = self.__safe_int(craft.get("pills_total"), 0) + max(
            0, self.__safe_int(craft_result.get("pills"), 0)
        )
        craft["last_message"] = craft_result.get("message") or ""
        craft["last_update"] = time.time()
        self._craft = craft
        try:
            self.save_data("craft", craft)
        except Exception as e:
            logger.debug(f"保存炼造状态失败：{e}")
        return craft

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
                title="【思齐炼魔丸】",
                text="已有任务正在执行，请等待当前任务结束。"
            )
            return
        logger.info("收到远程命令立即执行请求，提交一次性调度任务")
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "command_run_once",
            self.run_clean_task,
            "思齐炼魔丸命令立即运行",
            delay_seconds=1,
        )
        self.post_message(
            channel=channel,
            userid=userid,
            mtype=MessageType.Plugin,
            title="【思齐炼魔丸】",
            text="任务已开始，完成后会发送通知。"
        )

    async def run_clean_task(self) -> Dict[str, Any]:
        """执行一次清理任务：搬砖、进沙滩、清理沙滩、刷新物品、炼造并通知。"""
        lock = self.__get_lock()
        if lock.locked():
            return {"status": "running", "message": "已有清理任务正在执行"}

        async with lock:
            try:
                cookie = (self._cookie or await self.__get_site_cookie_async() or "").strip()
                if not cookie or "c_secure_pass=" not in cookie:
                    result = self.__new_result(status="auth_failed", message="缺少包含 c_secure_pass 的思齐 Cookie")
                    self.__finish_task(result)
                    return result

                logger.info("开始执行清理沙滩任务")

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

                craft_result: Dict[str, Any] = {}
                if self._craft_enabled:
                    craft_result = await self.__execute_craft(cookie)
                    if craft_result.get("crafted"):
                        total_count = sum(
                            item.get("count", 0)
                            for item in self._inventory.values()
                        )
                        self._stats["total_items_count"] = total_count
                craft_suffix = self.__craft_suffix(craft_result)

                if collect_success:
                    self._stats["last_result"] = (
                        f"收集了{trash_count}个垃圾，共{total_count}个物品"
                        f"{brick_suffix}{craft_suffix}"
                    )
                elif "冷却" in enter_result.get("message", ""):
                    self._stats["last_result"] = (
                        f"沙滩冷却中，共{total_count}个物品{brick_suffix}{craft_suffix}"
                    )
                else:
                    self._stats["last_result"] = (
                        f"未收集垃圾，共{total_count}个物品{brick_suffix}{craft_suffix}"
                    )

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
                    "bricks": bricks_moved,
                    "craft": craft_result
                }

                self.__finish_task(result)
                return result

            except Exception as e:
                logger.error(f"清理沙滩任务异常：{e}")
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
        html = await self.__fetch_mowan_html(cookie)
        if not html:
            return None

        try:
            inventory = self.__parse_inventory_html(html)
            if "砖块" in inventory:
                brick_entry = inventory.pop("砖块")
                self.__save_brick_state(
                    pocket=self.__safe_int(brick_entry.get("count"), 0)
                )
                logger.info("已过滤砖块")

            logger.info(f"解析到 {len(inventory)} 种物品")
            return inventory
        except Exception as e:
            logger.error(f"获取物品数据异常：{e}")
            return None

    async def __fetch_mowan_html(self, cookie: str) -> Optional[str]:
        """抓取 mowan.php 页面 HTML（包含库存网格与炼造配方）。"""
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            "user-agent": self.UA
        }
        try:
            logger.info("开始获取页面数据...")
            request = self.__build_client(cookie, headers, timeout=15)
            response = await request.get_res(
                self.MOWAN_URL, raise_exception=True
            )
            logger.info(f"获取页面数据响应状态码：{response.status_code}")
            if response.status_code != 200:
                return None
            return response.text
        except httpx2.TimeoutException:
            logger.error("获取页面数据请求超时")
            return None
        except httpx2.RequestError as e:
            logger.error(f"获取页面数据请求失败：{e}")
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
        title = "🏖️【思齐炼魔丸】"
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
            craft = data.get("craft") or {}
            pills = self.__safe_int(craft.get("pills"), 0)
            if pills > 0:
                text += f"\n⚗️ 炼造魔丸：{pills} 颗"

        total_count = sum(item.get("count", 0) for item in self._inventory.values())
        text += f"\n\n📊 物品种类：{len(self._inventory)} 种"
        text += f"\n📦 物品总数：{total_count:,} 个"

        self.post_message(
            mtype=MessageType.Plugin,
            title=title,
            text=text
        )
