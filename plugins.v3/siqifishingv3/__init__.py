"""思齐钓鱼 V3 插件：自动抛竿收竿、鱼饵/鱼竿策略、补饵预算、任务交付与竞猜。"""

import asyncio
import json
import random
import time
from datetime import datetime, timedelta, timezone
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


class SiQiFishingV3(_PluginBase):
    """思齐站点钓鱼小游戏的 V3 自动化插件。"""

    plugin_name = "思齐钓鱼"
    plugin_desc = "自动抛竿收竿（可调完美/随机/自定义），鱼饵鱼竿策略、补饵预算、任务交付与竞猜均可自由配置。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.1.0"
    plugin_author = "tycgd"
    author_url = "https://github.com/tycgd"
    plugin_config_prefix = "siqifishingv3_"
    plugin_order = 31
    auth_level = 1

    SITE_DOMAIN = "si-qi.xyz"
    BASE_URL = "https://si-qi.xyz"
    FISHING_URL = f"{BASE_URL}/siqi_fishing.php"
    UA = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
    )
    TZ_CN = timezone(timedelta(hours=8))

    FISHING_DATA_MARKER = "const FISHING_DATA = "
    DAILY_HISTORY_LIMIT = 30

    # 滑轨甜蜜区：页面 CSS 中 40%-60% 为深蓝色，50% 颜色最深
    PERFECT_MIN = 0.45
    PERFECT_MAX = 0.55

    MAX_CASTS_PER_RUN = 40
    MAX_DAILY_CASTS = 99
    MAX_CAST_INTERVAL = 60
    MAX_INTERVAL_MINUTES = 1440

    LURE_META: Dict[str, Dict[str, Any]] = {
        "normal": {"name": "蚯蚓鱼饵", "price": 300, "score": 0, "icon": "🪱"},
        "camouflage": {"name": "伪装鱼饵", "price": 1000, "score": 100, "icon": "🪶"},
        "red_worm": {"name": "红虫鱼饵", "price": 2000, "score": 50, "icon": "🐛"},
        "bubble": {"name": "泡囊鱼饵", "price": 3000, "score": 10, "icon": "🫧"},
        "insect": {"name": "昆虫鱼饵", "price": 4000, "score": 60, "icon": "🦗"},
        "yellow_fish": {"name": "黄鱼鱼饵", "price": 5000, "score": 20, "icon": "🐠"},
    }

    ROD_META: Dict[str, Dict[str, Any]] = {
        "mushroom": {"name": "蘑菇鱼竿", "score": 0, "icon": "🍄"},
        "green_leaf": {"name": "绿叶鱼竿", "score": 60, "icon": "🍃"},
        "float": {"name": "浮漂鱼竿", "score": 20, "icon": "🎣"},
        "fishling": {"name": "鱼仔鱼竿", "score": 45, "icon": "🐟"},
        "love": {"name": "爱心鱼竿", "score": 56, "icon": "💗"},
        "star": {"name": "星星鱼竿", "score": 60, "icon": "⭐"},
    }

    RARITY_LABELS = {
        "common": "普通",
        "uncommon": "不常见",
        "rare": "稀有",
        "epic": "史诗",
        "legendary": "传说",
    }

    # 页面快捷操作：单次最多 3 竿，且间隔压缩到 3 秒内，保证同步接口快速返回
    MAX_QUICK_CASTS = 3
    QUICK_CAST_INTERVAL = 3

    # 详情页允许直接修改的选项；其余配置只能在配置页调整
    OPTION_WHITELIST: Dict[str, Dict[str, Any]] = {
        "slider_mode": {
            "type": "enum",
            "label": "收竿方式",
            "values": ("perfect", "range", "custom"),
        },
        "lure_mode": {"type": "enum", "label": "鱼饵策略", "values": ("auto", "fixed")},
        "lure_key": {"type": "enum", "label": "固定鱼饵", "values": tuple(LURE_META)},
        "rod_mode": {"type": "enum", "label": "鱼竿策略", "values": ("auto", "fixed")},
        "rod_key": {"type": "enum", "label": "固定鱼竿", "values": tuple(ROD_META)},
        "lure_fixed": {"type": "lure_fixed", "label": "固定鱼饵"},
        "rod_fixed": {"type": "rod_fixed", "label": "固定鱼竿"},
        "auto_buy": {"type": "bool", "label": "自动补饵"},
        "dry_run": {"type": "bool", "label": "演练模式"},
        "auto_task": {"type": "bool", "label": "自动交付任务"},
        "auto_guess": {"type": "bool", "label": "自动竞猜"},
        "slot_spread": {"type": "bool", "label": "时段分散"},
        "buy_budget_per_day": {
            "type": "int",
            "label": "每日购饵预算",
            "min": 0,
            "max": 1_000_000,
        },
        "keep_lures": {"type": "int", "label": "鱼饵保底库存", "min": 0, "max": 999},
        "daily_cast_limit": {
            "type": "int",
            "label": "每日抛竿上限",
            "min": 1,
            "max": 99,
        },
    }

    # 敏感或需要精调、不提供页面入口的键
    OPTION_BLOCKED = (
        "cookie",
        "cron",
        "schedule_mode",
        "interval_minutes",
        "enabled",
        "notify",
        "run_once",
        "slider_center",
        "slider_jitter",
        "guess_value",
        "per_slot_casts",
        "max_casts_per_run",
        "cast_interval",
        "buy_lure_key",
    )

    _enabled = False
    _cookie = ""
    _schedule_mode = "interval"
    _cron = "7 */2 * * *"
    _interval_minutes = 61
    _notify = True

    _dry_run = False
    _max_casts_per_run = 6
    _daily_cast_limit = 33
    _cast_interval = 3

    _lure_mode = "auto"
    _lure_key = "normal"
    _auto_buy = False
    _buy_lure_key = "normal"
    _buy_budget_per_day = 0
    _keep_lures = 20

    _rod_mode = "auto"
    _rod_key = "mushroom"

    _slider_mode = "perfect"
    _slider_center = 0.5
    _slider_jitter = 0.02

    _auto_task = True
    _auto_guess = False
    _guess_value = 0

    _slot_spread = False
    _per_slot_casts = 8

    ONCE_JOB_IDS = ("run_once", "api_run_once", "command_run_once")

    _lock: Optional[asyncio.Lock] = None
    _daily: Dict[str, Any] = {
        "date": "",
        "casts": 0,
        "catches": 0,
        "new_discoveries": 0,
        "misses": 0,
        "miss_rewards": 0,
        "spent": 0,
        "slots": {},
        "last_result": "",
        "history": [],
    }
    _state: Dict[str, Any] = {
        "updated_at": None,
        "attempts_used": 0,
        "attempts_limit": 0,
        "extra_attempts": 0,
        "catches": 0,
        "magic": 0,
        "selected_lure": "",
        "selected_rod": "",
        "lures": {},
        "rods": {},
        "codex_total": 0,
        "codex_unlocked": 0,
        "quiz_status": "",
        "quiz_question": "",
        "quiz_options": [],
        "guess_status": "",
        "guess_reward": 0,
        "tasks": {},
        "tasks_ready": 0,
        "last_message": "",
    }

    def init_plugin(self, config: dict = None):
        """读取配置与持久化状态；允许重复调用，不在此处发起网络请求。"""
        config = config or {}
        site_cookie = self.__get_site_cookie()

        self._enabled = bool(config.get("enabled", False))
        self._cookie = (config.get("cookie") or site_cookie or "").strip()
        self._schedule_mode = config.get("schedule_mode", "interval")
        self._cron = (config.get("cron") or "7 */2 * * *").strip()
        self._interval_minutes = min(
            self.MAX_INTERVAL_MINUTES,
            self.__safe_int(config.get("interval_minutes"), 61, min_value=1),
        )
        self._notify = bool(config.get("notify", True))
        self._dry_run = bool(config.get("dry_run", False))

        self._max_casts_per_run = min(
            self.MAX_CASTS_PER_RUN,
            self.__safe_int(config.get("max_casts_per_run"), 6, min_value=1),
        )
        self._daily_cast_limit = min(
            self.MAX_DAILY_CASTS,
            self.__safe_int(config.get("daily_cast_limit"), 33, min_value=1),
        )
        self._cast_interval = min(
            self.MAX_CAST_INTERVAL,
            self.__safe_int(config.get("cast_interval"), 3, min_value=0),
        )

        self._lure_mode = config.get("lure_mode", "auto")
        self._lure_key = config.get("lure_key") or "normal"
        self._auto_buy = bool(config.get("auto_buy", False))
        self._buy_lure_key = config.get("buy_lure_key") or "normal"
        self._buy_budget_per_day = max(
            0, self.__safe_int(config.get("buy_budget_per_day"), 0)
        )
        self._keep_lures = max(0, self.__safe_int(config.get("keep_lures"), 20))

        self._rod_mode = config.get("rod_mode", "auto")
        self._rod_key = config.get("rod_key") or "mushroom"

        self._slider_mode = config.get("slider_mode", "perfect")
        self._slider_center = self.__safe_float(
            config.get("slider_center"), 0.5, 0.0, 1.0
        )
        self._slider_jitter = self.__safe_float(
            config.get("slider_jitter"), 0.02, 0.0, 0.1
        )

        self._auto_task = bool(config.get("auto_task", True))
        self._auto_guess = bool(config.get("auto_guess", False))
        self._guess_value = max(0, self.__safe_int(config.get("guess_value"), 0))

        self._slot_spread = bool(config.get("slot_spread", False))
        self._per_slot_casts = max(
            1, self.__safe_int(config.get("per_slot_casts"), 8, min_value=1)
        )

        saved_daily = self.get_data("daily") or {}
        self._daily = {
            "date": saved_daily.get("date") or "",
            "casts": self.__safe_int(saved_daily.get("casts"), 0),
            "catches": self.__safe_int(saved_daily.get("catches"), 0),
            "new_discoveries": self.__safe_int(
                saved_daily.get("new_discoveries"), 0
            ),
            "misses": self.__safe_int(saved_daily.get("misses"), 0),
            "miss_rewards": self.__safe_int(saved_daily.get("miss_rewards"), 0),
            "spent": self.__safe_int(saved_daily.get("spent"), 0),
            "slots": dict(saved_daily.get("slots") or {}),
            "last_result": saved_daily.get("last_result") or "",
            "history": list(saved_daily.get("history") or [])[-self.DAILY_HISTORY_LIMIT:],
        }
        self._daily = self.__roll_daily(
            self._daily, datetime.now(self.TZ_CN).strftime("%Y-%m-%d")
        )

        saved_state = self.get_data("state") or {}
        if isinstance(saved_state, dict):
            for key, value in saved_state.items():
                if key in self._state:
                    self._state[key] = value

        if config.get("run_once"):
            try:
                self.update_config({**config, "run_once": False})
                scheduler_sdk.add_plugin_once_job(
                    self.__class__.__name__,
                    "run_once",
                    self.run_fishing_task,
                    "思齐钓鱼立即运行",
                    delay_seconds=1,
                )
            except Exception as e:
                logger.warning(f"注册立即运行任务失败：{e}")

        logger.info(
            f"思齐钓鱼：{'已启用' if self._enabled else '未启用'}，"
            f"每日上限 {self._daily_cast_limit} 次，单次 {self._max_casts_per_run} 次"
        )

    def get_state(self) -> bool:
        return bool(self._enabled)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/siqi_fishing",
                "event": EventType.PluginAction,
                "desc": "立即执行一次思齐钓鱼",
                "category": "站点",
                "data": {"action": "siqi_fishing"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/run",
                "endpoint": self.run_once_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即执行一次钓鱼任务",
                "description": "提交后台任务，完成后发送通知",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "查询钓鱼状态",
                "description": "返回今日次数、鱼饵库存与最近记录",
            },
            {
                "path": "/fish_now",
                "endpoint": self.api_fish_now,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "页面快捷抛竿",
                "description": "同步钓 1~3 竿，不补饵、不竞猜、不交付任务",
            },
            {
                "path": "/sync",
                "endpoint": self.api_sync,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "仅同步状态",
                "description": "只抓取页面并刷新运行态，不调用任何 action 接口",
            },
            {
                "path": "/set_option",
                "endpoint": self.api_set_option,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "页面修改白名单选项",
                "description": "仅支持白名单内的键，其余配置请进配置页",
            },
            {
                "path": "/deliver",
                "endpoint": self.api_deliver,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "交付可完成任务",
                "description": "刷新状态后仅交付 progress.ready 的任务",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []

        if self._schedule_mode == "interval":
            try:
                trigger = IntervalTrigger(minutes=self._interval_minutes)
                logger.info(f"思齐钓鱼：使用间隔触发器，每 {self._interval_minutes} 分钟")
            except Exception as e:
                logger.warning(f"思齐钓鱼：间隔触发器创建失败：{e}")
                return []
        else:
            if not self._cron:
                return []
            try:
                trigger = CronTrigger.from_crontab(self._cron)
                logger.info(f"思齐钓鱼：使用 Cron 触发器 {self._cron}")
            except ValueError as e:
                logger.warning(f"思齐钓鱼：Cron 配置无效：{e}")
                return []

        return [
            {
                "id": self.__class__.__name__,
                "name": self.plugin_name,
                "trigger": trigger,
                "func": self.run_fishing_task,
                "kwargs": {},
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        site_cookie = self.__get_site_cookie()
        cookie_value = self._cookie or site_cookie or ""
        lure_items = [
            {"title": f"{meta['name']}（{meta['price']}魔力）", "value": key}
            for key, meta in self.LURE_META.items()
        ]
        rod_items = [
            {"title": meta["name"], "value": key}
            for key, meta in self.ROD_META.items()
        ]
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VSwitch",
                                    "props": {"model": "enabled", "label": "启用插件"},
                                },
                            ),
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VSwitch",
                                    "props": {"model": "notify", "label": "发送通知"},
                                },
                            ),
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "run_once",
                                        "label": "立即运行一次",
                                        "hint": "保存配置后执行，并自动关闭",
                                    },
                                },
                            ),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                12,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cookie",
                                        "label": "站点 Cookie",
                                        "placeholder": "留空则自动读取站点管理中 si-qi.xyz 的 Cookie",
                                        "value": cookie_value,
                                    },
                                },
                            ),
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "class": "mb-3",
                            "text": "钓鱼会消耗鱼饵与每日次数；滑轨甜蜜区为 40%-60%（50% 为最深处）。全部选项仅影响自动化行为，随时可改。",
                        },
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VSelect",
                                    "props": {
                                        "model": "schedule_mode",
                                        "label": "定时模式",
                                        "items": [
                                            {"title": "Cron表达式", "value": "cron"},
                                            {"title": "固定间隔(分钟)", "value": "interval"},
                                        ],
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VCronField",
                                    "props": {
                                        "model": "cron",
                                        "label": "Cron表达式",
                                        "placeholder": "7 */2 * * *",
                                        "hint": "Cron 模式下生效",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "interval_minutes",
                                        "label": "间隔分钟数",
                                        "type": "number",
                                        "min": 1,
                                        "value": 61,
                                        "hint": "间隔模式下生效",
                                    },
                                },
                            ),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "max_casts_per_run",
                                        "label": "单次最多抛竿",
                                        "type": "number",
                                        "min": 1,
                                        "max": 40,
                                        "hint": "每次任务最多抛几次",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "daily_cast_limit",
                                        "label": "每日抛竿上限",
                                        "type": "number",
                                        "min": 1,
                                        "max": 99,
                                        "hint": "留余地可改为 20 等",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cast_interval",
                                        "label": "抛竿间隔(秒)",
                                        "type": "number",
                                        "min": 0,
                                        "max": 60,
                                        "hint": "两次抛竿之间的等待",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "dry_run",
                                        "label": "演练模式",
                                        "hint": "只同步状态，不抛竿",
                                    },
                                },
                            ),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSelect",
                                    "props": {
                                        "model": "lure_mode",
                                        "label": "鱼饵策略",
                                        "items": [
                                            {"title": "自动选最好的库存", "value": "auto"},
                                            {"title": "固定鱼饵", "value": "fixed"},
                                        ],
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSelect",
                                    "props": {
                                        "model": "lure_key",
                                        "label": "固定鱼饵",
                                        "items": lure_items,
                                        "hint": "策略为固定时生效",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSelect",
                                    "props": {
                                        "model": "rod_mode",
                                        "label": "鱼竿策略",
                                        "items": [
                                            {"title": "自动选已解锁最强", "value": "auto"},
                                            {"title": "固定鱼竿", "value": "fixed"},
                                        ],
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSelect",
                                    "props": {
                                        "model": "rod_key",
                                        "label": "固定鱼竿",
                                        "items": rod_items,
                                        "hint": "策略为固定时生效",
                                    },
                                },
                            ),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "auto_buy",
                                        "label": "自动补饵",
                                        "hint": "库存低于保底值时购买",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSelect",
                                    "props": {
                                        "model": "buy_lure_key",
                                        "label": "购买鱼饵",
                                        "items": lure_items,
                                        "hint": "固定鱼饵策略下改为补买所选固定饵",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "buy_budget_per_day",
                                        "label": "每日购饵预算",
                                        "type": "number",
                                        "min": 0,
                                        "hint": "魔力值，0 表示不花钱",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "keep_lures",
                                        "label": "鱼饵保底库存",
                                        "type": "number",
                                        "min": 0,
                                        "hint": "低于该值才补货",
                                    },
                                },
                            ),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VSelect",
                                    "props": {
                                        "model": "slider_mode",
                                        "label": "收竿方式",
                                        "items": [
                                            {"title": "固定完美 0.50", "value": "perfect"},
                                            {"title": "甜蜜区内随机", "value": "range"},
                                            {"title": "自定义数值", "value": "custom"},
                                        ],
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "slider_center",
                                        "label": "自定义收竿点",
                                        "type": "number",
                                        "min": 0,
                                        "max": 1,
                                        "step": 0.01,
                                        "hint": "0~1，自定义模式下生效",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                4,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "slider_jitter",
                                        "label": "随机抖动幅度",
                                        "type": "number",
                                        "min": 0,
                                        "max": 0.1,
                                        "step": 0.01,
                                        "hint": "完美/自定义模式下生效；甜蜜区随机固定取 45%~55%",
                                    },
                                },
                            ),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "auto_task",
                                        "label": "自动交付任务",
                                        "hint": "满足条件时自动交付解锁鱼竿",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "auto_guess",
                                        "label": "自动今日竞猜",
                                        "hint": "仅首次下竿前有效",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "guess_value",
                                        "label": "竞猜预测条数",
                                        "type": "number",
                                        "min": 0,
                                        "hint": "猜中可加次数",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "slot_spread",
                                        "label": "按时段分散抛竿",
                                        "hint": "避免单一时段用光次数",
                                    },
                                },
                            ),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "per_slot_casts",
                                        "label": "每时段上限",
                                        "type": "number",
                                        "min": 1,
                                        "hint": "分散模式下生效，GMT+8 每 4 小时一段",
                                    },
                                },
                            ),
                        ],
                    },
                ],
            }
        ], {
            "enabled": self._enabled,
            "notify": self._notify,
            "cookie": cookie_value,
            "schedule_mode": self._schedule_mode,
            "cron": self._cron,
            "interval_minutes": self._interval_minutes,
            "max_casts_per_run": self._max_casts_per_run,
            "daily_cast_limit": self._daily_cast_limit,
            "cast_interval": self._cast_interval,
            "dry_run": self._dry_run,
            "lure_mode": self._lure_mode,
            "lure_key": self._lure_key,
            "rod_mode": self._rod_mode,
            "rod_key": self._rod_key,
            "auto_buy": self._auto_buy,
            "buy_lure_key": self._buy_lure_key,
            "buy_budget_per_day": self._buy_budget_per_day,
            "keep_lures": self._keep_lures,
            "slider_mode": self._slider_mode,
            "slider_center": self._slider_center,
            "slider_jitter": self._slider_jitter,
            "auto_task": self._auto_task,
            "auto_guess": self._auto_guess,
            "guess_value": self._guess_value,
            "slot_spread": self._slot_spread,
            "per_slot_casts": self._per_slot_casts,
            "run_once": False,
        }

    def get_page(self) -> List[dict]:
        """详情页：Hero 概览、次数进度、统计瓦片、快捷操作、策略调节、鱼饵库存、记录与配置摘要。"""
        state = self._state or {}
        daily = self._daily or {}

        updated_at = state.get("updated_at") or ""
        if updated_at:
            try:
                updated_at = datetime.fromtimestamp(float(updated_at)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except Exception:
                updated_at = str(updated_at)

        attempts_used = max(0, self.__safe_int(state.get("attempts_used"), 0))
        attempts_limit = max(0, self.__safe_int(state.get("attempts_limit"), 0))
        catches = max(0, self.__safe_int(state.get("catches"), 0))
        lures = state.get("lures") or {}
        selected_lure = state.get("selected_lure") or ""

        lure_total = sum(
            max(0, self.__safe_int(v, 0))
            for v in lures.values()
            if isinstance(v, (int, float))
        )
        codex_total = max(0, self.__safe_int(state.get("codex_total"), 0))
        codex_unlocked = max(0, self.__safe_int(state.get("codex_unlocked"), 0))

        if self._schedule_mode == "interval":
            schedule_info = f"每 {self._interval_minutes} 分钟"
        else:
            schedule_info = f"Cron: {self._cron}"

        slider_info = {
            "perfect": "完美 0.50（±抖动）",
            "range": "甜蜜区 45%~55% 随机",
            "custom": f"自定义 {self._slider_center:.2f}（±抖动）",
        }.get(self._slider_mode, self._slider_mode)

        lure_info = (
            f"自动（当前 {self.LURE_META.get(selected_lure, {}).get('name', selected_lure or '无')}）"
            if self._lure_mode == "auto"
            else f"固定 {self.LURE_META.get(self._lure_key, {}).get('name', self._lure_key)}"
        )
        rod_info = (
            "自动选最强已解锁"
            if self._rod_mode == "auto"
            else f"固定 {self.ROD_META.get(self._rod_key, {}).get('name', self._rod_key)}"
        )
        budget_info = (
            f"自动补饵，每日预算 {self._buy_budget_per_day} 魔力"
            if self._auto_buy
            else "不自动购买鱼饵"
        )

        return [
            self.__hero_card(
                catches,
                f"{attempts_used}/{attempts_limit or '?'}",
                schedule_info,
                updated_at,
                daily.get("last_result", "") or state.get("last_message", ""),
            ),
            self.__attempt_progress_card(attempts_used, attempts_limit),
            {
                "component": "VRow",
                "props": {"class": "mb-4"},
                "content": [
                    self.__tile("🎣", f"{catches:,}条", "今日渔获"),
                    self.__tile("🪱", f"{lure_total:,}枚", "鱼饵库存"),
                    self.__tile(
                        "📖", f"{codex_unlocked}/{codex_total or '?'}", "图鉴解锁"
                    ),
                    self.__tile(
                        "💰",
                        f"{self.__safe_int(state.get('magic'), 0):,}",
                        "魔力值",
                    ),
                ],
            },
            {
                "component": "VRow",
                "props": {"class": "mb-4"},
                "content": [
                    self.__tile("🔁", f"{self._max_casts_per_run}次", "单次抛竿"),
                    self.__tile(
                        "🎯",
                        f"{max(0, attempts_limit - attempts_used)}次",
                        "今日剩余",
                    ),
                    self.__tile(
                        "✨", f"{self.__safe_int(daily.get('new_discoveries'), 0)}种", "今日新发现"
                    ),
                    self.__tile(
                        "💸", f"{self.__safe_int(daily.get('spent'), 0)}", "今日购饵花费"
                    ),
                ],
            },
            self.__quick_actions_card(),
            self.__strategy_tuning_card(),
            self.__lure_table(lures, selected_lure),
            self.__log_table(daily.get("history") or []),
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-4"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center"},
                        "content": [
                            {"component": "span", "text": "⚙️ 当前策略"},
                            {"component": "VSpacer"},
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "small",
                                    "variant": "tonal",
                                    "color": "primary",
                                },
                                "text": "数值类选项请进配置页修改",
                            },
                        ],
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            self.__strategy_row("收竿方式", slider_info),
                            self.__strategy_row("鱼饵策略", lure_info),
                            self.__strategy_row("鱼竿策略", rod_info),
                            self.__strategy_row("购饵", budget_info),
                            self.__strategy_row(
                                "任务交付",
                                "自动交付可完成任务" if self._auto_task else "不自动交付",
                            ),
                            self.__strategy_row(
                                "竞猜",
                                f"自动竞猜 {self._guess_value} 条"
                                if self._auto_guess
                                else "不自动竞猜",
                            ),
                            self.__strategy_row(
                                "时段分散",
                                f"每时段最多 {self._per_slot_casts} 次"
                                if self._slot_spread
                                else "不限制时段",
                            ),
                            self.__strategy_row(
                                "演练模式", "开启（只同步不抛竿）" if self._dry_run else "关闭"
                            ),
                        ],
                    },
                ],
            },
            self.__quiz_card(state),
        ]

    def stop_service(self):
        """移除全部一次性任务并释放并发锁，可重复安全调用。"""
        for job_id in self.ONCE_JOB_IDS:
            try:
                scheduler_sdk.remove_plugin_once_job(self.__class__.__name__, job_id)
            except Exception as e:
                logger.debug(f"移除一次性任务 {job_id} 失败：{e}")
        self._lock = None

    @staticmethod
    def __form_col(cols: int, md: int, component: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "component": "VCol",
            "props": {"cols": cols, "md": md},
            "content": [component],
        }

    @staticmethod
    def __tile(icon: str, value: str, label: str) -> Dict[str, Any]:
        return {
            "component": "VCol",
            "props": {"cols": 6, "sm": 3},
            "content": [
                {
                    "component": "div",
                    "props": {
                        "style": (
                            "border: 1px solid rgba(148, 163, 184, 0.35);"
                            " border-radius: 14px; padding: 14px 16px;"
                            " background: rgba(148, 163, 184, 0.08); height: 100%;"
                        )
                    },
                    "content": [
                        {
                            "component": "div",
                            "props": {
                                "style": "font-size: 13px; opacity: 0.75; margin-bottom: 6px;"
                            },
                            "text": f"{icon} {label}",
                        },
                        {
                            "component": "div",
                            "props": {
                                "style": "font-size: 22px; font-weight: 700; line-height: 1.2;"
                            },
                            "text": value,
                        },
                    ],
                }
            ],
        }

    def __api_path(self, path: str) -> str:
        """拼出详情页事件可调用的插件 API 相对路径。

        宿主前端使用自带 JWT 的 axios 实例调用，无需在 URL 上附带 API Token。
        """
        return f"plugin/{self.__class__.__name__}/{path}"

    def __btn(
        self,
        text: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        method: str = "post",
        color: str = "primary",
        variant: str = "tonal",
        size: str = "small",
        disabled: bool = False,
    ) -> Dict[str, Any]:
        """构造带点击事件的 VBtn；点击后由宿主调用插件 API 并自动刷新页面。"""
        return {
            "component": "VBtn",
            "props": {
                "size": size,
                "variant": variant,
                "color": color,
                "disabled": bool(disabled),
                "class": "text-none",
            },
            "events": {
                "click": {
                    "api": self.__api_path(path),
                    "method": method,
                    "params": dict(params or {}),
                }
            },
            "text": text,
        }

    def __option_row(
        self, label: str, value_text: str, buttons: List[dict]
    ) -> Dict[str, Any]:
        """构造一行「名称 + 当前值 + 按钮组」的选项行。"""
        return {
            "component": "div",
            "props": {
                "style": (
                    "display:flex; align-items:center; gap:10px; flex-wrap:wrap;"
                    " padding:8px 0; border-bottom:1px dashed rgba(148,163,184,0.25);"
                    " font-size:13px;"
                )
            },
            "content": [
                {
                    "component": "div",
                    "props": {
                        "style": (
                            "display:flex; align-items:center; gap:8px;"
                            " flex:1 1 160px; min-width:0;"
                        )
                    },
                    "content": [
                        {
                            "component": "span",
                            "props": {"style": "opacity:0.75;"},
                            "text": label,
                        },
                        {
                            "component": "VChip",
                            "props": {
                                "size": "x-small",
                                "variant": "tonal",
                                "color": "primary",
                            },
                            "text": value_text,
                        },
                    ],
                },
                {
                    "component": "div",
                    "props": {
                        "style": (
                            "display:flex; align-items:center; gap:6px; flex-wrap:wrap;"
                            " justify-content:flex-end;"
                        )
                    },
                    "content": buttons,
                },
            ],
        }

    def __toggle_row(
        self,
        label: str,
        key: str,
        enabled: bool,
        *,
        on_text: str,
        off_text: str,
        on_color: str = "warning",
        off_color: str = "success",
    ) -> Dict[str, Any]:
        """构造开关行：展示当前状态，按钮用于切到相反状态。"""
        return self.__option_row(
            label,
            "开启" if enabled else "关闭",
            [
                self.__btn(
                    off_text if enabled else on_text,
                    "set_option",
                    {"key": key, "value": not enabled},
                    color=off_color if enabled else on_color,
                )
            ],
        )

    def __running_chip(self) -> Optional[Dict[str, Any]]:
        """锁被占用时返回「任务执行中」徽标，空闲返回 None。"""
        if not self.__get_lock().locked():
            return None
        return {
            "component": "VChip",
            "props": {
                "size": "small",
                "variant": "tonal",
                "color": "warning",
                "class": "ml-2",
            },
            "text": "⚙️ 任务执行中",
        }

    def __quick_actions_card(self) -> Dict[str, Any]:
        """快捷操作卡：同步钓 1 竿、后台跑满一轮、仅同步、交付任务。"""
        state = self._state or {}
        ready = max(0, self.__safe_int(state.get("tasks_ready"), 0))
        quick_limit = min(
            self.MAX_QUICK_CASTS,
            max(1, self.__safe_int(self._max_casts_per_run, 1, min_value=1)),
        )
        rows = [
            self.__option_row(
                "🎣 钓 1 竿",
                f"最多 {quick_limit} 竿",
                [
                    self.__btn(
                        "演练中·仅同步" if self._dry_run else "🎣 钓 1 竿",
                        "fish_now",
                        {"casts": 1},
                        color="primary",
                        disabled=self._dry_run,
                    )
                ],
            ),
            self.__option_row(
                "⏱ 后台钓满一轮",
                f"{self._max_casts_per_run} 竿/轮",
                [
                    self.__btn(
                        "⏱ 后台钓满一轮",
                        "run",
                        {},
                        color="secondary",
                    )
                ],
            ),
            self.__option_row(
                "🔄 仅同步状态",
                "只刷新数据",
                [
                    self.__btn(
                        "🔄 仅同步状态",
                        "sync",
                        {},
                        color="info",
                    )
                ],
            ),
            self.__option_row(
                "📦 交付可完成任务",
                f"{ready} 个待交付" if ready else "暂无",
                [
                    self.__btn(
                        "📦 交付可完成任务" if ready else "📦 暂无可交付",
                        "deliver",
                        {},
                        color="success",
                        disabled=ready == 0,
                    )
                ],
            ),
        ]
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center"},
                    "content": [
                        {"component": "span", "text": "⚡ 快捷操作"},
                        {"component": "VSpacer"},
                        {
                            "component": "VChip",
                            "props": {
                                "size": "small",
                                "variant": "tonal",
                                "color": "info",
                            },
                            "text": "点击后自动刷新页面",
                        },
                    ],
                },
                {"component": "VCardText", "content": rows},
            ],
        }

    def __rod_unlocked(self, key: str) -> bool:
        """判断站点返回的鱼竿信息中该鱼竿是否已解锁。"""
        rods = self._state.get("rods") or {}
        info = rods.get(key) if isinstance(rods, dict) else None
        if not isinstance(info, dict):
            return False
        return bool(info.get("unlocked"))

    def __strategy_tuning_card(self) -> Dict[str, Any]:
        """策略调节卡：常用选项点击即改，数值精调仍回配置页。"""
        state = self._state or {}
        lures = state.get("lures") or {}

        slider_text = {
            "perfect": "完美 0.50",
            "range": "甜蜜区随机",
            "custom": f"自定义 {self._slider_center:.2f}",
        }.get(self._slider_mode, str(self._slider_mode))
        slider_buttons = [
            self.__btn(
                {"perfect": "完美 0.50", "range": "甜蜜区随机", "custom": "自定义"}[mode],
                "set_option",
                {"key": "slider_mode", "value": mode},
                disabled=self._slider_mode == mode,
            )
            for mode in ("perfect", "range", "custom")
        ]

        selected_lure = state.get("selected_lure") or ""
        lure_text = (
            f"自动（{self.LURE_META.get(selected_lure, {}).get('name', '未选')}）"
            if self._lure_mode == "auto"
            else f"固定 {self.LURE_META.get(self._lure_key, {}).get('name', self._lure_key)}"
        )
        lure_buttons: List[Dict[str, Any]] = [
            self.__btn(
                "自动",
                "set_option",
                {"key": "lure_mode", "value": "auto"},
                color="secondary",
                disabled=self._lure_mode == "auto",
            )
        ]
        for key, meta in self.LURE_META.items():
            stock = max(0, self.__safe_int(lures.get(key), 0))
            chosen = self._lure_mode == "fixed" and self._lure_key == key
            lure_buttons.append(
                self.__btn(
                    f"{meta['icon']} {meta['name']}" + ("(库存0)" if stock <= 0 else ""),
                    "set_option",
                    {"key": "lure_fixed", "value": key},
                    disabled=chosen or stock <= 0,
                )
            )

        rod_text = (
            "自动选最强已解锁"
            if self._rod_mode == "auto"
            else f"固定 {self.ROD_META.get(self._rod_key, {}).get('name', self._rod_key)}"
        )
        rod_buttons: List[Dict[str, Any]] = [
            self.__btn(
                "自动",
                "set_option",
                {"key": "rod_mode", "value": "auto"},
                color="secondary",
                disabled=self._rod_mode == "auto",
            )
        ]
        for key, meta in self.ROD_META.items():
            if not self.__rod_unlocked(key):
                rod_buttons.append(
                    {
                        "component": "span",
                        "props": {"style": "font-size:12px; opacity:0.6;"},
                        "text": f"🔒 {meta['name']}",
                    }
                )
                continue
            chosen = self._rod_mode == "fixed" and self._rod_key == key
            rod_buttons.append(
                self.__btn(
                    f"{meta['icon']} {meta['name']}",
                    "set_option",
                    {"key": "rod_fixed", "value": key},
                    disabled=chosen,
                )
            )

        budget_buttons: List[Dict[str, Any]] = [
            self.__btn(
                f"{amount}",
                "set_option",
                {"key": "buy_budget_per_day", "value": amount},
                disabled=self._buy_budget_per_day == amount,
            )
            for amount in (0, 5000, 20000)
        ]
        budget_buttons.append(self.__hint_span("自定义预算请进配置页"))

        keep_buttons: List[Dict[str, Any]] = [
            self.__btn(
                f"{amount}",
                "set_option",
                {"key": "keep_lures", "value": amount},
                disabled=self._keep_lures == amount,
            )
            for amount in (10, 20, 50)
        ]
        keep_buttons.append(self.__hint_span("自定义数值请进配置页"))

        daily_buttons: List[Dict[str, Any]] = [
            self.__btn(
                f"{amount}",
                "set_option",
                {"key": "daily_cast_limit", "value": amount},
                disabled=self._daily_cast_limit == amount,
            )
            for amount in (6, 12, 24, 33)
        ]
        daily_buttons.append(self.__hint_span("自定义数值请进配置页"))

        rows = [
            self.__option_row("收竿方式", slider_text, slider_buttons),
            self.__option_row("鱼饵策略", lure_text, lure_buttons),
            self.__option_row("鱼竿策略", rod_text, rod_buttons),
            self.__toggle_row(
                "自动补饵",
                "auto_buy",
                self._auto_buy,
                on_text="开启补饵",
                off_text="关闭补饵",
            ),
            self.__option_row(
                "每日购饵预算", f"{self._buy_budget_per_day} 魔力", budget_buttons
            ),
            self.__option_row("鱼饵保底库存", f"{self._keep_lures} 枚", keep_buttons),
            self.__option_row(
                "每日抛竿上限", f"{self._daily_cast_limit} 次", daily_buttons
            ),
            self.__toggle_row(
                "演练模式",
                "dry_run",
                self._dry_run,
                on_text="开启演练",
                off_text="关闭演练",
            ),
            self.__toggle_row(
                "自动交付任务",
                "auto_task",
                self._auto_task,
                on_text="开启自动交付",
                off_text="关闭自动交付",
            ),
            self.__toggle_row(
                "自动竞猜",
                "auto_guess",
                self._auto_guess,
                on_text="开启自动竞猜",
                off_text="关闭自动竞猜",
            ),
            self.__toggle_row(
                "时段分散",
                "slot_spread",
                self._slot_spread,
                on_text="开启时段分散",
                off_text="关闭时段分散",
            ),
        ]
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center"},
                    "content": [
                        {"component": "span", "text": "🎛 策略调节"},
                        {"component": "VSpacer"},
                        {
                            "component": "VChip",
                            "props": {
                                "size": "small",
                                "variant": "tonal",
                                "color": "primary",
                            },
                            "text": "点击即生效",
                        },
                    ],
                },
                {"component": "VCardText", "content": rows},
            ],
        }

    @staticmethod
    def __hint_span(text: str) -> Dict[str, Any]:
        """构造行尾的灰色提示文字。"""
        return {
            "component": "span",
            "props": {"style": "font-size:12px; opacity:0.6;"},
            "text": text,
        }

    def __hero_card(
        self,
        catches: int,
        attempt_text: str,
        schedule_info: str,
        updated_at: str,
        last_result: str,
    ) -> Dict[str, Any]:
        pills = [
            {
                "component": "span",
                "props": {
                    "style": (
                        "display:inline-block; padding:2px 10px; border-radius:999px;"
                        " background: rgba(255,255,255,0.18); font-size:12px;"
                        " margin-right:6px;"
                    )
                },
                "text": f"⏱ {schedule_info}",
            },
            {
                "component": "span",
                "props": {
                    "style": (
                        "display:inline-block; padding:2px 10px; border-radius:999px;"
                        " background: rgba(255,255,255,0.18); font-size:12px;"
                    )
                },
                "text": f"🎣 今日次数 {attempt_text}",
            },
        ]
        if self._dry_run:
            pills.append(
                {
                    "component": "span",
                    "props": {
                        "style": (
                            "display:inline-block; padding:2px 10px; border-radius:999px;"
                            " background: rgba(255, 193, 7, 0.3); font-size:12px;"
                            " margin-left:6px;"
                        )
                    },
                    "text": "演练模式",
                }
            )

        summary = "尚未执行"
        if updated_at or last_result:
            summary = f"🕐 上次执行 {updated_at or '未知'}"
            if last_result:
                summary += f" ｜ 📝 {last_result}"

        content: List[Dict[str, Any]] = [
            {
                "component": "div",
                "props": {"style": "font-size: 13px; opacity: 0.9; letter-spacing: 1px;"},
                "text": "🎣 思齐钓鱼",
            },
            {
                "component": "div",
                "props": {
                    "style": "display:flex; align-items:baseline; gap:10px; margin:6px 0 10px 0;"
                },
                "content": [
                    {
                        "component": "div",
                        "props": {"style": "font-size: 44px; font-weight: 800; line-height: 1;"},
                        "text": f"{catches:,}条",
                    },
                    {
                        "component": "div",
                        "props": {"style": "font-size: 14px; opacity: 0.85;"},
                        "text": "今日已钓",
                    },
                ],
            },
            {"component": "div", "props": {"style": "margin-bottom:8px;"}, "content": pills},
            {
                "component": "div",
                "props": {"style": "font-size: 13px; opacity: 0.9;"},
                "text": summary,
            },
        ]
        running = self.__running_chip()
        if running:
            content.append(
                {
                    "component": "div",
                    "props": {"style": "margin-top:10px;"},
                    "content": [running],
                }
            )

        return {
            "component": "div",
            "props": {
                "style": (
                    "border-radius: 16px; padding: 22px 24px; margin-bottom: 16px;"
                    " background: linear-gradient(135deg, #0ea5e9 0%, #2563eb 55%, #1e3a8a 100%);"
                    " color: #ffffff; box-shadow: 0 8px 24px rgba(37, 99, 235, 0.25);"
                )
            },
            "content": content,
        }

    def __attempt_progress_card(self, used: int, limit: int) -> Dict[str, Any]:
        ratio = 0.0
        if limit > 0:
            ratio = min(1.0, max(0.0, used / limit))
        percent = int(round(ratio * 100))
        color = "#22c55e"
        if percent >= 90:
            color = "#ef4444"
        elif percent >= 60:
            color = "#f59e0b"
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center"},
                    "content": [
                        {"component": "span", "text": "🎯 今日抛竿进度"},
                        {"component": "VSpacer"},
                        {
                            "component": "VChip",
                            "props": {"size": "small", "variant": "tonal", "color": "primary"},
                            "text": f"{used} / {limit or '?'}",
                        },
                    ],
                },
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "div",
                            "props": {
                                "style": (
                                    "height: 10px; border-radius: 999px; overflow: hidden;"
                                    " background: rgba(148, 163, 184, 0.25);"
                                )
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "props": {
                                        "style": (
                                            f"height: 100%; width: {percent}%;"
                                            f" background: {color}; border-radius: 999px;"
                                        )
                                    },
                                }
                            ],
                        },
                        {
                            "component": "div",
                            "props": {"style": "font-size: 12px; opacity: 0.75; margin-top: 8px;"},
                            "text": (
                                f"已用 {used} 次，剩余 {max(0, limit - used)} 次；"
                                f"每日次数于 GMT+8 00:00 重置。"
                            ),
                        },
                    ],
                },
            ],
        }

    def __lure_table(self, lures: Dict[str, Any], selected: str) -> Dict[str, Any]:
        rows = []
        for key, meta in self.LURE_META.items():
            count = max(0, self.__safe_int(lures.get(key), 0))
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {
                            "component": "td",
                            "text": f"{meta['icon']} {meta['name']}"
                            + ("（使用中）" if key == selected else ""),
                        },
                        {"component": "td", "text": f"{count:,}"},
                        {"component": "td", "text": f"{meta['price']:,} 魔力"},
                    ],
                }
            )
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center"},
                    "content": [
                        {"component": "span", "text": "🪱 鱼饵仓库"},
                        {"component": "VSpacer"},
                        {
                            "component": "VChip",
                            "props": {
                                "size": "small",
                                "variant": "tonal",
                                "color": "secondary",
                            },
                            "text": "高级饵可提升稀有鱼权重",
                        },
                    ],
                },
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VTable",
                            "props": {"density": "compact"},
                            "content": [
                                {
                                    "component": "thead",
                                    "content": [
                                        {
                                            "component": "tr",
                                            "content": [
                                                {"component": "th", "text": "鱼饵"},
                                                {"component": "th", "text": "库存"},
                                                {"component": "th", "text": "价格"},
                                            ],
                                        }
                                    ],
                                },
                                {"component": "tbody", "content": rows},
                            ],
                        }
                    ],
                },
            ],
        }

    def __log_table(self, history: List[Any]) -> Dict[str, Any]:
        rows = []
        for item in list(history)[-8:][::-1]:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": str(item.get("time", ""))},
                        {"component": "td", "text": str(item.get("action", ""))},
                        {"component": "td", "text": str(item.get("detail", ""))},
                    ],
                }
            )
        if not rows:
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {
                            "component": "td",
                            "props": {"colspan": 3},
                            "text": "暂无执行记录",
                        }
                    ],
                }
            )
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center"},
                    "content": [
                        {"component": "span", "text": "🗒️ 今日渔获"},
                        {"component": "VSpacer"},
                        {
                            "component": "VChip",
                            "props": {"size": "small", "variant": "tonal"},
                            "text": f"最近 {len(rows)} 条",
                        },
                    ],
                },
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VTable",
                            "props": {"density": "compact"},
                            "content": [
                                {
                                    "component": "thead",
                                    "content": [
                                        {
                                            "component": "tr",
                                            "content": [
                                                {"component": "th", "text": "时间"},
                                                {"component": "th", "text": "结果"},
                                                {"component": "th", "text": "详情"},
                                            ],
                                        }
                                    ],
                                },
                                {"component": "tbody", "content": rows},
                            ],
                        }
                    ],
                },
            ],
        }

    @staticmethod
    def __strategy_row(label: str, value: str) -> Dict[str, Any]:
        return {
            "component": "div",
            "props": {
                "style": (
                    "display: flex; justify-content: space-between; gap: 12px;"
                    " padding: 6px 0; border-bottom: 1px dashed rgba(148,163,184,0.25);"
                    " font-size: 13px;"
                )
            },
            "content": [
                {"component": "span", "props": {"style": "opacity: 0.75;"}, "text": label},
                {"component": "span", "props": {"style": "font-weight: 600;"}, "text": value},
            ],
        }

    def __quiz_card(self, state: Dict[str, Any]) -> Dict[str, Any]:
        question = state.get("quiz_question") or ""
        options = state.get("quiz_options") or []
        status = state.get("quiz_status") or "not_started"
        status_text = {
            "not_started": "未开始",
            "in_progress": "进行中",
            "completed": "已完成",
        }.get(status, "未知")
        content: List[Dict[str, Any]] = []
        if question:
            content.append(
                {
                    "component": "div",
                    "props": {"style": "font-weight: 600; margin-bottom: 8px;"},
                    "text": f"❓ {question}",
                }
            )
            for index, option in enumerate(options):
                content.append(
                    {
                        "component": "div",
                        "props": {"style": "font-size: 13px; padding: 2px 0; opacity: 0.85;"},
                        "text": f"{chr(65 + index)}. {option}",
                    }
                )
            content.append(
                {
                    "component": "div",
                    "props": {"style": "font-size: 12px; opacity: 0.7; margin-top: 8px;"},
                    "text": "答题需在站点手动进行，答对每题 +1 次钓鱼机会；答错奖励清零。",
                }
            )
        else:
            content.append(
                {
                    "component": "div",
                    "props": {"style": "font-size: 13px; opacity: 0.8;"},
                    "text": "今日暂无待答题目（未开始，或已完成）。",
                }
            )
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center"},
                    "content": [
                        {"component": "span", "text": "🧠 今日知识问答"},
                        {"component": "VSpacer"},
                        {
                            "component": "VChip",
                            "props": {"size": "small", "variant": "tonal"},
                            "text": status_text,
                        },
                    ],
                },
                {"component": "VCardText", "content": content},
            ],
        }

    async def run_once_api(self) -> Dict[str, Any]:
        """API：提交一次后台钓鱼任务，不阻塞请求。"""
        if self.__get_lock().locked():
            return {"success": False, "message": "已有钓鱼任务正在执行"}
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "api_run_once",
            self.run_fishing_task,
            "思齐钓鱼API立即运行",
            delay_seconds=1,
        )
        return {"success": True, "message": "任务已提交，完成后会发送通知"}

    async def api_status(self) -> Dict[str, Any]:
        """API：返回今日统计与运行态快照。"""
        return {"success": True, "data": {"daily": self._daily, "state": self._state}}

    async def api_fish_now(
        self, payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """API：页面快捷钓 1~3 竿，不补饵、不竞猜、不交付任务。"""
        payload = payload if isinstance(payload, dict) else {}
        limit = min(
            self.MAX_QUICK_CASTS,
            max(1, self.__safe_int(self._max_casts_per_run, 1, min_value=1)),
        )
        casts = min(max(1, self.__safe_int(payload.get("casts"), 1)), limit)
        lock = self.__get_lock()
        if lock.locked():
            logger.warning("思齐钓鱼：快捷抛竿被跳过，已有任务执行中")
            return {
                "success": False,
                "status": "running",
                "message": "已有钓鱼任务正在执行，请稍后再试",
            }

        async with lock:
            result = self.__new_result()
            try:
                await self.__execute(
                    result,
                    casts_limit=casts,
                    interval_override=min(
                        self._cast_interval, self.QUICK_CAST_INTERVAL
                    ),
                    allow_buy=False,
                    allow_guess=False,
                    allow_deliver=False,
                )
            except httpx2.TimeoutException:
                result["status"] = "error"
                result["message"] = "请求超时"
                logger.error("思齐钓鱼：快捷抛竿请求超时")
            except httpx2.RequestError as e:
                result["status"] = "error"
                result["message"] = f"请求异常：{e}"
                logger.error(f"思齐钓鱼：快捷抛竿请求异常：{e}")
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"执行异常：{e}"
                logger.error(f"思齐钓鱼：快捷抛竿执行异常：{e}")

            self.__finish_task(result)

        response = dict(result)
        response["success"] = result.get("status") == "completed"
        return response

    async def api_sync(self) -> Dict[str, Any]:
        """API：只同步站点状态，绝不调用任何 action 接口。"""
        lock = self.__get_lock()
        if lock.locked():
            return {
                "success": False,
                "status": "running",
                "message": "已有钓鱼任务正在执行，请稍后再试",
            }

        async with lock:
            try:
                cookie = self._cookie or await self.__get_site_cookie_async()
                if not cookie or "c_secure_pass=" not in cookie:
                    return {
                        "success": False,
                        "message": "缺少包含 c_secure_pass 的思齐 Cookie，请检查配置或站点管理",
                    }
                self._daily = self.__roll_daily(
                    self._daily, datetime.now(self.TZ_CN).strftime("%Y-%m-%d")
                )
                client = self.__build_client(cookie, self.__game_headers(), 20)
                data = self.__parse_state(await self.__fetch_page(client))
                if not data:
                    return {
                        "success": False,
                        "message": "无法读取钓鱼页面，请检查 Cookie 是否失效",
                    }
                self.__apply_state(data)
                self.__record("sync", "手动同步状态")
                self.__save_state()
            except httpx2.TimeoutException:
                logger.error("思齐钓鱼：同步状态请求超时")
                return {"success": False, "message": "请求超时"}
            except httpx2.RequestError as e:
                logger.error(f"思齐钓鱼：同步状态请求异常：{e}")
                return {"success": False, "message": f"请求异常：{e}"}
            except Exception as e:
                logger.error(f"思齐钓鱼：同步状态执行异常：{e}")
                return {"success": False, "message": f"同步失败：{e}"}

        state = self._state or {}
        return {
            "success": True,
            "message": "已同步状态",
            "data": {
                "attempts_used": state.get("attempts_used"),
                "attempts_limit": state.get("attempts_limit"),
                "catches": state.get("catches"),
                "lures": state.get("lures"),
                "tasks_ready": state.get("tasks_ready"),
                "updated_at": state.get("updated_at"),
            },
        }

    async def api_set_option(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """API：页面修改白名单选项，整体回写配置并同步内存属性。"""
        payload = payload if isinstance(payload, dict) else {}
        key = str(payload.get("key") or "").strip()
        value = payload.get("value")
        ok, normalized, label = self.__normalize_option(key, value)
        if not ok:
            logger.warning(f"思齐钓鱼：页面修改选项被拒绝：{label}")
            return {"success": False, "message": label, "data": {"key": key}}

        if key == "rod_fixed" and not self.__rod_unlocked(normalized["rod_key"]):
            return {"success": False, "message": "鱼竿未解锁", "data": {"key": key}}

        try:
            config = dict(self.get_config() or {})
        except Exception as e:
            logger.error(f"思齐钓鱼：读取配置失败，未保存：{e}")
            return {
                "success": False,
                "message": "读取配置失败，未保存",
                "data": {"key": key},
            }
        updates = (
            dict(normalized) if isinstance(normalized, dict) else {key: normalized}
        )
        config.update(updates)
        try:
            self.update_config(config)
        except Exception as e:
            logger.error(f"思齐钓鱼：保存选项失败：{e}")
            return {
                "success": False,
                "message": f"保存失败：{e}",
                "data": {"key": key},
            }

        for field, field_value in updates.items():
            setattr(self, f"_{field}", field_value)
        logger.info(f"思齐钓鱼：页面已更新{label}")
        return {
            "success": True,
            "message": f"已更新{label}",
            "data": {"key": key, "value": normalized},
        }

    async def api_deliver(self) -> Dict[str, Any]:
        """API：刷新状态后仅交付 progress.ready 的任务。"""
        lock = self.__get_lock()
        if lock.locked():
            return {
                "success": False,
                "status": "running",
                "message": "已有钓鱼任务正在执行，请稍后再试",
            }

        async with lock:
            try:
                cookie = self._cookie or await self.__get_site_cookie_async()
                if not cookie or "c_secure_pass=" not in cookie:
                    return {
                        "success": False,
                        "message": "缺少包含 c_secure_pass 的思齐 Cookie，请检查配置或站点管理",
                    }
                self._daily = self.__roll_daily(
                    self._daily, datetime.now(self.TZ_CN).strftime("%Y-%m-%d")
                )
                client = self.__build_client(cookie, self.__game_headers(), 20)
                data = self.__parse_state(await self.__fetch_page(client))
                if not data:
                    return {
                        "success": False,
                        "message": "无法读取钓鱼页面，请检查 Cookie 是否失效",
                    }
                self.__apply_state(data)
                ready = self.__safe_int(self._state.get("tasks_ready"), 0)
                if ready <= 0:
                    self.__save_state()
                    return {
                        "success": True,
                        "data": {"delivered": 0},
                        "message": "当前没有可交付的任务",
                    }
                delivered = await self.__deliver_tasks(client)
                self.__save_state()
            except httpx2.TimeoutException:
                logger.error("思齐钓鱼：交付任务请求超时")
                return {"success": False, "message": "请求超时"}
            except httpx2.RequestError as e:
                logger.error(f"思齐钓鱼：交付任务请求异常：{e}")
                return {"success": False, "message": f"请求异常：{e}"}
            except Exception as e:
                logger.error(f"思齐钓鱼：交付任务执行异常：{e}")
                return {"success": False, "message": f"交付失败：{e}"}

        message = f"已交付 {delivered} 个任务" if delivered else "任务交付未成功"
        return {"success": True, "data": {"delivered": delivered}, "message": message}

    async def run_fishing_task(self, **kwargs) -> Dict[str, Any]:
        """执行一次任务：同步状态、补饵、选饵选竿、抛竿、交付任务。"""
        lock = self.__get_lock()
        async with lock:
            result = self.__new_result()
            try:
                await self.__execute(result)
            except httpx2.TimeoutException:
                result["status"] = "error"
                result["message"] = "请求超时"
                logger.error("思齐钓鱼：请求超时")
            except httpx2.RequestError as e:
                result["status"] = "error"
                result["message"] = f"请求异常：{e}"
                logger.error(f"思齐钓鱼：请求异常：{e}")
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"执行异常：{e}"
                logger.error(f"思齐钓鱼：执行异常：{e}")

            self.__finish_task(result)
            return result

    async def __execute(
        self,
        result: Dict[str, Any],
        *,
        casts_limit: Optional[int] = None,
        interval_override: Optional[int] = None,
        allow_buy: bool = True,
        allow_guess: bool = True,
        allow_deliver: bool = True,
    ) -> None:
        cookie = self._cookie or await self.__get_site_cookie_async()
        if not cookie or "c_secure_pass=" not in cookie:
            result["status"] = "auth_failed"
            result["message"] = "缺少包含 c_secure_pass 的思齐 Cookie，请检查配置或站点管理"
            logger.error("思齐钓鱼：Cookie 缺失或格式不正确")
            return

        self._daily = self.__roll_daily(
            self._daily, datetime.now(self.TZ_CN).strftime("%Y-%m-%d")
        )
        client = self.__build_client(cookie, self.__game_headers(), 20)

        page_html = await self.__fetch_page(client)
        data = self.__parse_state(page_html)
        if not data:
            result["status"] = "auth_failed"
            result["message"] = "无法读取钓鱼页面，请检查 Cookie 是否失效"
            logger.error("思齐钓鱼：页面解析失败，可能 Cookie 失效")
            return

        self.__apply_state(data)
        result["data"] = {
            "attempts_used": self._state.get("attempts_used"),
            "attempts_limit": self._state.get("attempts_limit"),
            "catches": self._state.get("catches"),
            "lures": self._state.get("lures"),
        }

        if self._dry_run:
            result["status"] = "completed"
            result["message"] = "演练模式：已同步状态，未抛竿"
            self.__record("sync", result["message"])
            return

        if allow_guess and self._auto_guess:
            await self.__submit_guess(client)

        await self.__ensure_lure(client, allow_buy=allow_buy)
        await self.__ensure_rod(client)

        cast_result = await self.__cast_loop(
            client, max_casts=casts_limit, interval=interval_override
        )
        result["data"]["casts"] = cast_result["casts"]
        result["data"]["caught"] = cast_result["caught"]
        result["data"]["new_discoveries"] = cast_result["new_discoveries"]
        result["data"]["misses"] = cast_result["misses"]
        result["data"]["miss_rewards"] = cast_result["miss_rewards"]
        result["data"]["spent"] = self.__safe_int(self._daily.get("spent"), 0)

        if allow_deliver and self._auto_task:
            delivered = await self.__deliver_tasks(client)
            result["data"]["delivered"] = delivered

        summary = (
            f"抛竿 {cast_result['casts']} 次，钓到 {cast_result['caught']} 条"
            f"（新发现 {cast_result['new_discoveries']} 种），"
            f"剩余次数 {max(0, cast_result['remaining'])}"
        )
        if result["data"].get("delivered"):
            summary += f"，交付任务 {result['data']['delivered']} 个"
        if cast_result.get("failure"):
            summary += f"；停止原因：{cast_result['failure']}"

        result["status"] = cast_result["status"]
        result["message"] = summary
        self._daily["last_result"] = summary
        self.__record("run", summary)

    async def __cast_loop(
        self,
        client: AsyncRequestUtils,
        *,
        max_casts: Optional[int] = None,
        interval: Optional[int] = None,
    ) -> Dict[str, Any]:
        cast_limit = max_casts or self._max_casts_per_run
        wait = self._cast_interval if interval is None else interval
        casts = 0
        caught = 0
        new_discoveries = 0
        misses = 0
        miss_rewards = 0
        status = "completed"
        failure = ""

        while casts < cast_limit:
            # 每竿重算时段，避免跨 4 小时边界后仍记在旧时段
            slot_now = str(self.__time_slot(datetime.now(self.TZ_CN).hour))
            if not isinstance(self._daily.get("slots"), dict):
                self._daily["slots"] = {}
            slots = self._daily["slots"]
            slot_used = self.__safe_int(slots.get(slot_now), 0)
            attempts_limit = self.__safe_int(self._state.get("attempts_limit"), 0)
            attempts_used = self.__safe_int(self._state.get("attempts_used"), 0)
            # 站点次数未知（limit<=0）时不作为约束
            site_remaining = (
                max(0, attempts_limit - attempts_used)
                if attempts_limit > 0
                else self.MAX_DAILY_CASTS
            )
            # 依次检查：插件日限、站点剩余、时段分散，任一耗尽即停止
            if self._daily["casts"] >= self._daily_cast_limit:
                status = "partial"
                failure = "已达插件每日上限"
                break
            if attempts_limit > 0 and attempts_used >= attempts_limit:
                status = "partial"
                failure = "站点今日次数已用完"
                break
            if self._slot_spread and slot_used >= self._per_slot_casts:
                status = "partial"
                failure = "本时段已达上限"
                break
            allowed = self.__allowed_casts(
                cast_limit - casts,
                self._daily["casts"],
                self._daily_cast_limit,
                slot_used if self._slot_spread else 0,
                self._per_slot_casts if self._slot_spread else self.MAX_DAILY_CASTS,
                site_remaining,
            )
            if allowed <= 0:
                status = "partial"
                failure = "可用次数不足"
                break

            lure_key = self._state.get("selected_lure")
            stock = self.__safe_int(
                (self._state.get("lures") or {}).get(lure_key), 0
            )
            if not lure_key or stock <= 0:
                status = "partial"
                failure = "鱼饵不足，已停止抛竿"
                self.__record("stop", "鱼饵不足，已停止抛竿")
                break

            slider = self.__slider_value(
                self._slider_mode, self._slider_center, self._slider_jitter
            )
            payload = await self.__post_action(
                client, "perform_fishing", slider_position=f"{slider:.4f}"
            )
            if not self.__game_ok(payload):
                message = payload.get("message") or "抛竿失败"
                failure = message
                self.__record("fail", message)
                status = "partial" if casts else "failed"
                break

            if isinstance(payload.get("data"), dict):
                self.__apply_state(payload["data"])
            else:
                logger.debug("思齐钓鱼：抛竿回包缺少 data，本地次数不更新")

            casts += 1
            self._daily["casts"] += 1
            self._daily["slots"][slot_now] = (
                self.__safe_int(self._daily["slots"].get(slot_now), 0) + 1
            )

            outcome = payload.get("result") or {}
            kind = outcome.get("status")
            if kind == "catch":
                caught += 1
                self._daily["catches"] += 1
                fish = outcome.get("fish") or {}
                is_new = bool(outcome.get("is_new_discovery"))
                if is_new:
                    new_discoveries += 1
                    self._daily["new_discoveries"] += 1
                detail = (
                    f"{fish.get('name', '未知鱼')}"
                    f"（{fish.get('rarity_label', '')}"
                    f"/{self.__timing_label(outcome.get('timing'))}）"
                )
                if is_new:
                    detail += " 新发现"
                self.__record("catch", detail)
            elif kind == "miss_reward":
                miss_rewards += 1
                self._daily["miss_rewards"] += 1
                self.__record("miss", "空钩，拾到补偿物品")
            else:
                misses += 1
                self._daily["misses"] += 1
                self.__record("miss", "鱼儿溜走了")

            if wait > 0:
                await asyncio.sleep(wait)

        remaining = max(
            0,
            self.__safe_int(self._state.get("attempts_limit"), 0)
            - self.__safe_int(self._state.get("attempts_used"), 0),
        )
        if casts == 0 and status == "completed":
            status = "partial"
        return {
            "casts": casts,
            "caught": caught,
            "new_discoveries": new_discoveries,
            "misses": misses,
            "miss_rewards": miss_rewards,
            "remaining": remaining,
            "status": status,
            "failure": failure,
        }

    async def __ensure_lure(
        self, client: AsyncRequestUtils, *, allow_buy: bool = True
    ) -> None:
        """按策略补饵并选中本次要用的鱼饵。"""
        lures = self._state.get("lures") or {}
        key = self.__pick_lure(lures, self._lure_mode, self._lure_key)

        # 固定策略补固定饵，其它策略补指定的补货饵种；关闭补饵或预算为零则不买
        buy_key = self._lure_key if self._lure_mode == "fixed" else self._buy_lure_key
        price = self.__safe_int(self.LURE_META.get(buy_key, {}).get("price"), 0)
        quantity = 0
        if allow_buy and self._auto_buy and self._buy_budget_per_day > 0:
            quantity = self.__buy_quantity(
                self.__safe_int(lures.get(buy_key), 0),
                self._keep_lures,
                price,
                self._buy_budget_per_day,
                self.__safe_int(self._daily.get("spent"), 0),
            )
        if quantity > 0:
            payload = await self.__post_action(
                client, "purchase_lure", lure_key=buy_key, quantity=quantity
            )
            if self.__game_ok(payload):
                self._daily["spent"] = (
                    self.__safe_int(self._daily.get("spent"), 0) + price * quantity
                )
                self.__record(
                    "buy",
                    f"购买 {self.LURE_META.get(buy_key, {}).get('name', buy_key)} × {quantity}",
                )
                if isinstance(payload.get("data"), dict):
                    self.__apply_state(payload["data"])
                lures = self._state.get("lures") or {}
                key = self.__pick_lure(lures, self._lure_mode, self._lure_key)

        if not key:
            return
        if self._state.get("selected_lure") == key:
            return
        payload = await self.__post_action(client, "select_lure", lure_key=key)
        if self.__game_ok(payload) and isinstance(payload.get("data"), dict):
            self.__apply_state(payload["data"])

    async def __ensure_rod(self, client: AsyncRequestUtils) -> None:
        """按策略切换鱼竿。"""
        rods = self._state.get("rods") or {}
        key = self.__pick_rod(rods, self._rod_mode, self._rod_key)
        if not key:
            return
        info = rods.get(key) or {}
        if isinstance(info, dict) and not info.get("unlocked"):
            self.__record("warn", f"鱼竿 {key} 未解锁，跳过切换")
            logger.warning(f"思齐钓鱼：鱼竿 {key} 未解锁，跳过切换")
            return
        if self._state.get("selected_rod") == key:
            return
        payload = await self.__post_action(client, "select_rod", rod_key=key)
        if self.__game_ok(payload) and isinstance(payload.get("data"), dict):
            self.__apply_state(payload["data"])

    async def __submit_guess(self, client: AsyncRequestUtils) -> None:
        """仅在今日尚未抛竿时提交竞猜，避免重复占用次数。"""
        if self.__safe_int(self._state.get("attempts_used"), 0) != 0:
            return
        payload = await self.__post_action(
            client, "submit_guess", guess_value=self._guess_value
        )
        if self.__game_ok(payload):
            self.__record("guess", f"提交竞猜 {self._guess_value} 条")
            if isinstance(payload.get("data"), dict):
                self.__apply_state(payload["data"])

    async def __deliver_tasks(self, client: AsyncRequestUtils) -> int:
        tasks = self._state.get("tasks") or {}
        delivered = 0
        for task_id in sorted(tasks, key=lambda x: self.__safe_int(x, 0)):
            info = tasks.get(task_id)
            if not isinstance(info, dict):
                continue
            progress = info.get("progress") or {}
            if not progress.get("ready"):
                continue
            payload = await self.__post_action(
                client, "deliver_task", task_id=self.__safe_int(task_id, 0)
            )
            if self.__game_ok(payload):
                delivered += 1
                self.__record("task", f"交付任务 {task_id}")
                if isinstance(payload.get("data"), dict):
                    self.__apply_state(payload["data"])
            else:
                break
        return delivered

    @staticmethod
    def __pick_lure(lures: Dict[str, Any], mode: str, fixed_key: str) -> str:
        """按策略挑选鱼饵：固定返回指定值，自动挑库存中权重最高的。"""
        lures = lures if isinstance(lures, dict) else {}
        if mode == "fixed":
            return (
                fixed_key
                if SiQiFishingV3.__safe_int(lures.get(fixed_key), 0) > 0
                else ""
            )

        best_key = ""
        best_score = -1
        for key, meta in SiQiFishingV3.LURE_META.items():
            if SiQiFishingV3.__safe_int(lures.get(key), 0) <= 0:
                continue
            score = SiQiFishingV3.__safe_int(meta.get("score"), 0)
            if score > best_score:
                best_score = score
                best_key = key
        return best_key

    @staticmethod
    def __pick_rod(rods: Dict[str, Any], mode: str, fixed_key: str) -> str:
        """按策略挑选鱼竿：固定返回指定值，自动挑已解锁中权重最高的。"""
        rods = rods if isinstance(rods, dict) else {}
        if mode == "fixed":
            return fixed_key

        best_key = ""
        best_score = -1
        for key, meta in SiQiFishingV3.ROD_META.items():
            info = rods.get(key) or {}
            unlocked = bool(info.get("unlocked")) if isinstance(info, dict) else False
            if not unlocked:
                continue
            score = SiQiFishingV3.__safe_int(meta.get("score"), 0)
            if score > best_score:
                best_score = score
                best_key = key
        return best_key

    @staticmethod
    def __slider_value(mode: str, center: float, jitter: float) -> float:
        """计算本次收竿的滑块位置：完美居中抖动、甜蜜区随机或自定义抖动。"""
        try:
            jitter = abs(float(jitter))
        except (TypeError, ValueError):
            jitter = 0.0
        if mode == "range":
            return random.uniform(SiQiFishingV3.PERFECT_MIN, SiQiFishingV3.PERFECT_MAX)
        if mode == "custom":
            base = SiQiFishingV3.__safe_float(center, 0.5, 0.0, 1.0)
            return min(1.0, max(0.0, base + random.uniform(-jitter, jitter)))
        position = 0.5 + random.uniform(-jitter, jitter)
        return min(
            SiQiFishingV3.PERFECT_MAX, max(SiQiFishingV3.PERFECT_MIN, position)
        )

    @staticmethod
    def __timing_label(timing: Any) -> str:
        if timing == "perfect":
            return "完美"
        if timing == "normal":
            return "正常"
        return str(timing or "未知")

    @staticmethod
    def __option_label(key: str) -> str:
        """选项键 → 中文名；未知键回退为原始键。"""
        name = str(key or "").strip()
        spec = SiQiFishingV3.OPTION_WHITELIST.get(name)
        if isinstance(spec, dict) and spec.get("label"):
            return str(spec["label"])
        return name

    @staticmethod
    def __normalize_option(key: str, value: Any) -> Tuple[bool, Any, str]:
        """校验页面提交的选项。

        返回 ``(是否通过, 归一化值, 中文项名或失败原因)``；复合键展开为多字段字典，
        拒绝时不返回任何可落库的值。纯函数，不访问实例状态。
        """
        name = str(key or "").strip()
        if not name:
            return False, None, "缺少选项名，请进配置页调整"
        if name in SiQiFishingV3.OPTION_BLOCKED:
            return False, None, f"「{name}」只能在配置页修改"
        spec = SiQiFishingV3.OPTION_WHITELIST.get(name)
        if not isinstance(spec, dict):
            return False, None, f"选项「{name}」不支持页面修改，请进配置页调整"

        label = str(spec.get("label") or name)
        kind = spec.get("type")

        if kind == "enum":
            text = str(value if value is not None else "").strip()
            values = tuple(spec.get("values") or ())
            if text not in values:
                return False, None, f"{label}仅支持：{' / '.join(values)}"
            return True, text, label

        if kind == "lure_fixed":
            text = str(value if value is not None else "").strip()
            if text not in SiQiFishingV3.LURE_META:
                return False, None, f"{label}仅支持：{' / '.join(SiQiFishingV3.LURE_META)}"
            return True, {"lure_mode": "fixed", "lure_key": text}, label

        if kind == "rod_fixed":
            text = str(value if value is not None else "").strip()
            if text not in SiQiFishingV3.ROD_META:
                return False, None, f"{label}仅支持：{' / '.join(SiQiFishingV3.ROD_META)}"
            return True, {"rod_mode": "fixed", "rod_key": text}, label

        if kind == "bool":
            if isinstance(value, bool):
                return True, value, label
            if isinstance(value, int) and value in (0, 1):
                return True, bool(value), label
            if isinstance(value, str):
                text = value.strip().lower()
                if text in ("true", "false"):
                    return True, text == "true", label
                if text in ("1", "0"):
                    return True, text == "1", label
            return False, None, f"{label}仅支持 true / false"

        if kind == "int":
            lower = int(spec.get("min", 0))
            upper = int(spec.get("max", 0))
            number: Optional[int] = None
            if isinstance(value, bool):
                number = None
            elif isinstance(value, int):
                number = value
            elif isinstance(value, float):
                number = int(value)
            elif isinstance(value, str):
                try:
                    number = int(value.strip())
                except ValueError:
                    number = None
            if number is None or number < lower or number > upper:
                return False, None, f"{label}需为 {lower}~{upper} 的整数"
            return True, number, label

        return False, None, f"{label}只能在配置页修改"

    async def __fetch_page(self, client: AsyncRequestUtils) -> str:
        """抓取钓鱼页面的原始 HTML；网络异常不在此处吞掉，交由调用方分类。"""
        response = await client.get_res(self.FISHING_URL, raise_exception=True)
        return getattr(response, "text", "") or ""

    async def __post_action(
        self, client: AsyncRequestUtils, action: str, **payload: Any
    ) -> Dict[str, Any]:
        data = {"action": action}
        for key, value in payload.items():
            data[key] = str(value)
        try:
            response = await client.post_res(
                self.FISHING_URL, data=data, raise_exception=True
            )
        except httpx2.TimeoutException:
            logger.error(f"思齐钓鱼：{action} 请求超时")
            return {}
        except httpx2.HTTPError as e:
            logger.error(f"思齐钓鱼：{action} 请求异常：{e}")
            return {}
        if response is None:
            logger.error(f"思齐钓鱼：{action} 无响应")
            return {}
        try:
            result = response.json()
        except Exception:
            logger.error(f"思齐钓鱼：{action} 返回非 JSON")
            return {}
        return result if isinstance(result, dict) else {}

    @staticmethod
    def __parse_state(html: str) -> Optional[Dict[str, Any]]:
        """从页面中解析 FISHING_DATA 内联 JSON。"""
        if not html:
            return None
        index = html.find(SiQiFishingV3.FISHING_DATA_MARKER)
        if index < 0:
            return None
        start = index + len(SiQiFishingV3.FISHING_DATA_MARKER)
        try:
            data, _ = json.JSONDecoder().raw_decode(html[start:])
        except ValueError:
            return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def __extract_state(data: Dict[str, Any]) -> Dict[str, Any]:
        """把接口返回或页面解析到的数据结构映射成运行态快照（不含 updated_at）。"""
        if not isinstance(data, dict):
            return {}

        def as_dict(value: Any) -> Dict[str, Any]:
            return value if isinstance(value, dict) else {}

        state = as_dict(data.get("state"))
        inventory = as_dict(data.get("inventory"))
        config = as_dict(data.get("config"))
        user = as_dict(data.get("user"))
        quiz = as_dict(data.get("knowledge_quiz"))
        tasks = as_dict(data.get("tasks"))
        codex = data.get("codex") or []

        lures = {
            key: SiQiFishingV3.__safe_int(value, 0)
            for key, value in (inventory.get("lures") or {}).items()
        }
        rods = {
            key: dict(value) if isinstance(value, dict) else {"unlocked": bool(value)}
            for key, value in (inventory.get("rods") or {}).items()
        }

        unlocked = 0
        total = 0
        if isinstance(codex, list):
            total = len(codex)
            unlocked = sum(
                1 for item in codex if isinstance(item, dict) and item.get("discovered")
            )

        question = as_dict(quiz.get("current_question"))

        return {
            "attempts_used": SiQiFishingV3.__safe_int(
                state.get("daily_attempts_used"), 0
            ),
            "attempts_limit": SiQiFishingV3.__safe_int(
                state.get("daily_attempts_limit"), 0
            ),
            "extra_attempts": SiQiFishingV3.__safe_int(
                state.get("daily_extra_attempts"), 0
            ),
            "catches": SiQiFishingV3.__safe_int(state.get("daily_catch_count"), 0),
            "magic": SiQiFishingV3.__safe_int(user.get("seedbonus"), 0),
            "selected_lure": state.get("selected_lure") or "",
            "selected_rod": state.get("selected_rod") or "",
            "lures": lures,
            "rods": rods,
            "codex_total": total,
            "codex_unlocked": unlocked,
            "quiz_status": quiz.get("status") or "",
            "quiz_question": question.get("question") or "",
            "quiz_options": list(question.get("options") or []),
            "guess_status": str(state.get("daily_guess_status") or ""),
            "guess_reward": SiQiFishingV3.__safe_int(
                config.get("guessRewardAttempts"), 0
            ),
            "tasks": tasks,
            "tasks_ready": sum(
                1
                for info in tasks.values()
                if isinstance(info, dict) and (info.get("progress") or {}).get("ready")
            ),
            "last_message": "",
        }

    def __apply_state(self, data: Dict[str, Any]) -> None:
        """把接口返回或页面解析到的数据结构写入运行态快照。"""
        snapshot = self.__extract_state(data)
        if not snapshot:
            return
        snapshot["updated_at"] = time.time()
        self._state.update(snapshot)

    @staticmethod
    def __allowed_casts(
        max_per_run: int,
        daily_casts: int,
        daily_limit: int,
        slot_used: int,
        per_slot_limit: int,
        site_remaining: int,
    ) -> int:
        """计算本次最多可抛竿数：单次、每日、时段、站点剩余四重截断后取最小值。"""
        limits = [
            SiQiFishingV3.__safe_int(max_per_run, 0),
            SiQiFishingV3.__safe_int(daily_limit, 0)
            - SiQiFishingV3.__safe_int(daily_casts, 0),
            SiQiFishingV3.__safe_int(per_slot_limit, 0)
            - SiQiFishingV3.__safe_int(slot_used, 0),
            SiQiFishingV3.__safe_int(site_remaining, 0),
        ]
        return max(0, min(limits))

    @staticmethod
    def __buy_quantity(
        stock: int, keep: int, price: int, budget: int, spent: int
    ) -> int:
        """计算本次补饵数量：库存已达保底、无价格或无预算时不购买。"""
        stock = SiQiFishingV3.__safe_int(stock, 0)
        keep = SiQiFishingV3.__safe_int(keep, 0)
        price = SiQiFishingV3.__safe_int(price, 0)
        budget = SiQiFishingV3.__safe_int(budget, 0)
        spent = SiQiFishingV3.__safe_int(spent, 0)
        if stock >= keep or price <= 0 or budget <= 0:
            return 0
        need = max(1, keep - stock)
        affordable = (budget - spent) // price
        return max(0, min(need, affordable))

    @staticmethod
    def __roll_daily(daily: Dict[str, Any], today: str) -> Dict[str, Any]:
        """按 GMT+8 自然日重置当日计数，同日则原样返回。"""
        daily = daily if isinstance(daily, dict) else {}
        if daily.get("date") == today:
            return daily
        return {
            "date": today,
            "casts": 0,
            "catches": 0,
            "new_discoveries": 0,
            "misses": 0,
            "miss_rewards": 0,
            "spent": 0,
            "slots": {},
            "last_result": "",
            "history": [],
        }

    @staticmethod
    def __time_slot(hour: int) -> int:
        """返回 GMT+8 当天所属时段序号（每 4 小时一段，1~6）。"""
        return SiQiFishingV3.__safe_int(hour, 0) // 4 + 1

    def __record(self, action: str, detail: str) -> None:
        history = list(self._daily.get("history") or [])
        history.append(
            {
                "time": datetime.now(self.TZ_CN).strftime("%H:%M:%S"),
                "action": action,
                "detail": detail,
            }
        )
        self._daily["history"] = history[-self.DAILY_HISTORY_LIMIT:]

    def __get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def __build_client(
        self, cookie: str, headers: Dict[str, str], timeout: int
    ) -> AsyncRequestUtils:
        return AsyncRequestUtils(
            headers=headers,
            cookies=self.__cookie_to_dict(cookie),
            timeout=timeout,
        )

    def __get_site_cookie(self) -> str:
        try:
            site = SiteOper().get_by_domain(self.SITE_DOMAIN)
            if site and site.cookie:
                return site.cookie
        except Exception as e:
            logger.error(f"思齐钓鱼：读取站点 Cookie 异常：{e}")
        return ""

    async def __get_site_cookie_async(self) -> str:
        try:
            site = await SiteOper().async_get_by_domain(self.SITE_DOMAIN)
            if site and site.cookie:
                return site.cookie
        except Exception as e:
            logger.error(f"思齐钓鱼：读取站点 Cookie 异常：{e}")
        return ""

    @staticmethod
    def __cookie_to_dict(cookie: str) -> Dict[str, str]:
        cookies: Dict[str, str] = {}
        for item in (cookie or "").split(";"):
            if "=" not in item:
                continue
            key, value = item.split("=", 1)
            key = key.strip()
            if key:
                cookies[key] = value.strip()
        return cookies

    def __game_headers(self) -> Dict[str, str]:
        return {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "content-type": "application/x-www-form-urlencoded",
            "origin": self.BASE_URL,
            "referer": self.FISHING_URL,
            "user-agent": self.UA,
        }

    @staticmethod
    def __game_ok(payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False
        return bool(payload.get("success")) or payload.get("status") == "success"

    @staticmethod
    def __safe_int(value: Any, default: int, min_value: int = None) -> int:
        try:
            result = int(value) if value is not None else default
        except (ValueError, TypeError):
            result = default
        if min_value is not None:
            result = max(min_value, result)
        return result

    @staticmethod
    def __safe_float(
        value: Any, default: float, min_value: float, max_value: float
    ) -> float:
        try:
            result = float(value)
        except (ValueError, TypeError):
            result = default
        return min(max_value, max(min_value, result))

    @staticmethod
    def __new_result(status: str = "running", message: str = "") -> Dict[str, Any]:
        return {
            "status": status,
            "message": message,
            "data": {},
            "timestamp": time.time(),
        }

    def __finish_task(self, result: Dict[str, Any]) -> None:
        if self._notify:
            self.__send_notification(result)
        self._state["last_message"] = result.get("message", "")
        self.__save_state()

    def __save_state(self) -> None:
        try:
            self.save_data("daily", self._daily)
            self.save_data("state", self._state)
        except Exception as e:
            logger.error(f"思齐钓鱼：保存状态失败：{e}")

    def __send_notification(self, result: Dict[str, Any]) -> None:
        status_map = {
            "completed": "✅ 成功",
            "partial": "⚠️ 部分完成",
            "failed": "❌ 失败",
            "auth_failed": "🔐 认证失败",
            "error": "💥 异常",
        }
        status_text = status_map.get(result.get("status"), result.get("status", "未知"))
        text = f"状态：{status_text}\n消息：{result.get('message')}"
        data = result.get("data") or {}
        if data.get("attempts_limit"):
            text += (
                f"\n\n🎣 今日次数：{data.get('attempts_used')}/{data.get('attempts_limit')}"
            )
        if data.get("caught") is not None:
            text += f"\n🐟 本次钓到：{data.get('caught')} 条"
        if data.get("new_discoveries"):
            text += f"\n✨ 新发现：{data.get('new_discoveries')} 种"
        try:
            self.post_message(
                mtype=MessageType.Plugin, title="🎣【思齐钓鱼】", text=text
            )
        except Exception as e:
            logger.error(f"思齐钓鱼：发送通知失败：{e}")

    @eventmanager.register(EventType.PluginAction)
    async def run_once_command(self, event: Event = None):
        """远程命令入口：只处理本插件的 action。"""
        event_data = event.event_data if event else {}
        if not event_data or event_data.get("action") != "siqi_fishing":
            return
        channel = event_data.get("channel")
        userid = event_data.get("user")
        if self.__get_lock().locked():
            self.post_message(
                channel=channel,
                userid=userid,
                mtype=MessageType.Plugin,
                title="【思齐钓鱼】",
                text="已有任务正在执行，请等待当前任务结束。",
            )
            return
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "command_run_once",
            self.run_fishing_task,
            "思齐钓鱼命令立即运行",
            delay_seconds=1,
        )
        self.post_message(
            channel=channel,
            userid=userid,
            mtype=MessageType.Plugin,
            title="【思齐钓鱼】",
            text="任务已开始，完成后会发送通知。",
        )
