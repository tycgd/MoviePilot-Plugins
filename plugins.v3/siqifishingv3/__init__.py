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
    plugin_version = "1.0.0"
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
        """详情页：Hero 概览、次数进度、统计瓦片、鱼饵库存、记录与配置摘要。"""
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
                                "text": "可在配置页随时修改",
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

        return {
            "component": "div",
            "props": {
                "style": (
                    "border-radius: 16px; padding: 22px 24px; margin-bottom: 16px;"
                    " background: linear-gradient(135deg, #0ea5e9 0%, #2563eb 55%, #1e3a8a 100%);"
                    " color: #ffffff; box-shadow: 0 8px 24px rgba(37, 99, 235, 0.25);"
                )
            },
            "content": [
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
                            "props": {
                                "style": "font-size: 44px; font-weight: 800; line-height: 1;"
                            },
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
            ],
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

    async def __execute(self, result: Dict[str, Any]) -> None:
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

        if self._auto_guess:
            await self.__submit_guess(client)

        await self.__ensure_lure(client)
        await self.__ensure_rod(client)

        cast_result = await self.__cast_loop(client)
        result["data"]["casts"] = cast_result["casts"]
        result["data"]["caught"] = cast_result["caught"]
        result["data"]["new_discoveries"] = cast_result["new_discoveries"]
        result["data"]["misses"] = cast_result["misses"]
        result["data"]["miss_rewards"] = cast_result["miss_rewards"]
        result["data"]["spent"] = self.__safe_int(self._daily.get("spent"), 0)

        if self._auto_task:
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

    async def __cast_loop(self, client: AsyncRequestUtils) -> Dict[str, Any]:
        casts = 0
        caught = 0
        new_discoveries = 0
        misses = 0
        miss_rewards = 0
        status = "completed"
        failure = ""

        while casts < self._max_casts_per_run:
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
                self._max_casts_per_run - casts,
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

            if self._cast_interval > 0:
                await asyncio.sleep(self._cast_interval)

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

    async def __ensure_lure(self, client: AsyncRequestUtils) -> None:
        """按策略补饵并选中本次要用的鱼饵。"""
        lures = self._state.get("lures") or {}
        key = self.__pick_lure(lures, self._lure_mode, self._lure_key)

        # 固定策略补固定饵，其它策略补指定的补货饵种；关闭补饵或预算为零则不买
        buy_key = self._lure_key if self._lure_mode == "fixed" else self._buy_lure_key
        price = self.__safe_int(self.LURE_META.get(buy_key, {}).get("price"), 0)
        quantity = 0
        if self._auto_buy and self._buy_budget_per_day > 0:
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
