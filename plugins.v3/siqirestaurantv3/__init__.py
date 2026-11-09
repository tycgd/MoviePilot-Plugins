"""思齐开餐馆 V3 插件：自动开店、按顾客需求采购食材与上菜、员工雇佣、每日下馆子。"""

import asyncio
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


class SiQiRestaurantV3(_PluginBase):
    """思齐站点开餐馆小游戏的 V3 自动化插件。"""

    plugin_name = "思齐开餐馆"
    plugin_desc = "思齐站点开餐馆小游戏自动化：自动开店、按顾客需求采购食材与上菜、员工雇佣、每日下馆子，策略全部可配置。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.0"
    plugin_author = "tycgd"
    author_url = "https://github.com/tycgd"
    plugin_config_prefix = "siqirestaurantv3_"
    plugin_order = 32
    auth_level = 1

    SITE_DOMAIN = "si-qi.xyz"
    BASE_URL = "https://si-qi.xyz"
    GAME_URL = f"{BASE_URL}/siqi_restaurant.php"
    UA = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
    )
    TZ_CN = timezone(timedelta(hours=8))

    DAILY_HISTORY_LIMIT = 30
    MAX_INTERVAL_MINUTES = 1440
    POLL_SECONDS = 3
    MAX_QUICK_SERVES = 3

    # 详情页允许直接修改的选项；其余配置只能在配置页调整
    OPTION_WHITELIST: Dict[str, Dict[str, Any]] = {
        "dry_run": {"type": "bool", "label": "演练模式"},
        "auto_start_session": {"type": "bool", "label": "自动开店"},
        "auto_purchase": {"type": "bool", "label": "自动采购食材"},
        "buy_budget_per_day": {
            "type": "int",
            "label": "每日采购预算",
            "min": 0,
            "max": 10_000_000,
        },
        "serve_strategy": {
            "type": "enum",
            "label": "上菜策略",
            "values": ("match", "cheapest", "expensive"),
        },
        "min_serve_delay_seconds": {
            "type": "int",
            "label": "上菜间隔秒数",
            "min": 0,
            "max": 600,
        },
        "auto_hire_staff": {"type": "bool", "label": "自动雇佣员工"},
        "auto_visit": {"type": "bool", "label": "自动下馆子"},
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
        "visit_targets",
        "max_iterations",
        "max_customers_per_run",
        "keep_ingredient_stock",
    )

    _enabled = False
    _notify = True
    _cookie = ""
    _schedule_mode = "interval"
    _cron = "7 */2 * * *"
    _interval_minutes = 30

    _dry_run = False
    _auto_start_session = True
    _auto_purchase = True
    _buy_budget_per_day = 5000
    _keep_ingredient_stock = 3
    _serve_strategy = "match"
    _min_serve_delay_seconds = 0
    _max_customers_per_run = 0
    _max_iterations = 60
    _auto_hire_staff = True
    _auto_visit = False
    _visit_targets = ""
    _visit_count_limit = 3

    ONCE_JOB_IDS = ("run_once", "api_run_once", "command_run_once")

    _lock: Optional[asyncio.Lock] = None
    _daily: Dict[str, Any] = {
        "date": "",
        "sessions": 0,
        "customers_served": 0,
        "customers_left": 0,
        "magic_spent": 0,
        "magic_earned": 0,
        "dishes": {},
        "visits": 0,
        "last_result": "",
        "history": [],
    }
    _state: Dict[str, Any] = {
        "updated_at": None,
        "popularity": 0,
        "max_popularity": 0,
        "magic": 0,
        "cur_date": "",
        "session_status": "ready",
        "customers_total": 0,
        "customers_served": 0,
        "customers_remaining": 0,
        "queued_customers": 0,
        "earnings_today": 0,
        "net_earnings_today": 0,
        "net_earnings_total": 0,
        "customer_plan": [],
        "unlocked": {},
        "dishes": {},
        "ingredient_meta": {},
        "ingredients": {},
        "staff": {},
        "visits": {},
        "logs": [],
        "last_message": "",
        "staff_snapshot": {},
        "inventory_snapshot": {},
    }

    def init_plugin(self, config: dict = None):
        """读取配置与持久化状态；允许重复调用，不在此处发起网络请求。"""
        config = config or {}
        site_cookie = self.__get_site_cookie()

        self._enabled = bool(config.get("enabled", False))
        self._notify = bool(config.get("notify", True))
        self._cookie = (config.get("cookie") or site_cookie or "").strip()
        self._schedule_mode = config.get("schedule_mode", "interval")
        self._cron = (config.get("cron") or "7 */2 * * *").strip()
        self._interval_minutes = min(
            self.MAX_INTERVAL_MINUTES,
            self.__safe_int(config.get("interval_minutes"), 30, min_value=1),
        )

        self._dry_run = bool(config.get("dry_run", False))
        self._auto_start_session = bool(config.get("auto_start_session", True))
        self._auto_purchase = bool(config.get("auto_purchase", True))
        self._buy_budget_per_day = max(
            0, self.__safe_int(config.get("buy_budget_per_day"), 5000)
        )
        self._keep_ingredient_stock = max(
            0, self.__safe_int(config.get("keep_ingredient_stock"), 3)
        )
        strategy = str(config.get("serve_strategy") or "match").strip().lower()
        if strategy not in ("match", "cheapest", "expensive"):
            strategy = "match"
        self._serve_strategy = strategy
        self._min_serve_delay_seconds = min(
            600, self.__safe_int(config.get("min_serve_delay_seconds"), 0, min_value=0)
        )
        self._max_customers_per_run = max(
            0, self.__safe_int(config.get("max_customers_per_run"), 0)
        )
        self._max_iterations = min(
            600, self.__safe_int(config.get("max_iterations"), 60, min_value=1)
        )
        self._auto_hire_staff = bool(config.get("auto_hire_staff", True))
        self._auto_visit = bool(config.get("auto_visit", False))
        self._visit_targets = str(config.get("visit_targets") or "").strip()
        self._visit_count_limit = min(
            3, self.__safe_int(config.get("visit_count_limit"), 3, min_value=0)
        )

        saved_daily = self.get_data("daily") or {}
        self._daily = {
            "date": saved_daily.get("date") or "",
            "sessions": self.__safe_int(saved_daily.get("sessions"), 0),
            "customers_served": self.__safe_int(
                saved_daily.get("customers_served"), 0
            ),
            "customers_left": self.__safe_int(saved_daily.get("customers_left"), 0),
            "magic_spent": self.__safe_int(saved_daily.get("magic_spent"), 0),
            "magic_earned": self.__safe_int(saved_daily.get("magic_earned"), 0),
            "dishes": dict(saved_daily.get("dishes") or {}),
            "visits": self.__safe_int(saved_daily.get("visits"), 0),
            "last_result": saved_daily.get("last_result") or "",
            "history": list(saved_daily.get("history") or [])[
                -self.DAILY_HISTORY_LIMIT:
            ],
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
                    self.run_restaurant_task,
                    "思齐开餐馆立即运行",
                    delay_seconds=1,
                )
            except Exception as e:
                logger.warning(f"思齐开餐馆：注册立即运行任务失败：{e}")

        logger.info(
            f"思齐开餐馆：{'已启用' if self._enabled else '未启用'}，"
            f"策略 {self._serve_strategy}，每日采购预算 {self._buy_budget_per_day}"
        )

    def get_state(self) -> bool:
        """返回插件是否启用。"""
        return bool(self._enabled)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回远程命令定义：/siqi_restaurant 触发一次任务。"""
        return [
            {
                "cmd": "/siqi_restaurant",
                "event": EventType.PluginAction,
                "desc": "立即执行一次思齐开餐馆",
                "category": "站点",
                "data": {"action": "siqi_restaurant"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """返回详情页/外部可调用的插件 API 列表（全部三键外壳）。"""
        return [
            {
                "path": "/run",
                "endpoint": self.run_once_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即执行一次开餐馆任务",
                "description": "提交后台一次性任务，完成后发送通知",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "查询开餐馆状态",
                "description": "返回今日统计与运行态快照",
            },
            {
                "path": "/serve_now",
                "endpoint": self.api_serve_now,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "页面快捷上菜",
                "description": "同步执行少量上菜；演练模式下仅同步状态",
            },
            {
                "path": "/sync",
                "endpoint": self.api_sync,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "仅同步状态",
                "description": "只抓取状态并刷新运行态，不调用任何写操作",
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
                "path": "/visit",
                "endpoint": self.api_visit,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即下馆子",
                "description": "按目标名单下馆子，受每日上限约束",
            },
            {
                "path": "/hire",
                "endpoint": self.api_hire,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即雇佣员工",
                "description": "员工系统解锁后按缺口与魔力预算雇佣",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """按调度模式返回周期服务；未启用或配置非法时返回空列表。"""
        if not self._enabled:
            return []

        if self._schedule_mode == "interval":
            try:
                trigger = IntervalTrigger(minutes=self._interval_minutes)
                logger.info(f"思齐开餐馆：使用间隔触发器，每 {self._interval_minutes} 分钟")
            except Exception as e:
                logger.warning(f"思齐开餐馆：间隔触发器创建失败：{e}")
                return []
        else:
            if not self._cron:
                return []
            try:
                trigger = CronTrigger.from_crontab(self._cron)
                logger.info(f"思齐开餐馆：使用 Cron 触发器 {self._cron}")
            except ValueError as e:
                logger.warning(f"思齐开餐馆：Cron 配置无效：{e}")
                return []

        return [
            {
                "id": self.__class__.__name__,
                "name": self.plugin_name,
                "trigger": trigger,
                "func": self.run_restaurant_task,
                "kwargs": {},
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """构造配置页表单与默认值模型。"""
        site_cookie = self.__get_site_cookie()
        cookie_value = self._cookie or site_cookie or ""
        strategy_items = [
            {"title": "按顾客指定（推荐）", "value": "match"},
            {"title": "优先便宜", "value": "cheapest"},
            {"title": "优先贵价", "value": "expensive"},
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
                            "text": "每日只能开一次店；食材在上菜时扣除，未上菜不扣；耐心耗尽顾客离开会扣受欢迎值。全部选项仅影响自动化行为，随时可改。",
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
                                        "max": 1440,
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
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "dry_run",
                                        "label": "演练模式",
                                        "hint": "只同步状态，不发生任何写操作",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "auto_start_session",
                                        "label": "自动开店",
                                        "hint": "有顾客时自动开店营业",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "auto_purchase",
                                        "label": "自动采购食材",
                                        "hint": "按顾客需求与保底库存买菜",
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
                                        "label": "每日采购预算",
                                        "type": "number",
                                        "min": 0,
                                        "hint": "魔力值，0 表示不花钱",
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
                                        "model": "serve_strategy",
                                        "label": "上菜策略",
                                        "items": strategy_items,
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "min_serve_delay_seconds",
                                        "label": "上菜间隔(秒)",
                                        "type": "number",
                                        "min": 0,
                                        "max": 600,
                                        "hint": "两次上菜之间的等待",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "keep_ingredient_stock",
                                        "label": "食材保底库存",
                                        "type": "number",
                                        "min": 0,
                                        "hint": "低于该值才补货",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "max_customers_per_run",
                                        "label": "单次最多上菜",
                                        "type": "number",
                                        "min": 0,
                                        "hint": "0 表示不限",
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
                                        "model": "auto_hire_staff",
                                        "label": "自动雇佣员工",
                                        "hint": "受欢迎值达标解锁后生效",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "auto_visit",
                                        "label": "自动下馆子",
                                        "hint": "按目标名单每日下馆子",
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "visit_count_limit",
                                        "label": "每日下馆子上限",
                                        "type": "number",
                                        "min": 0,
                                        "max": 3,
                                    },
                                },
                            ),
                            self.__form_col(
                                12,
                                3,
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "max_iterations",
                                        "label": "单次最大轮次",
                                        "type": "number",
                                        "min": 1,
                                        "max": 600,
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
                                        "model": "visit_targets",
                                        "label": "下馆子目标名单",
                                        "placeholder": "留空则读取站点可拜访列表；多个用户名用逗号分隔",
                                        "hint": "仅自动下馆子开启时生效",
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
            "dry_run": self._dry_run,
            "auto_start_session": self._auto_start_session,
            "auto_purchase": self._auto_purchase,
            "buy_budget_per_day": self._buy_budget_per_day,
            "keep_ingredient_stock": self._keep_ingredient_stock,
            "serve_strategy": self._serve_strategy,
            "min_serve_delay_seconds": self._min_serve_delay_seconds,
            "max_customers_per_run": self._max_customers_per_run,
            "max_iterations": self._max_iterations,
            "auto_hire_staff": self._auto_hire_staff,
            "auto_visit": self._auto_visit,
            "visit_targets": self._visit_targets,
            "visit_count_limit": self._visit_count_limit,
            "run_once": False,
        }

    def get_page(self) -> List[dict]:
        """详情页：Hero 概览、今日概览、快捷操作、策略调节、经营记录与策略摘要。"""
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

        if self._schedule_mode == "interval":
            schedule_info = f"每 {self._interval_minutes} 分钟"
        else:
            schedule_info = f"Cron: {self._cron}"

        served_today = max(0, self.__safe_int(state.get("customers_served"), 0))
        total_today = max(0, self.__safe_int(state.get("customers_total"), 0))
        return [
            self.__hero_card(
                state,
                served_today,
                total_today,
                schedule_info,
                updated_at,
                daily.get("last_result", "") or state.get("last_message", ""),
            ),
            self.__today_overview_card(),
            self.__quick_actions_card(),
            self.__strategy_tuning_card(),
            self.__log_table(state.get("logs") or daily.get("history") or []),
            self.__strategy_summary_card(),
        ]

    def stop_service(self):
        """移除全部一次性任务并释放并发锁，可重复安全调用。"""
        for job_id in self.ONCE_JOB_IDS:
            try:
                scheduler_sdk.remove_plugin_once_job(self.__class__.__name__, job_id)
            except Exception as e:
                logger.debug(f"思齐开餐馆：移除一次性任务 {job_id} 失败：{e}")
        self._lock = None

    # ------------------------------------------------------------------ #
    # 表单 / 页面构造辅助
    # ------------------------------------------------------------------ #
    @staticmethod
    def __form_col(cols: int, md: int, component: Dict[str, Any]) -> Dict[str, Any]:
        """把单个组件包进一列栅格。"""
        return {
            "component": "VCol",
            "props": {"cols": cols, "md": md},
            "content": [component],
        }

    @staticmethod
    def __tile(icon: str, value: str, label: str) -> Dict[str, Any]:
        """构造今日概览里的单个方砖。"""
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
        """拼出详情页事件可调用的插件 API 相对路径。"""
        return f"plugin/{self.__class__.__name__}/{path}"

    def __btn(
        self,
        text: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        method: str = "post",
        selected: bool = False,
        danger: bool = False,
        disabled: bool = False,
        color: Optional[str] = None,
        variant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """构造四态 VBtn 并绑定插件 API。"""
        if disabled:
            variant, color = "outlined", "#6b7280"
        elif danger:
            variant, color = "tonal", "error"
        elif selected:
            variant, color = "flat", "primary"
        else:
            variant = variant or "outlined"
            color = color or "#475569"
        return {
            "component": "VBtn",
            "props": {
                "size": "small",
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
        self,
        label: str,
        value_text: str,
        buttons: List[dict],
        *,
        last: bool = False,
    ) -> Dict[str, Any]:
        """构造一行「名称 + 当前值 chip + 右对齐按钮组」的选项行。"""
        row_style = (
            "display:flex; align-items:center; gap:8px; flex-wrap:wrap;"
            " padding:9px 0;"
        )
        if not last:
            row_style += " border-bottom:1px dashed rgba(148,163,184,0.25);"
        return {
            "component": "div",
            "props": {"style": row_style},
            "content": [
                {
                    "component": "span",
                    "props": {"style": "flex:0 0 118px; font-size:13px; color:#334155;"},
                    "text": label,
                },
                {
                    "component": "VChip",
                    "props": {"size": "x-small", "variant": "tonal", "color": "primary"},
                    "text": value_text,
                },
                {
                    "component": "div",
                    "props": {
                        "style": (
                            "margin-left:auto; display:flex; flex-wrap:wrap;"
                            " gap:6px; justify-content:flex-end;"
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
        last: bool = False,
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
                    variant="tonal",
                )
            ],
            last=last,
        )

    @staticmethod
    def __dead_span(text: str) -> Dict[str, Any]:
        """不可点击项降级为灰色原生 span。"""
        return {
            "component": "span",
            "props": {"style": "color:#6b7280;font-size:12px;padding:4px 6px;"},
            "text": text,
        }

    @staticmethod
    def __hint_span(text: str) -> Dict[str, Any]:
        """构造行尾的灰色提示文字。"""
        return {
            "component": "span",
            "props": {"style": "font-size:12px; opacity:0.6;"},
            "text": text,
        }

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

    @staticmethod
    def __strategy_row(label: str, value: str) -> Dict[str, Any]:
        """构造策略摘要里的一行「名称 : 取值」。"""
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

    def __hero_card(
        self,
        state: Dict[str, Any],
        served_today: int,
        total_today: int,
        schedule_info: str,
        updated_at: str,
        last_result: str,
    ) -> Dict[str, Any]:
        """Hero 卡：魔力值/受欢迎值/今日顾客/净收益；渐变由原生 div 承载。"""
        pill_style = (
            "display:inline-block; padding:2px 10px; border-radius:999px;"
            " background: rgba(255,255,255,0.18); font-size:12px;"
            " margin-right:6px;"
        )
        magic = max(0, self.__safe_int(state.get("magic"), 0))
        popularity = max(0, self.__safe_int(state.get("popularity"), 0))
        net_earned = self.__safe_int(state.get("net_earnings_today"), 0)
        remaining = max(0, self.__safe_int(state.get("customers_remaining"), 0))

        pills = [
            {"component": "span", "props": {"style": pill_style}, "text": f"⏱ {schedule_info}"},
            {
                "component": "span",
                "props": {"style": pill_style},
                "text": f"🔥 受欢迎 {popularity:,}",
            },
            {
                "component": "span",
                "props": {"style": pill_style},
                "text": f"💰 魔力 {magic:,}",
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
                "text": "🍽️ 思齐开餐馆",
            },
            {
                "component": "div",
                "props": {"style": "font-size: 13px; opacity: 0.9; margin-top: 8px;"},
                "text": "今日顾客（已服务/总数）",
            },
            {
                "component": "div",
                "props": {"style": "font-size: 44px; font-weight: 800; line-height: 1.1;"},
                "text": f"{served_today}/{total_today}",
            },
            {
                "component": "div",
                "props": {"style": "font-size: 13px; opacity: 0.9; margin: 4px 0 10px 0;"},
                "text": f"剩余顾客 {remaining} · 今日净收益 {net_earned:,}",
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
                    " background: linear-gradient(135deg, #22c55e 0%, #16a34a 55%, #166534 100%);"
                    " color: #ffffff; box-shadow: 0 8px 24px rgba(22, 163, 74, 0.25);"
                )
            },
            "content": content,
        }

    def __today_overview_card(self) -> Dict[str, Any]:
        """今日概览卡：状态、剩余顾客、下馆子次数与食材库存。"""
        state = self._state or {}
        daily = self._daily or {}
        status = state.get("session_status") or "ready"
        status_text = {"ready": "未开店", "open": "营业中", "closed": "已打烊"}.get(
            status, str(status)
        )
        status_color = {"open": "success", "closed": "secondary"}.get(status, "primary")

        remaining = max(0, self.__safe_int(state.get("customers_remaining"), 0))
        queued = max(0, self.__safe_int(state.get("queued_customers"), 0))
        visits = self._state.get("visits") or {}
        visit_used = max(0, self.__safe_int(daily.get("visits"), 0))
        visit_limit = max(0, self.__safe_int(visits.get("limit"), self._visit_count_limit))
        visit_remaining = self.__safe_int(visits.get("remaining"), None)
        ingredients = state.get("ingredients") or {}
        ing_total = sum(max(0, self.__safe_int(v, 0)) for v in ingredients.values())
        popularity = max(0, self.__safe_int(state.get("popularity"), 0))
        cur_date = state.get("cur_date") or "未知"

        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center"},
                    "content": [
                        {"component": "span", "text": "📊 今日概览"},
                        {"component": "VSpacer"},
                        {
                            "component": "VChip",
                            "props": {
                                "size": "small",
                                "variant": "tonal",
                                "color": status_color,
                            },
                            "text": status_text,
                        },
                    ],
                },
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VRow",
                            "content": [
                                self.__tile("👥", f"{remaining}", "剩余顾客"),
                                self.__tile("⏳", f"{queued}", "排队顾客"),
                                self.__tile(
                                    "🍽️",
                                    f"{visit_used}/{visit_limit or '?'}",
                                    "今日下馆子",
                                ),
                                self.__tile("🥕", f"{ing_total}", "食材库存"),
                            ],
                        },
                        {
                            "component": "div",
                            "props": {"style": "font-size:12px; color:#6b7280; margin-top:8px;"},
                            "text": (
                                f"受欢迎值 {popularity:,} · 站点剩余下馆子 "
                                f"{visit_remaining if visit_remaining is not None else '?'}"
                                f" · 日期 {cur_date}"
                            ),
                        },
                    ],
                },
            ],
        }

    def __quick_actions_card(self) -> Dict[str, Any]:
        """快捷操作卡：立即运行、少量上菜、仅同步、切换演练模式。"""
        rows = [
            self.__option_row(
                "🚀 立即运行一轮",
                "后台执行",
                [self.__btn("🚀 立即运行一轮", "run", {}, danger=True)],
            ),
            self.__option_row(
                "🍽️ 立即上菜",
                f"最多 {self.MAX_QUICK_SERVES} 位",
                [
                    self.__btn(
                        "演练中·仅同步" if self._dry_run else "🍽️ 立即上菜",
                        "serve_now",
                        {"count": self.MAX_QUICK_SERVES},
                        danger=not self._dry_run,
                        disabled=self._dry_run,
                    )
                ],
            ),
            self.__option_row(
                "🔄 仅同步状态",
                "只刷新数据",
                [
                    self.__btn(
                        "🔄 仅同步状态", "sync", {}, color="info", variant="tonal"
                    )
                ],
            ),
            self.__toggle_row(
                "演练模式",
                "dry_run",
                self._dry_run,
                on_text="开启演练",
                off_text="关闭演练",
                last=True,
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
                            "props": {"size": "small", "variant": "tonal", "color": "info"},
                            "text": "点击后自动刷新页面",
                        },
                    ],
                },
                {"component": "VCardText", "content": rows},
            ],
        }

    @staticmethod
    def __tuning_section(title: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """构造策略调节的子区块：小标题 + 若干选项行。"""
        return {
            "component": "div",
            "props": {"style": "margin-bottom:16px;"},
            "content": [
                {
                    "component": "span",
                    "props": {
                        "style": (
                            "display:block; font-size:14px; font-weight:700;"
                            " color:#334155; margin-bottom:6px;"
                        )
                    },
                    "text": title,
                },
                *rows,
            ],
        }

    def __strategy_tuning_card(self) -> Dict[str, Any]:
        """策略调节卡：常用选项点击即改，数值精调仍回配置页。"""
        strategy_text = {
            "match": "按顾客指定",
            "cheapest": "优先便宜",
            "expensive": "优先贵价",
        }.get(self._serve_strategy, str(self._serve_strategy))
        strategy_buttons = [
            self.__btn(
                {"match": "按顾客指定", "cheapest": "优先便宜", "expensive": "优先贵价"}[mode],
                "set_option",
                {"key": "serve_strategy", "value": mode},
                selected=self._serve_strategy == mode,
            )
            for mode in ("match", "cheapest", "expensive")
        ]

        serve_rows = [
            self.__option_row("上菜策略", strategy_text, strategy_buttons),
            self.__option_row(
                "上菜间隔",
                f"{self._min_serve_delay_seconds} 秒",
                [self.__hint_span("数值请进配置页修改")],
                last=True,
            ),
        ]
        purchase_rows = [
            self.__toggle_row(
                "自动采购",
                "auto_purchase",
                self._auto_purchase,
                on_text="开启采购",
                off_text="关闭采购",
            ),
            self.__option_row(
                "每日预算",
                f"{self._buy_budget_per_day} 魔力",
                [
                    self.__btn(
                        f"{amount}",
                        "set_option",
                        {"key": "buy_budget_per_day", "value": amount},
                        selected=self._buy_budget_per_day == amount,
                    )
                    for amount in (0, 5000, 20000)
                ]
                + [self.__hint_span("自定义预算请进配置页")],
                last=True,
            ),
        ]
        auto_rows = [
            self.__toggle_row(
                "自动开店",
                "auto_start_session",
                self._auto_start_session,
                on_text="开启自动开店",
                off_text="关闭自动开店",
            ),
            self.__toggle_row(
                "自动雇佣",
                "auto_hire_staff",
                self._auto_hire_staff,
                on_text="开启自动雇佣",
                off_text="关闭自动雇佣",
            ),
            self.__toggle_row(
                "自动下馆子",
                "auto_visit",
                self._auto_visit,
                on_text="开启自动下馆子",
                off_text="关闭自动下馆子",
                last=True,
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
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 6},
                                    "content": [
                                        self.__tuning_section("🍽️ 上菜", serve_rows),
                                        self.__tuning_section("🤖 自动化", auto_rows),
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 6},
                                    "content": [
                                        self.__tuning_section("🛒 采购", purchase_rows),
                                    ],
                                },
                            ],
                        }
                    ],
                },
            ],
        }

    def __log_table(self, history: List[Any]) -> Dict[str, Any]:
        """经营记录表：优先展示站点 logs，兼容每日 history。"""
        rows = []
        for item in list(history or [])[-8:][::-1]:
            if not isinstance(item, dict):
                continue
            if "log_date" in item or "customers_served" in item:
                rows.append(
                    {
                        "component": "tr",
                        "content": [
                            {"component": "td", "text": str(item.get("log_date", ""))},
                            {
                                "component": "td",
                                "text": f"{self.__safe_int(item.get('customers_served'), 0)}"
                                f"/{self.__safe_int(item.get('customers_total'), 0)}",
                            },
                            {
                                "component": "td",
                                "text": str(item.get("popularity_change", "")),
                            },
                            {"component": "td", "text": str(item.get("net_earnings", ""))},
                        ],
                    }
                )
            else:
                rows.append(
                    {
                        "component": "tr",
                        "content": [
                            {"component": "td", "text": str(item.get("time", ""))},
                            {"component": "td", "text": str(item.get("action", ""))},
                            {"component": "td", "text": str(item.get("detail", ""))},
                            {"component": "td", "text": ""},
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
                            "props": {"colspan": 4},
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
                        {"component": "span", "text": "🗒️ 经营记录"},
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
                                                {"component": "th", "text": "日期"},
                                                {"component": "th", "text": "顾客(服务/总)"},
                                                {"component": "th", "text": "受欢迎变化"},
                                                {"component": "th", "text": "净收益"},
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

    def __strategy_summary_card(self) -> Dict[str, Any]:
        """当前策略摘要卡：只读展示，数值类选项请进配置页。"""
        strategy_text = {
            "match": "按顾客指定",
            "cheapest": "优先便宜",
            "expensive": "优先贵价",
        }.get(self._serve_strategy, str(self._serve_strategy))
        return {
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
                        self.__strategy_row("上菜策略", strategy_text),
                        self.__strategy_row(
                            "自动采购",
                            f"预算 {self._buy_budget_per_day} 魔力"
                            if self._auto_purchase
                            else "不自动采购",
                        ),
                        self.__strategy_row(
                            "食材保底", f"{self._keep_ingredient_stock} 份"
                        ),
                        self.__strategy_row(
                            "自动开店",
                            "有顾客时自动开店" if self._auto_start_session else "不自动开店",
                        ),
                        self.__strategy_row(
                            "自动雇佣", "解锁后自动雇佣" if self._auto_hire_staff else "不自动雇佣"
                        ),
                        self.__strategy_row(
                            "自动下馆子",
                            f"每日至多 {self._visit_count_limit} 次"
                            if self._auto_visit
                            else "不自动下馆子",
                        ),
                        self.__strategy_row(
                            "演练模式", "开启（只同步不写操作）" if self._dry_run else "关闭"
                        ),
                    ],
                },
            ],
        }

    # ------------------------------------------------------------------ #
    # API 接口
    # ------------------------------------------------------------------ #
    async def run_once_api(self) -> Dict[str, Any]:
        """API：提交一次后台开餐馆任务，不阻塞请求。"""
        if self.__get_lock().locked():
            return self.__api_response(
                False, "已有开餐馆任务正在执行", {"status": "running"}
            )
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "api_run_once",
            self.run_restaurant_task,
            "思齐开餐馆API立即运行",
            delay_seconds=1,
        )
        return self.__api_response(True, "任务已提交，完成后会发送通知")

    async def api_status(self) -> Dict[str, Any]:
        """API：返回今日统计与运行态快照。"""
        return self.__api_response(
            True, "已获取状态", {"daily": self._daily, "state": self._state}
        )

    async def api_serve_now(
        self, payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """API：页面快捷上菜；同步执行，响应为三键外壳。"""
        payload = payload if isinstance(payload, dict) else {}
        count = min(
            self.MAX_QUICK_SERVES,
            max(1, self.__safe_int(payload.get("count"), 3, min_value=1)),
        )
        lock = self.__get_lock()
        if lock.locked():
            logger.warning("思齐开餐馆：快捷上菜被跳过，已有任务执行中")
            return self.__api_response(
                False, "已有开餐馆任务正在执行，请稍后再试", {"status": "running"}
            )

        async with lock:
            result = self.__new_result()
            try:
                await self.__execute(
                    result,
                    serves_limit=count,
                    allow_visit=False,
                    allow_hire=False,
                    allow_start=False,
                )
            except httpx2.TimeoutException:
                result["status"] = "error"
                result["message"] = "请求超时"
                logger.error("思齐开餐馆：快捷上菜请求超时")
            except httpx2.RequestError as e:
                result["status"] = "error"
                result["message"] = f"请求异常：{e}"
                logger.error(f"思齐开餐馆：快捷上菜请求异常：{e}")
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"执行异常：{e}"
                logger.error(f"思齐开餐馆：快捷上菜执行异常：{e}")

            self.__finish_task(result)

        status = str(result.get("status") or "")
        data = result.get("data")
        data = dict(data) if isinstance(data, dict) else {}
        data["status"] = status
        data["timestamp"] = result.get("timestamp")
        return self.__api_response(status == "completed", result.get("message"), data)

    async def api_sync(self) -> Dict[str, Any]:
        """API：只同步站点状态，绝不调用任何写操作。"""
        lock = self.__get_lock()
        if lock.locked():
            return self.__api_response(
                False, "已有开餐馆任务正在执行，请稍后再试", {"status": "running"}
            )

        async with lock:
            try:
                cookie = self._cookie or self.__get_site_cookie()
                if not cookie or "c_secure_pass" not in cookie:
                    return self.__api_response(
                        False,
                        "缺少包含 c_secure_pass 的思齐 Cookie，请检查配置或站点管理",
                    )
                self._daily = self.__roll_daily(
                    self._daily, datetime.now(self.TZ_CN).strftime("%Y-%m-%d")
                )
                client = self.__build_client(cookie, self.__game_headers(), 20)
                data = await self.__fetch_state(client)
                if not data or not data.get("success"):
                    return self.__api_response(
                        False, "无法读取餐馆状态，请检查 Cookie 是否失效"
                    )
                self.__apply_state(data)
                self.__sync_daily_date(self._state.get("cur_date"))
                self.__record("sync", "手动同步状态")
                self.__save_state()
            except httpx2.TimeoutException:
                logger.error("思齐开餐馆：同步状态请求超时")
                return self.__api_response(False, "请求超时")
            except httpx2.RequestError as e:
                logger.error(f"思齐开餐馆：同步状态请求异常：{e}")
                return self.__api_response(False, f"请求异常：{e}")
            except Exception as e:
                logger.error(f"思齐开餐馆：同步状态执行异常：{e}")
                return self.__api_response(False, f"同步失败：{e}")

        state = self._state or {}
        return self.__api_response(
            True,
            "已同步状态",
            {
                "popularity": state.get("popularity"),
                "session_status": state.get("session_status"),
                "customers_served": state.get("customers_served"),
                "customers_total": state.get("customers_total"),
                "customers_remaining": state.get("customers_remaining"),
                "updated_at": state.get("updated_at"),
            },
        )

    async def api_set_option(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """API：页面修改白名单选项，整体回写配置并同步内存属性。"""
        payload = payload if isinstance(payload, dict) else {}
        key = str(payload.get("key") or "").strip()
        value = payload.get("value")
        ok, normalized, label = self.__normalize_option(key, value)
        if not ok:
            logger.warning(f"思齐开餐馆：页面修改选项被拒绝：{label}")
            return self.__api_response(False, label, {"key": key})

        try:
            config = dict(self.get_config() or {})
        except Exception as e:
            logger.error(f"思齐开餐馆：读取配置失败，未保存：{e}")
            return self.__api_response(False, "读取配置失败，未保存", {"key": key})
        updates = (
            dict(normalized) if isinstance(normalized, dict) else {key: normalized}
        )
        config.update(updates)
        try:
            self.update_config(config)
        except Exception as e:
            logger.error(f"思齐开餐馆：保存选项失败：{e}")
            return self.__api_response(False, f"保存失败：{e}", {"key": key})

        for field, field_value in updates.items():
            setattr(self, f"_{field}", field_value)
        logger.info(f"思齐开餐馆：页面已更新{label}")
        return self.__api_response(
            True, f"已更新{label}", {"key": key, "value": normalized}
        )

    async def api_visit(self) -> Dict[str, Any]:
        """API：手动触发一次下馆子。"""
        lock = self.__get_lock()
        if lock.locked():
            return self.__api_response(
                False, "已有开餐馆任务正在执行，请稍后再试", {"status": "running"}
            )
        async with lock:
            try:
                cookie = self._cookie or self.__get_site_cookie()
                if not cookie or "c_secure_pass" not in cookie:
                    return self.__api_response(
                        False,
                        "缺少包含 c_secure_pass 的思齐 Cookie，请检查配置或站点管理",
                    )
                client = self.__build_client(cookie, self.__game_headers(), 20)
                data = await self.__fetch_state(client)
                if not data or not data.get("success"):
                    return self.__api_response(False, "无法读取餐馆状态，请检查 Cookie 是否失效")
                self.__apply_state(data)
                done = await self.__do_visits(client)
                self.__save_state()
            except httpx2.TimeoutException:
                return self.__api_response(False, "请求超时")
            except httpx2.RequestError as e:
                return self.__api_response(False, f"请求异常：{e}")
            except Exception as e:
                logger.error(f"思齐开餐馆：下馆子执行异常：{e}")
                return self.__api_response(False, f"下馆子失败：{e}")
        return self.__api_response(True, f"已下馆子 {done} 次", {"visited": done})

    async def api_hire(self) -> Dict[str, Any]:
        """API：手动触发一次员工雇佣。"""
        lock = self.__get_lock()
        if lock.locked():
            return self.__api_response(
                False, "已有开餐馆任务正在执行，请稍后再试", {"status": "running"}
            )
        async with lock:
            try:
                cookie = self._cookie or self.__get_site_cookie()
                if not cookie or "c_secure_pass" not in cookie:
                    return self.__api_response(
                        False,
                        "缺少包含 c_secure_pass 的思齐 Cookie，请检查配置或站点管理",
                    )
                client = self.__build_client(cookie, self.__game_headers(), 20)
                data = await self.__fetch_state(client)
                if not data or not data.get("success"):
                    return self.__api_response(False, "无法读取餐馆状态，请检查 Cookie 是否失效")
                self.__apply_state(data)
                hired = await self.__do_hiring(client)
                self.__save_state()
            except httpx2.TimeoutException:
                return self.__api_response(False, "请求超时")
            except httpx2.RequestError as e:
                return self.__api_response(False, f"请求异常：{e}")
            except Exception as e:
                logger.error(f"思齐开餐馆：雇佣执行异常：{e}")
                return self.__api_response(False, f"雇佣失败：{e}")
        return self.__api_response(True, f"已雇佣 {hired} 名员工", {"hired": hired})

    # ------------------------------------------------------------------ #
    # 任务主流程
    # ------------------------------------------------------------------ #
    async def run_restaurant_task(self, **kwargs) -> Dict[str, Any]:
        """执行一次开餐馆任务：检查锁、加锁执行、异常分类并收尾。"""
        lock = self.__get_lock()
        if lock.locked():
            return self.__new_result("running", "已有开餐馆任务正在执行")
        async with lock:
            result = self.__new_result()
            try:
                await self.__execute(result)
            except httpx2.TimeoutException:
                result["status"] = "error"
                result["message"] = "请求超时"
                logger.error("思齐开餐馆：请求超时")
            except httpx2.RequestError as e:
                result["status"] = "error"
                result["message"] = f"请求异常：{e}"
                logger.error(f"思齐开餐馆：请求异常：{e}")
            except Exception as e:
                result["status"] = "error"
                result["message"] = f"执行异常：{e}"
                logger.error(f"思齐开餐馆：执行异常：{e}")

            self.__finish_task(result)
            return result

    async def __execute(
        self,
        result: Dict[str, Any],
        *,
        serves_limit: Optional[int] = None,
        allow_purchase: bool = True,
        allow_visit: bool = True,
        allow_hire: bool = True,
        allow_start: bool = True,
    ) -> None:
        """核心流程：取 Cookie、同步状态、开店、下馆子、雇佣、上菜、收尾汇总。"""
        cookie = self._cookie or self.__get_site_cookie()
        if not cookie or "c_secure_pass" not in cookie:
            result["status"] = "auth_failed"
            result["message"] = "缺少包含 c_secure_pass 的思齐 Cookie，请检查配置或站点管理"
            logger.error("思齐开餐馆：Cookie 缺失或格式不正确")
            return

        self._daily = self.__roll_daily(
            self._daily, datetime.now(self.TZ_CN).strftime("%Y-%m-%d")
        )
        client = self.__build_client(cookie, self.__game_headers(), 20)

        data = await self.__fetch_state(client)
        if data is None:
            # 无法解析返回内容（页面异常/非 JSON），按执行失败处理
            result["status"] = "failed"
            result["message"] = "无法解析餐馆状态返回内容，请稍后重试"
            logger.error("思齐开餐馆：状态返回无法解析（非 JSON 或空响应）")
            return
        if not data.get("success"):
            result["status"] = "auth_failed"
            result["message"] = "无法读取餐馆状态，请检查 Cookie 是否失效"
            logger.error("思齐开餐馆：状态读取失败，可能 Cookie 失效")
            return

        self.__apply_state(data)
        self.__sync_daily_date(self._state.get("cur_date"))
        result["data"] = self.__result_snapshot()

        if self._dry_run:
            result["status"] = "completed"
            result["message"] = "演练模式：已同步状态，未执行任何写操作"
            self.__record("sync", result["message"])
            return

        if allow_start and self._auto_start_session:
            await self.__ensure_session(client)
            refreshed = await self.__fetch_state(client)
            if refreshed and refreshed.get("success"):
                self.__apply_state(refreshed)

        # N1：执行顺序改为 开店 → 上菜 → 下馆子 → 雇佣
        served = 0
        if (
            self._state.get("session_status") == "open"
            or self.__safe_int(self._state.get("queued_customers"), 0) > 0
        ):
            served = await self.__serve_loop(
                client,
                serves_limit=serves_limit,
                delay_override=None,
                allow_purchase=allow_purchase,
            )

        if allow_visit and self._auto_visit:
            await self.__do_visits(client)
        if allow_hire and self._auto_hire_staff:
            await self.__do_hiring(client)

        try:
            final = await self.__fetch_state(client)
            if final and final.get("success"):
                self.__apply_state(final)
        except Exception as e:
            logger.debug(f"思齐开餐馆：收尾刷新状态失败（不致命）：{e}")

        self._daily["magic_earned"] = max(
            0, self.__safe_int(self._state.get("net_earnings_today"), 0)
        )
        summary = self.__build_summary(served)
        # P1-3：收尾状态分级：remaining<=0 → completed；served>0 → partial；否则 failed
        remaining = self.__safe_int(self._state.get("customers_remaining"), 0)
        if remaining <= 0:
            status = "completed"
        elif served > 0:
            status = "partial"
            summary += f"；停止原因：仍有 {remaining} 位顾客未上菜（部分完成）"
        else:
            status = "failed"
            summary += f"；停止原因：{self.__failure_reason()}"
        result["status"] = status
        result["message"] = summary
        self._daily["last_result"] = summary
        result["data"].update(self.__result_snapshot())
        result["data"]["served"] = served
        self.__record("run", summary)

    def __failure_reason(self) -> str:
        """推断本次 0 上菜的失败原因，用于收尾 message。"""
        if self._state.get("session_status") != "open":
            return "开店失败或未进入营业状态"
        history = self._daily.get("history") or []
        if any(isinstance(r, dict) and r.get("action") == "fail" for r in history):
            return "服务端拒绝上菜"
        if any(isinstance(r, dict) and r.get("action") == "skip" for r in history):
            return "食材不足或无可上菜顾客"
        return "无可上菜顾客或未产生有效上菜"

    async def __ensure_session(self, client: AsyncRequestUtils) -> None:
        """确保今日已开店：仅在 ready 且今日有顾客时开店；已开店不视为失败。"""
        status = self._state.get("session_status")
        total = self.__safe_int(self._state.get("customers_total"), 0)
        if status != "ready" or total <= 0:
            return
        payload = await self.__post_action(client, "start_session")
        if self.__game_ok(payload):
            self._daily["sessions"] = self.__safe_int(self._daily.get("sessions"), 0) + 1
            self.__record("open", "已开启今日营业")
            self.__apply_state(payload)
            return
        refreshed = await self.__fetch_state(client)
        if refreshed and refreshed.get("success"):
            self.__apply_state(refreshed)
        if self._state.get("session_status") == "open":
            # 服务端返回已开店，不视为失败
            return
        message = payload.get("message") or "开店失败"
        self.__record("warn", f"{message}")
        logger.warning(f"思齐开餐馆：开店未成功：{message}")

    async def __serve_loop(
        self,
        client: AsyncRequestUtils,
        *,
        serves_limit: Optional[int] = None,
        delay_override: Optional[int] = None,
        allow_purchase: bool = True,
    ) -> int:
        """循环上菜：处理过期顾客、按需采购、按截止时间上菜，直到无顾客或达上限。"""
        served_total = 0
        if serves_limit is not None:
            run_cap: Optional[int] = max(1, self.__safe_int(serves_limit, 1))
        elif self._max_customers_per_run > 0:
            run_cap = self._max_customers_per_run
        else:
            run_cap = None
        delay = (
            self._min_serve_delay_seconds
            if delay_override is None
            else self.__safe_int(delay_override, 0)
        )
        max_iterations = max(1, self._max_iterations)

        for _ in range(max_iterations):
            now_ts = time.time()

            # a) 处理已过期顾客
            expired = self.__expired_customer_ids(
                self._state.get("customer_plan") or [], now_ts
            )
            for customer_id in expired:
                payload = await self.__post_action(
                    client, "customer_left", customer_id=customer_id
                )
                if self.__game_ok(payload):
                    self._daily["customers_left"] = (
                        self.__safe_int(self._daily.get("customers_left"), 0) + 1
                    )
                    self.__record("left", f"顾客 {customer_id} 等待超时离开")
                    self.__mark_plan_status(customer_id, "left")
                    self.__apply_state(payload)

            plan = self._state.get("customer_plan") or []

            # b) 按需求采购食材
            purchase_spent = 0
            if allow_purchase:
                purchase_spent = await self.__purchase_ingredients(client, plan)

            # c) 按截止时间升序上菜（N1：单个失败不中断整轮，continue 尝试其余顾客）
            dishes = self._state.get("dishes") or {}
            waiting = [
                c
                for c in plan
                if isinstance(c, dict) and c.get("status") == "waiting"
            ]
            waiting.sort(key=lambda c: self.__deadline_key(c))
            served_this_round = 0
            attempted_ids: set = set()
            for customer in waiting:
                customer_id = customer.get("id")
                if customer_id in attempted_ids:
                    continue
                if run_cap is not None and served_total >= run_cap:
                    break
                # P2-9：选菜接入 serve_strategy；有 dish_key 时按 match 保持不变
                ingredients = self._state.get("ingredients") or {}
                original_key = customer.get("dish_key")
                if original_key:
                    chosen_key = str(original_key)
                else:
                    chosen_key = self.__choose_dish(
                        customer, self._serve_strategy, dishes, ingredients
                    )
                    if chosen_key:
                        self.__record(
                            "strategy",
                            f"顾客 {customer_id} 未指定菜谱，按 {self._serve_strategy} 选择 {chosen_key}",
                        )
                        logger.debug(
                            f"思齐开餐馆：顾客 {customer_id} 未指定菜谱，"
                            f"按策略 {self._serve_strategy} 选择 {chosen_key}"
                        )
                dish_key = chosen_key
                dish = dishes.get(dish_key)
                if not isinstance(dish, dict):
                    self.__record("skip", f"未知菜谱 {dish_key}，跳过顾客")
                    attempted_ids.add(customer_id)
                    continue
                missing = self.__missing_ingredients(dish, ingredients)
                if missing:
                    self.__record("skip", f"食材不足，跳过顾客 {customer_id}")
                    attempted_ids.add(customer_id)
                    continue
                payload = await self.__post_action(
                    client,
                    "serve_customer",
                    customer_id=customer_id,
                    dish_key=dish_key,
                )
                if self.__game_ok(payload):
                    served_total += 1
                    served_this_round += 1
                    self._daily["customers_served"] = (
                        self.__safe_int(self._daily.get("customers_served"), 0) + 1
                    )
                    dishes_count = self._daily.get("dishes")
                    if not isinstance(dishes_count, dict):
                        dishes_count = {}
                        self._daily["dishes"] = dishes_count
                    dishes_count[dish_key] = (
                        self.__safe_int(dishes_count.get(dish_key), 0) + 1
                    )
                    self.__record("serve", f"上菜 {dish_key} 给顾客 {customer_id}")
                    self.__mark_plan_status(customer_id, "served")
                    # P1-2：成功上菜后本地扣减食材，避免同轮同一食材被重复使用
                    self.__deduct_ingredients(dish)
                    self.__apply_state(payload)
                    if delay > 0:
                        await asyncio.sleep(delay)
                else:
                    message = payload.get("message") or "上菜失败"
                    self.__record("fail", f"顾客 {customer_id} 上菜失败：{message}")
                    logger.warning(
                        f"思齐开餐馆：顾客 {customer_id} 上菜失败：{message}"
                    )
                    attempted_ids.add(customer_id)
                    continue

            # d) 每轮重 fetch 并判断是否结束
            refreshed = await self.__fetch_state(client)
            if refreshed is None:
                logger.warning("思齐开餐馆：上菜循环刷新状态失败，结束本轮")
                break
            self.__apply_state(refreshed)
            remaining = self.__safe_int(self._state.get("customers_remaining"), 0)
            queued = self.__safe_int(self._state.get("queued_customers"), 0)
            status = self._state.get("session_status")

            if run_cap is not None and served_total >= run_cap:
                break
            if remaining <= 0 and queued <= 0:
                break
            if status == "closed" and queued <= 0:
                break
            if served_this_round == 0 and purchase_spent == 0:
                current_plan = self._state.get("customer_plan") or []
                has_waiting = any(
                    isinstance(c, dict) and c.get("status") == "waiting"
                    for c in current_plan
                )
                if has_waiting:
                    # 有顾客但既买不起也上不了菜，停止以免空转
                    break
                if queued > 0 or remaining > 0:
                    await asyncio.sleep(self.POLL_SECONDS)
                    continue
                break

        return served_total

    def __deduct_ingredients(self, dish: Dict[str, Any]) -> None:
        """上菜成功后按菜谱配方在本地扣减食材库存（不小于 0）。"""
        if not isinstance(dish, dict):
            return
        stock = self._state.get("ingredients")
        if not isinstance(stock, dict):
            stock = {}
            self._state["ingredients"] = stock
        for item in dish.get("ingredients") or []:
            key = None
            qty = 0
            if isinstance(item, dict):
                key = item.get("key")
                qty = self.__safe_int(item.get("quantity"), 0)
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                key = item[0]
                qty = self.__safe_int(item[1], 0)
            if key is None or qty <= 0:
                continue
            text = str(key)
            stock[text] = max(0, self.__safe_int(stock.get(text), 0) - qty)

    async def __purchase_ingredients(
        self, client: AsyncRequestUtils, plan: List[Any]
    ) -> int:
        """根据顾客菜品需求规划并采购食材，受每日预算与保底库存约束，返回花费。"""
        if not self._auto_purchase:
            return 0
        dishes = self._state.get("dishes") or {}
        stock = self.__normalize_ingredients(self._state.get("ingredients") or {})
        dish_demand = self.__dish_demand(plan, dishes)
        ingredient_demand = self.__ingredient_demand(dish_demand, dishes)
        if not ingredient_demand:
            return 0
        prices = {
            key: self.__safe_int(meta.get("price"), 0)
            for key, meta in (self._state.get("ingredient_meta") or {}).items()
            if isinstance(meta, dict)
        }
        plan_list, _total = self.__purchase_plan(
            ingredient_demand,
            stock,
            self._keep_ingredient_stock,
            prices,
            self._buy_budget_per_day,
            self._daily.get("magic_spent"),
        )
        spent_now = 0
        for item in plan_list:
            key = item["ingredient_key"]
            quantity = item["quantity"]
            if quantity <= 0:
                continue
            payload = await self.__post_action(
                client, "purchase_ingredient", ingredient_key=key, quantity=quantity
            )
            if self.__game_ok(payload):
                self._daily["magic_spent"] = (
                    self.__safe_int(self._daily.get("magic_spent"), 0) + item["cost"]
                )
                spent_now += item["cost"]
                self.__record("buy", f"购买食材 {key} × {quantity}")
                # 先记入本地库存，服务端若回传 inventory 会以权威值覆盖
                current = self._state.get("ingredients")
                if not isinstance(current, dict):
                    current = {}
                    self._state["ingredients"] = current
                current[key] = self.__safe_int(current.get(key), 0) + quantity
                self.__apply_state(payload)
        return spent_now

    async def __do_visits(self, client: AsyncRequestUtils) -> int:
        """按目标名单下馆子，受每日上限与站点剩余次数约束，返回完成次数。"""
        visits = self._state.get("visits") or {}
        server_remaining = visits.get("remaining")
        remaining = self.__remaining_visits(
            self.__safe_int(self._daily.get("visits"), 0),
            self._visit_count_limit,
            server_remaining,
        )
        if remaining <= 0:
            return 0

        targets = self.__parse_visit_targets(self._visit_targets)
        if not targets:
            payload = await self.__post_action(
                client, "list_visitable_restaurants", limit=remaining
            )
            targets = self.__extract_visit_targets(payload)
        if not targets:
            return 0

        done = 0
        for name in targets:
            if done >= remaining:
                break
            payload = await self.__post_action(
                client,
                "visit_restaurant",
                target_username=name,
                confirm="yes",
            )
            if self.__game_ok(payload):
                done += 1
                self._daily["visits"] = self.__safe_int(self._daily.get("visits"), 0) + 1
                self.__record("visit", f"下馆子：{name}")
                self.__apply_state(payload)
            else:
                # P2-11：单个目标失败不放弃其余目标
                message = payload.get("message") or "下馆子失败"
                logger.warning(f"思齐开餐馆：下馆子目标 {name} 失败：{message}")
                self.__record("warn", f"下馆子 {name} 失败：{message}")
                continue
        return done

    async def __do_hiring(self, client: AsyncRequestUtils) -> int:
        """按雇佣规划自动雇佣厨师/服务员，返回雇佣数量。"""
        staff = self._state.get("staff") or {}
        # N1：只花一半魔力用于雇佣，避免掏空采购资金
        magic = self.__safe_int(self._state.get("magic"), 0)
        hire_budget = max(0, magic // 2)
        hires = self.__hire_plan(staff, hire_budget)
        hired = 0
        for role, quantity in hires.items():
            if quantity <= 0:
                continue
            payload = await self.__post_action(
                client, "hire_staff", role=role, quantity=quantity
            )
            if self.__game_ok(payload):
                hired += quantity
                label = "厨师" if role == "chef" else "服务员"
                self.__record("hire", f"雇佣{label} × {quantity}")
                self.__apply_state(payload)
        return hired

    def __build_summary(self, served: int) -> str:
        """组装本次运行的中文摘要。"""
        state = self._state or {}
        served_today = self.__safe_int(state.get("customers_served"), 0)
        total_today = self.__safe_int(state.get("customers_total"), 0)
        remaining = self.__safe_int(state.get("customers_remaining"), 0)
        status = state.get("session_status")
        status_cn = {"ready": "未开店", "open": "营业中", "closed": "已打烊"}.get(
            status, str(status or "未知")
        )
        spent = self.__safe_int(self._daily.get("magic_spent"), 0)
        earned = self.__safe_int(state.get("net_earnings_today"), 0)
        return (
            f"本次上菜 {served} 位；今日 {served_today}/{total_today}；"
            f"剩余 {remaining}；状态 {status_cn}；采购花费 {spent}；净收益 {earned}"
        )

    def __result_snapshot(self) -> Dict[str, Any]:
        """从运行态提取用于 result.data 的摘要字段。"""
        state = self._state or {}
        return {
            "popularity": self.__safe_int(state.get("popularity"), 0),
            "magic": self.__safe_int(state.get("magic"), 0),
            "session_status": state.get("session_status"),
            "customers_total": self.__safe_int(state.get("customers_total"), 0),
            "customers_served": self.__safe_int(state.get("customers_served"), 0),
            "customers_remaining": self.__safe_int(state.get("customers_remaining"), 0),
            "magic_spent": self.__safe_int(self._daily.get("magic_spent"), 0),
            "visits": self.__safe_int(self._daily.get("visits"), 0),
        }

    # ------------------------------------------------------------------ #
    # 纯函数（可单测）
    # ------------------------------------------------------------------ #
    @staticmethod
    def __normalize_ingredients(value: Any) -> Dict[str, int]:
        """把食材库存归一为 {key: int}，兼容 dict 与 list（{key,quantity} 或 [key,qty]）。"""
        result: Dict[str, int] = {}
        if isinstance(value, dict):
            for key, qty in value.items():
                if isinstance(qty, dict):
                    qty = qty.get("quantity", qty.get("qty", 0))
                result[str(key)] = max(0, SiQiRestaurantV3.__safe_int(qty, 0))
            return result
        if isinstance(value, list):
            for item in value:
                key = None
                qty: Any = 0
                if isinstance(item, dict):
                    key = item.get("key") or item.get("ingredient_key")
                    qty = item.get("quantity", item.get("qty", 0))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    key = item[0]
                    qty = item[1]
                if key is None:
                    continue
                text = str(key)
                result[text] = result.get(text, 0) + max(
                    0, SiQiRestaurantV3.__safe_int(qty, 0)
                )
        return result

    @staticmethod
    def __normalize_inventory(value: Any) -> Dict[str, int]:
        """兼容旧命名的库存归一化，等价于 __normalize_ingredients。"""
        return SiQiRestaurantV3.__normalize_ingredients(value)

    @staticmethod
    def __unlocked_dishes(
        config_dishes: Any, unlocked_keys: Any
    ) -> List[Dict[str, Any]]:
        """返回已解锁的菜谱元数据列表（按 unlocked 键过滤，保持配置顺序）。"""
        dishes = config_dishes if isinstance(config_dishes, list) else []
        if unlocked_keys is None:
            return [dict(d) for d in dishes if isinstance(d, dict)]
        keys = set(str(k) for k in (unlocked_keys or []))
        result: List[Dict[str, Any]] = []
        for dish in dishes:
            if not isinstance(dish, dict):
                continue
            key = dish.get("key")
            if key and str(key) in keys:
                result.append(dict(dish))
        return result

    @staticmethod
    def __dish_demand(customers: Any, dishes: Any) -> Dict[str, int]:
        """统计待上菜顾客对各菜品的需求量（served/left 不计入；queued 纳入）。"""
        demand: Dict[str, int] = {}
        dishes = dishes if isinstance(dishes, dict) else {}
        for customer in customers or []:
            if not isinstance(customer, dict):
                continue
            status = customer.get("status")
            if status in ("served", "left"):
                continue
            if status not in ("waiting", "queued", None):
                continue
            key = customer.get("dish_key")
            if not key:
                continue
            if dishes and key not in dishes:
                continue
            demand[key] = demand.get(key, 0) + 1
        return demand

    @staticmethod
    def __unique_dish_demand(plan: Any) -> Dict[str, int]:
        """统计顾客计划中各菜品的需求量（兼容旧命名，忽略 served/left）。"""
        demand: Dict[str, int] = {}
        for customer in plan or []:
            if not isinstance(customer, dict):
                continue
            if customer.get("status") in ("served", "left"):
                continue
            key = customer.get("dish_key")
            if key:
                demand[key] = demand.get(key, 0) + 1
        return demand

    @staticmethod
    def __ingredient_demand(
        dish_demand: Dict[str, int], dishes: Dict[str, Any]
    ) -> Dict[str, int]:
        """把菜品需求展开为食材需求（按菜谱配方累加）。"""
        demand: Dict[str, int] = {}
        for dish_key, count in (dish_demand or {}).items():
            dish = (dishes or {}).get(dish_key)
            if not isinstance(dish, dict):
                continue
            count = SiQiRestaurantV3.__safe_int(count, 0)
            if count <= 0:
                continue
            for item in dish.get("ingredients") or []:
                key = None
                qty = 0
                if isinstance(item, dict):
                    key = item.get("key")
                    qty = SiQiRestaurantV3.__safe_int(item.get("quantity"), 0)
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    key = item[0]
                    qty = SiQiRestaurantV3.__safe_int(item[1], 0)
                if key is None or qty <= 0:
                    continue
                text = str(key)
                demand[text] = demand.get(text, 0) + qty * count
        return demand

    @staticmethod
    def __purchase_plan(
        demand: Dict[str, int],
        stock: Dict[str, int],
        keep: Any,
        prices: Dict[str, int],
        budget: int,
        spent: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """根据食材需求、库存、保底与预算规划采购清单与总花费（不超预算）。"""
        plan: List[Dict[str, Any]] = []
        total = 0
        budget_left = max(0, SiQiRestaurantV3.__safe_int(budget, 0) - SiQiRestaurantV3.__safe_int(spent, 0))
        stock = stock if isinstance(stock, dict) else {}
        prices = prices if isinstance(prices, dict) else {}
        for key, need in (demand or {}).items():
            need = SiQiRestaurantV3.__safe_int(need, 0)
            if need <= 0:
                continue
            if isinstance(keep, dict):
                keep_qty = SiQiRestaurantV3.__safe_int(keep.get(key), 0)
            else:
                keep_qty = SiQiRestaurantV3.__safe_int(keep, 0)
            have = SiQiRestaurantV3.__safe_int(stock.get(key), 0)
            target = need + max(0, keep_qty)
            missing = target - have
            if missing <= 0:
                continue
            price = SiQiRestaurantV3.__safe_int(prices.get(key), 0)
            if price <= 0:
                continue
            affordable = budget_left // price
            if affordable <= 0:
                continue
            quantity = min(missing, affordable)
            if quantity <= 0:
                continue
            cost = quantity * price
            plan.append(
                {
                    "ingredient_key": key,
                    "quantity": quantity,
                    "price": price,
                    "cost": cost,
                }
            )
            total += cost
            budget_left -= cost
        return plan, total

    @staticmethod
    def __plan_purchases(
        required: Dict[str, int],
        inventory: Dict[str, int],
        prices: Dict[str, int],
        budget: int,
        keep: Any,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """兼容旧命名的采购规划，等价于 __purchase_plan（spent 视为 0）。"""
        return SiQiRestaurantV3.__purchase_plan(
            required, inventory, keep, prices, budget, 0
        )

    @staticmethod
    def __choose_dish(
        customer: Dict[str, Any], strategy: str, dishes: Dict[str, Any], stock: Dict[str, Any]
    ) -> str:
        """为顾客选择要上的菜品：优先顾客指定的 dish_key（服务端要求，不改变）。"""
        dishes = dishes if isinstance(dishes, dict) else {}
        stock = stock if isinstance(stock, dict) else {}
        key = (customer or {}).get("dish_key")
        if key and (not dishes or key in dishes):
            return str(key)
        # 顾客未指定或菜谱未知时，按策略挑一道库存可满足的已解锁菜
        candidates = [
            dkey
            for dkey, dish in dishes.items()
            if isinstance(dish, dict)
            and not SiQiRestaurantV3.__missing_ingredients(dish, stock)
        ]
        if not candidates:
            return str(key) if key else ""
        if strategy == "expensive":
            return max(
                candidates,
                key=lambda k: SiQiRestaurantV3.__safe_int(dishes[k].get("price"), 0),
            )
        if strategy == "cheapest":
            return min(
                candidates,
                key=lambda k: SiQiRestaurantV3.__safe_int(dishes[k].get("price"), 0),
            )
        return candidates[0]

    @staticmethod
    def __pick_targets(plan: Any, strategy: str, limit: Optional[int]) -> List[Dict[str, Any]]:
        """按策略挑选待上菜顾客（保持服务端要求的 dish_key，仅排序与截断）。"""
        waiting = [
            c
            for c in plan or []
            if isinstance(c, dict) and c.get("status") == "waiting" and c.get("dish_key")
        ]
        waiting.sort(key=lambda c: SiQiRestaurantV3.__deadline_key(c))
        if limit is not None and limit > 0:
            waiting = waiting[:limit]
        return waiting

    @staticmethod
    def __missing_ingredients(dish: Dict[str, Any], stock: Dict[str, Any]) -> Dict[str, int]:
        """返回该菜谱相比现有库存还缺的食材清单 {key: 缺口数量}。"""
        missing: Dict[str, int] = {}
        stock = stock if isinstance(stock, dict) else {}
        dish = dish if isinstance(dish, dict) else {}
        for item in dish.get("ingredients") or []:
            key = None
            qty = 0
            if isinstance(item, dict):
                key = item.get("key")
                qty = SiQiRestaurantV3.__safe_int(item.get("quantity"), 0)
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                key = item[0]
                qty = SiQiRestaurantV3.__safe_int(item[1], 0)
            if key is None or qty <= 0:
                continue
            have = SiQiRestaurantV3.__safe_int(stock.get(key), 0)
            gap = qty - have
            if gap > 0:
                text = str(key)
                missing[text] = missing.get(text, 0) + gap
        return missing

    @staticmethod
    def __deadline_ts(value: Any) -> Optional[float]:
        """把顾客 deadline 统一解析为**秒级**时间戳。

        - 数值/数字字符串：> 1e12 视为毫秒，除以 1000；
        - ISO 字符串：用 datetime.fromisoformat 解析；
        - 无法解析：返回 None（并打 debug 日志）。

        页面 JS 使用 ``Math.floor(Date.now()/1000)``，说明 deadline 为秒级；
        此函数仅作单位兜底防御。
        """
        if value is None or isinstance(value, bool):
            return None
        number: Optional[float] = None
        if isinstance(value, (int, float)):
            number = float(value)
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                number = float(text)
            except ValueError:
                try:
                    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                except ValueError:
                    logger.debug(f"思齐开餐馆：无法解析截止时间 {value!r}")
                    return None
                try:
                    return parsed.timestamp()
                except (OverflowError, OSError, ValueError):
                    logger.debug(f"思齐开餐馆：截止时间转换失败 {value!r}")
                    return None
        else:
            return None
        if number > 1e12:
            number = number / 1000.0
        return number

    @staticmethod
    def __expired_customer_ids(plan: Any, now_ts: Any) -> List[Any]:
        """返回耐心截止时间已过、仍处于 waiting 的顾客 id 列表（queued 不处理）。"""
        try:
            now = float(now_ts)
        except (TypeError, ValueError):
            now = time.time()
        result: List[Any] = []
        for customer in plan or []:
            if not isinstance(customer, dict):
                continue
            if customer.get("status") != "waiting":
                continue
            deadline_ts = SiQiRestaurantV3.__deadline_ts(customer.get("deadline"))
            if deadline_ts is None:
                continue
            if deadline_ts <= now:
                customer_id = customer.get("id")
                if customer_id is not None:
                    result.append(customer_id)
        return result

    @staticmethod
    def __parse_visit_targets(raw: Any) -> List[str]:
        """把逗号/中文逗号/分号/空白分隔的字符串解析为去重后的用户名列表。"""
        if not raw:
            return []
        text = str(raw)
        for sep in ("，", ";", "；", "、", ",", "\n", "\t"):
            text = text.replace(sep, " ")
        result: List[str] = []
        for name in text.split():
            if name and name not in result:
                result.append(name)
        return result

    @staticmethod
    def __remaining_visits(
        daily_visits: int, limit: int, server_remaining: Any
    ) -> int:
        """计算今日还能下馆子几次：limit<=0 表示关闭下馆子，直接返回 0。"""
        limit = SiQiRestaurantV3.__safe_int(limit, 0)
        if limit <= 0:
            return 0
        used = SiQiRestaurantV3.__safe_int(daily_visits, 0)
        plugin_left = max(0, limit - used)
        try:
            server_left: Optional[int] = int(server_remaining)
        except (TypeError, ValueError):
            server_left = None
        if server_left is None:
            return plugin_left
        server_left = max(0, server_left)
        return max(0, min(plugin_left, server_left))

    @staticmethod
    def __hire_plan(staff: Dict[str, Any], budget_magic: Any) -> Dict[str, int]:
        """规划自动雇佣：仅在员工系统已解锁时，按容量缺口与魔力预算给出 {角色: 数量}。"""
        staff = staff if isinstance(staff, dict) else {}
        if not staff.get("unlocked"):
            return {}
        budget = max(0, SiQiRestaurantV3.__safe_int(budget_magic, 0))
        capacity = staff.get("capacity") or {}
        available = staff.get("available") or {}
        next_prices = staff.get("next_prices") or {}
        plan: Dict[str, int] = {}
        for role in ("chef", "server"):
            cap = SiQiRestaurantV3.__safe_int(capacity.get(role), 0)
            avail = SiQiRestaurantV3.__safe_int(available.get(role), 0)
            gap = cap - avail
            if gap <= 0:
                continue
            price = next_prices.get(role)
            try:
                price = int(price)
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            affordable = budget // price
            quantity = min(gap, affordable)
            if quantity <= 0:
                continue
            plan[role] = quantity
            budget -= quantity * price
        return plan

    @staticmethod
    def __deadline_key(customer: Any) -> float:
        """取顾客截止时间用于升序排序；缺失/非法视为最早（0）。"""
        ts = SiQiRestaurantV3.__deadline_ts((customer or {}).get("deadline"))
        return ts if ts is not None else 0.0

    @staticmethod
    def __roll_daily(daily: Dict[str, Any], today: str) -> Dict[str, Any]:
        """按自然日重置当日计数，同日则原样返回。"""
        daily = daily if isinstance(daily, dict) else {}
        if daily.get("date") == today:
            return daily
        return {
            "date": today,
            "sessions": 0,
            "customers_served": 0,
            "customers_left": 0,
            "magic_spent": 0,
            "magic_earned": 0,
            "dishes": {},
            "visits": 0,
            "last_result": "",
            "history": [],
        }

    @staticmethod
    def __safe_int(value: Any, default: int, min_value: int = None) -> int:
        """安全转 int，失败回退默认值，可选下界。"""
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
        """安全转 float 并夹取到 [min, max]。"""
        try:
            result = float(value)
        except (ValueError, TypeError):
            result = default
        return min(max_value, max(min_value, result))

    @staticmethod
    def __game_ok(payload: Dict[str, Any]) -> bool:
        """判定写操作是否成功：success 为真或 status == success。"""
        if not isinstance(payload, dict):
            return False
        return bool(payload.get("success")) or payload.get("status") == "success"

    @staticmethod
    def __api_response(
        success: bool, message: Any, data: Any = None
    ) -> Dict[str, Any]:
        """构造前端要求的响应外壳：恰好 success/message/data 三键。"""
        text = "" if message is None else str(message)
        if isinstance(data, dict):
            payload = dict(data)
        elif data is None:
            payload = {}
        else:
            payload = {"value": data}
        return {"success": bool(success), "message": text, "data": payload}

    @staticmethod
    def __new_result(status: str = "running", message: str = "") -> Dict[str, Any]:
        """构造一次任务的运行结果容器。"""
        return {
            "status": status,
            "message": message,
            "data": {},
            "timestamp": time.time(),
        }

    # ------------------------------------------------------------------ #
    # 网络 / 状态映射
    # ------------------------------------------------------------------ #
    def __fetch_url(self) -> str:
        """返回状态读取接口 URL（GET ?action=fetch）。"""
        return f"{self.GAME_URL}?action=fetch"

    async def __fetch_state(self, client: AsyncRequestUtils) -> Optional[Dict[str, Any]]:
        """GET ?action=fetch 拉取整站 JSON 状态；解析失败返回 None，网络异常上抛。"""
        response = await client.get_res(self.__fetch_url(), raise_exception=True)
        if response is None:
            return None
        try:
            data = response.json()
        except Exception:
            logger.error("思齐开餐馆：状态接口返回非 JSON")
            return None
        return data if isinstance(data, dict) else None

    async def __post_action(
        self, client: AsyncRequestUtils, action: str, **payload: Any
    ) -> Dict[str, Any]:
        """POST form-urlencoded 执行写操作，异常统一归一为 {}。"""
        data = {"action": action}
        for key, value in payload.items():
            data[key] = str(value)
        try:
            response = await client.post_res(
                self.GAME_URL, data=data, raise_exception=True
            )
        except httpx2.TimeoutException:
            logger.error(f"思齐开餐馆：{action} 请求超时")
            return {}
        except httpx2.HTTPError as e:
            logger.error(f"思齐开餐馆：{action} 请求异常：{e}")
            return {}
        if response is None:
            logger.error(f"思齐开餐馆：{action} 无响应")
            return {}
        try:
            result = response.json()
        except Exception:
            logger.error(f"思齐开餐馆：{action} 返回非 JSON")
            return {}
        return result if isinstance(result, dict) else {}

    def __apply_state(self, data: Dict[str, Any]) -> None:
        """把接口返回（整站或写操作回包）写入运行态快照，缺失字段不动。"""
        if not isinstance(data, dict):
            return
        user = data.get("user")
        if isinstance(user, dict) and "magic" in user:
            self._state["magic"] = self.__safe_int(user.get("magic"), 0)
        config = data.get("config")
        if isinstance(config, dict) and config:
            self.__apply_config(config)
        st = data.get("state")
        if isinstance(st, dict) and st:
            self.__apply_state_fields(st)
        inventory = data.get("inventory")
        if isinstance(inventory, dict) and "ingredients" in inventory:
            self._state["ingredients"] = self.__normalize_ingredients(
                inventory.get("ingredients")
            )
        staff = data.get("staff")
        if isinstance(staff, dict) and staff:
            self._state["staff"] = dict(staff)
        visits = data.get("visits")
        if isinstance(visits, dict) and visits:
            self._state["visits"] = dict(visits)
        logs = data.get("logs")
        if isinstance(logs, list):
            self._state["logs"] = list(logs)
        self._state["updated_at"] = time.time()

    def __apply_config(self, config: Dict[str, Any]) -> None:
        """从 config 缓存菜谱与食材价格元数据。"""
        dishes = config.get("dishes")
        if isinstance(dishes, list):
            self._state["dishes"] = {
                str(d.get("key")): dict(d)
                for d in dishes
                if isinstance(d, dict) and d.get("key")
            }
        ingredients = config.get("ingredients")
        if isinstance(ingredients, list):
            self._state["ingredient_meta"] = {
                str(i.get("key")): {
                    "name": i.get("name"),
                    "price": self.__safe_int(i.get("price"), 0),
                    "icon": i.get("icon"),
                }
                for i in ingredients
                if isinstance(i, dict) and i.get("key")
            }

    def __apply_state_fields(self, st: Dict[str, Any]) -> None:
        """从 state 字段更新运行态（仅更新存在的键）。"""
        int_fields = (
            "popularity",
            "max_popularity",
            "customers_total",
            "customers_served",
            "customers_remaining",
            "queued_customers",
            "earnings_today",
            "net_earnings_today",
            "net_earnings_total",
        )
        for field in int_fields:
            if field in st:
                self._state[field] = self.__safe_int(st.get(field), 0)
        if "session_status" in st and st.get("session_status"):
            self._state["session_status"] = str(st.get("session_status"))
        if "cur_date" in st and st.get("cur_date"):
            self._state["cur_date"] = str(st.get("cur_date"))
        if isinstance(st.get("customer_plan"), list):
            self._state["customer_plan"] = [
                dict(c) for c in st.get("customer_plan") if isinstance(c, dict)
            ]
        if isinstance(st.get("unlocked"), dict):
            self._state["unlocked"] = dict(st.get("unlocked"))

    def __mark_plan_status(self, customer_id: Any, status: str) -> None:
        """在本地顾客计划里标记某顾客状态，避免同一轮重复处理。"""
        plan = self._state.get("customer_plan")
        if not isinstance(plan, list):
            return
        for customer in plan:
            if isinstance(customer, dict) and customer.get("id") == customer_id:
                customer["status"] = status

    def __sync_daily_date(self, server_date: Any) -> None:
        """把每日计数对齐到服务器 cur_date，跨日则归零。"""
        text = str(server_date or "").strip()
        if text and text != self._daily.get("date"):
            self._daily = self.__roll_daily(self._daily, text)

    def __extract_visit_targets(self, payload: Dict[str, Any]) -> List[str]:
        """从可拜访列表回包中提取用户名列表（兼容多种结构）。"""
        if not isinstance(payload, dict):
            return []
        candidates: List[Any] = []
        visits = payload.get("visits")
        if isinstance(visits, dict):
            for field in ("records", "list", "pending_visitors"):
                value = visits.get(field)
                if isinstance(value, dict):
                    candidates.extend(value.get("list") or [])
                elif isinstance(value, list):
                    candidates.extend(value)
        for field in ("restaurants", "targets", "data"):
            value = payload.get(field)
            if isinstance(value, list):
                candidates.extend(value)
            elif isinstance(value, dict) and isinstance(value.get("list"), list):
                candidates.extend(value["list"])
        result: List[str] = []
        for item in candidates:
            name = None
            if isinstance(item, dict):
                name = (
                    item.get("target_username")
                    or item.get("username")
                    or item.get("name")
                )
            elif isinstance(item, str):
                name = item
            if name and name not in result:
                result.append(str(name))
        return result

    def __record(self, action: str, detail: str) -> None:
        """向今日历史追加一条经营记录（最多保留 DAILY_HISTORY_LIMIT 条）。"""
        history = list(self._daily.get("history") or [])
        history.append(
            {
                "time": datetime.now(self.TZ_CN).strftime("%H:%M:%S"),
                "action": action,
                "detail": detail,
            }
        )
        self._daily["history"] = history[-self.DAILY_HISTORY_LIMIT:]

    @staticmethod
    def __option_label(key: str) -> str:
        """选项键 → 中文名；未知键回退为原始键。"""
        name = str(key or "").strip()
        spec = SiQiRestaurantV3.OPTION_WHITELIST.get(name)
        if isinstance(spec, dict) and spec.get("label"):
            return str(spec["label"])
        return name

    @staticmethod
    def __normalize_option(key: str, value: Any) -> Tuple[bool, Any, str]:
        """校验页面提交的选项，返回 (是否通过, 归一化值, 中文名或失败原因)。"""
        name = str(key or "").strip()
        if not name:
            return False, None, "缺少选项名，请进配置页调整"
        if name in SiQiRestaurantV3.OPTION_BLOCKED:
            return False, None, f"「{name}」只能在配置页修改"
        spec = SiQiRestaurantV3.OPTION_WHITELIST.get(name)
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

    def __get_lock(self) -> asyncio.Lock:
        """惰性创建并返回并发锁。"""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def __build_client(
        self, cookie: str, headers: Dict[str, str], timeout: int
    ) -> AsyncRequestUtils:
        """构造带 Cookie 与请求头的异步客户端。"""
        return AsyncRequestUtils(
            headers=headers,
            cookies=self.__cookie_to_dict(cookie),
            timeout=timeout,
        )

    def __get_site_cookie(self) -> str:
        """从站点管理读取 si-qi.xyz 的 Cookie，失败返回空串。"""
        try:
            site = SiteOper().get_by_domain(self.SITE_DOMAIN)
            if site and site.cookie:
                return site.cookie
        except Exception as e:
            logger.error(f"思齐开餐馆：读取站点 Cookie 异常：{e}")
        return ""

    @staticmethod
    def __cookie_to_dict(cookie: str) -> Dict[str, str]:
        """把 Cookie 字符串解析为字典。"""
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
        """构造游戏接口所需的请求头。"""
        return {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "content-type": "application/x-www-form-urlencoded",
            "origin": self.BASE_URL,
            "referer": self.GAME_URL,
            "user-agent": self.UA,
        }

    def __finish_task(self, result: Dict[str, Any]) -> None:
        """任务收尾：按需通知并保存状态。"""
        if self._notify:
            self.__send_notification(result)
        self._state["last_message"] = result.get("message", "")
        self.__save_state()

    def __save_state(self) -> None:
        """持久化每日统计与运行态快照。"""
        try:
            self._state["staff_snapshot"] = dict(self._state.get("staff") or {})
            self._state["inventory_snapshot"] = {
                "ingredients": dict(self._state.get("ingredients") or {})
            }
            self.save_data("daily", self._daily)
            self.save_data("state", self._state)
        except Exception as e:
            logger.error(f"思齐开餐馆：保存状态失败：{e}")

    def __send_notification(self, result: Dict[str, Any]) -> None:
        """发送插件通知。"""
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
        if data.get("customers_total"):
            text += (
                f"\n\n🍽️ 今日顾客：{data.get('customers_served')}"
                f"/{data.get('customers_total')}"
            )
        if data.get("served") is not None:
            text += f"\n👨‍🍳 本次上菜：{data.get('served')} 位"
        if data.get("magic_spent"):
            text += f"\n💸 采购花费：{data.get('magic_spent')}"
        try:
            self.post_message(
                mtype=MessageType.Plugin, title="🍽️【思齐开餐馆】", text=text
            )
        except Exception as e:
            logger.error(f"思齐开餐馆：发送通知失败：{e}")

    @eventmanager.register(EventType.PluginAction)
    async def run_once_command(self, event: Event = None):
        """远程命令入口：只处理本插件的 action。"""
        event_data = event.event_data if event else {}
        if not event_data or event_data.get("action") != "siqi_restaurant":
            return
        channel = event_data.get("channel")
        userid = event_data.get("user")
        if self.__get_lock().locked():
            self.post_message(
                channel=channel,
                userid=userid,
                mtype=MessageType.Plugin,
                title="【思齐开餐馆】",
                text="已有任务正在执行，请等待当前任务结束。",
            )
            return
        scheduler_sdk.add_plugin_once_job(
            self.__class__.__name__,
            "command_run_once",
            self.run_restaurant_task,
            "思齐开餐馆命令立即运行",
            delay_seconds=1,
        )
        self.post_message(
            channel=channel,
            userid=userid,
            mtype=MessageType.Plugin,
            title="【思齐开餐馆】",
            text="任务已开始，完成后会发送通知。",
        )
