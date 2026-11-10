"""SiQiRestaurantV3 纯逻辑测试：不连接宿主数据库、调度器与真实站点。

风格对齐 ``tests/v3/siqifishingv3/test_plugin.py``：内置 ``ActionRequestUtils``
假客户端、``FakeResponse``、``StubSiteOper``、``build_plugin``、``_MiniMonkeyPatch``
与 ``_run_standalone``，因此本文件既可在 pytest 下运行，也可 ``python test_plugin.py`` 独立运行。
"""

import asyncio
import importlib.util
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

PLUGIN_INIT = (
    Path(__file__).resolve().parents[3]
    / "plugins.v3"
    / "siqirestaurantv3"
    / "__init__.py"
)


def load_plugin_module():
    """按文件路径加载插件模块，避免依赖插件市场安装结果。"""
    spec = importlib.util.spec_from_file_location(
        "siqirestaurantv3_under_test", PLUGIN_INIT
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    """模拟 HTTPX2 响应对象：``json()`` 返回预置 payload，缺失时抛 ValueError。"""

    def __init__(self, *, status_code: int = 200, text: str = "", payload=None) -> None:
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self):
        """返回预置 JSON；模拟非 JSON 响应时抛 ValueError。"""
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class ActionRequestUtils:
    """按 action 分发预置响应的假客户端。

    - ``post_res`` 依据 ``data["action"]`` 取响应，并记录 ``actions`` / ``bodies`` / ``calls``；
    - ``get_res`` 返回 ``FakeResponse(payload=fetch_payload)``，用于模拟 ``?action=fetch``；
      ``fetch_payload`` 为 list 时按顺序逐个弹出（用于模拟多次 fetch 状态变化）。
    """

    actions: list = []
    bodies: list = []
    calls: list = []
    responses: dict = {}
    fetch_payload = None

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    @classmethod
    def reset(cls, responses=None, fetch_payload=None) -> None:
        """清空调用记录并装入新的响应队列与 fetch 状态。"""
        cls.actions = []
        cls.bodies = []
        cls.calls = []
        cls.responses = dict(responses or {})
        cls.fetch_payload = fetch_payload

    async def post_res(self, url, **kwargs):
        data = kwargs.get("data") or {}
        action = data.get("action", "")
        ActionRequestUtils.actions.append(action)
        ActionRequestUtils.bodies.append(dict(data))
        ActionRequestUtils.calls.append((action, dict(data)))
        queued = ActionRequestUtils.responses.get(action)
        if queued is None:
            return FakeResponse(payload={"success": True})
        if isinstance(queued, list):
            if not queued:
                return FakeResponse(payload={"success": False, "message": "无预置响应"})
            return queued.pop(0)
        return queued

    async def get_res(self, url, **kwargs):
        ActionRequestUtils.calls.append(("GET", {}))
        payload = ActionRequestUtils.fetch_payload
        if isinstance(payload, list):
            payload = payload.pop(0) if payload else {}
        return FakeResponse(payload=payload)


class StubSiteOper:
    """隔离站点数据库查询，始终返回未配置站点。"""

    @staticmethod
    def get_by_domain(domain):
        return None

    @staticmethod
    async def async_get_by_domain(domain):
        return None


class StubScheduler:
    """记录一次性任务注册与移除，避免触碰真实调度器。"""

    def __init__(self) -> None:
        self.added: list = []
        self.removed: list = []

    def add_plugin_once_job(self, *args, **kwargs):
        self.added.append((args, kwargs))
        return True

    def remove_plugin_once_job(self, *args, **kwargs):
        self.removed.append((args, kwargs))
        return True


_CN_TZ = timezone(timedelta(hours=8))


def _timezone(module):
    """读取插件使用的东八区时区，缺失时回退到 UTC+8。"""
    return getattr(module.SiQiRestaurantV3, "TZ_CN", _CN_TZ)


def _today(module) -> str:
    """返回插件时区下的自然日字符串。"""
    return datetime.now(_timezone(module)).strftime("%Y-%m-%d")


def _today_str() -> str:
    """不依赖插件模块的东八区自然日字符串。"""
    return datetime.now(_CN_TZ).strftime("%Y-%m-%d")


def make_config(**overrides) -> dict:
    """构造一份完整的插件配置快照，用于校验 set_option 的整体回写与 init 幂等。"""
    config = {
        "enabled": True,
        "notify": False,
        "cookie": "c_secure_pass=token",
        "schedule_mode": "interval",
        "cron": "7 */2 * * *",
        "interval_minutes": 30,
        "dry_run": False,
        "auto_start_session": True,
        "auto_purchase": True,
        "buy_budget_per_day": 5000,
        "keep_ingredient_stock": 3,
        "serve_strategy": "match",
        "min_serve_delay_seconds": 0,
        "max_customers_per_run": 0,
        "max_iterations": 60,
        "auto_hire_staff": True,
        "auto_visit": False,
        "visit_targets": "",
        "visit_count_limit": 3,
        "run_once": False,
    }
    config.update(overrides)
    return config


DEFAULT_DISHES = [
    {
        "key": "tomato_egg",
        "name": "番茄炒蛋",
        "tool": "wok",
        "icon": "",
        "cook_time": 3,
        "price": 120,
        "unlock_popularity": 0,
        "description": "",
        "ingredients": [
            {"key": "tomato", "quantity": 1},
            {"key": "egg", "quantity": 2},
        ],
    }
]

DEFAULT_INGREDIENTS = [
    {"key": "tomato", "name": "番茄", "icon": "", "price": 10},
    {"key": "egg", "name": "鸡蛋", "icon": "", "price": 5},
]


def make_fetch_payload(
    *,
    success: bool = True,
    magic: int = 1000,
    popularity: int = 107,
    max_popularity: int = 0,
    session_status: str = "ready",
    customers_total: int = 0,
    customers_served: int = 0,
    customers_remaining: int = 0,
    queued_customers: int = 0,
    earnings_today: int = 0,
    net_earnings_today: int = 0,
    net_earnings_total: int = 0,
    cur_date=None,
    customer_plan=None,
    unlocked=None,
    config_dishes=None,
    config_ingredients=None,
    inventory_ingredients=None,
    staff=None,
    visits=None,
    logs=None,
) -> dict:
    """构造一份与站点契约一致的完整 fetch JSON。

    覆盖 ``success/user/state/config/inventory/staff/logs/visits`` 全部字段，
    各子项均可通过关键字覆盖。
    """
    dishes = DEFAULT_DISHES if config_dishes is None else config_dishes
    ingredients = (
        DEFAULT_INGREDIENTS if config_ingredients is None else config_ingredients
    )
    if unlocked is None:
        unlocked = {
            "stations": 1,
            "seats": 1,
            "dishes": [d["key"] for d in dishes if isinstance(d, dict)],
        }
    if staff is None:
        staff = {
            "inventory": {"chef": 0, "server": 0},
            "assignments": [],
            "assignments_map": [],
            "assigned_counts": {"chef": 0, "server": 0},
            "capacity": {"chef": 0, "server": 0},
            "available": {"chef": 0, "server": 0},
            "next_prices": {"chef": None, "server": None},
            "unlocked": False,
        }
    if visits is None:
        visits = {
            "count": 0,
            "limit": 3,
            "remaining": 3,
            "records": [],
            "reached_limit": False,
            "pending_visitors": {"count": 0, "list": []},
        }
    return {
        "success": success,
        "message": "",
        "user": {
            "id": 1,
            "username": "tester",
            "magic": magic,
            "hnr": {"value": 1, "rank": "A"},
            "seed_level": {"level": 1, "icon": ""},
        },
        "state": {
            "popularity": popularity,
            "popularity_at_reset": 0,
            "max_popularity": max_popularity,
            "cur_date": cur_date if cur_date is not None else _today_str(),
            "customers_total": customers_total,
            "customers_served": customers_served,
            "customers_remaining": customers_remaining,
            "queued_customers": queued_customers,
            "earnings_today": earnings_today,
            "net_earnings_today": net_earnings_today,
            "net_earnings_total": net_earnings_total,
            "session_status": session_status,
            "customer_plan": list(customer_plan or []),
            "unlocked": dict(unlocked),
        },
        "config": {
            "ingredients": list(ingredients),
            "tools": [],
            "dishes": list(dishes),
            "settings": {
                "max_stations": 4,
                "max_seats": 6,
                "popularity_thresholds": [],
                "automation_mode": "auto",
            },
            "staff": {"packages": []},
        },
        "inventory": {
            "ingredients": {} if inventory_ingredients is None else inventory_ingredients,
            "tools": {},
        },
        "staff": dict(staff),
        "logs": list(logs or []),
        "visits": dict(visits),
    }


def build_plugin(module):
    """绕过 _PluginBase.__init__ 构造最小实例，并注入默认属性与依赖。"""
    plugin = object.__new__(module.SiQiRestaurantV3)
    plugin._enabled = True
    plugin._notify = False
    plugin._cookie = "c_secure_pass=token"
    plugin._schedule_mode = "cron"
    plugin._cron = "12 10 * * *"
    plugin._interval_minutes = 30
    plugin._retry_enabled = True
    plugin._retry_cron = "12 15 * * *"

    plugin._dry_run = False
    plugin._auto_start_session = True
    plugin._auto_purchase = True
    plugin._buy_budget_per_day = 5000
    plugin._keep_ingredient_stock = 3
    plugin._serve_strategy = "match"
    plugin._min_serve_delay_seconds = 0
    plugin._max_customers_per_run = 0
    plugin._max_iterations = 60
    plugin._auto_hire_staff = True
    plugin._auto_visit = False
    plugin._visit_targets = ""
    plugin._visit_count_limit = 3

    plugin._lock = None
    plugin._daily = {
        "date": _today(module),
        "sessions": 0,
        "completed": False,
        "customers_served": 0,
        "customers_left": 0,
        "magic_spent": 0,
        "magic_earned": 0,
        "dishes": {},
        "visits": 0,
        "last_result": "",
        "history": [],
    }
    plugin._state = {
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
    plugin._data = {}
    plugin._config_snapshot = make_config()
    plugin._updated_configs = []
    plugin.save_data = lambda key, value: plugin._data.__setitem__(key, value)
    plugin.get_data = lambda key, default=None: plugin._data.get(key, default)
    plugin.get_config = lambda: plugin._config_snapshot

    def _update_config(config):
        plugin._updated_configs.append(config)
        plugin._config_snapshot = dict(config)

    plugin.update_config = _update_config
    plugin.post_message = lambda **kwargs: None
    return plugin


def _patch_sleep(monkeypatch, module):
    """把 ``asyncio.sleep`` 换成只记录参数、立即返回的协程，避免测试真实等待。"""
    sleeps: list = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(module.asyncio, "sleep", fake_sleep)
    return sleeps


def _stub_runtime(monkeypatch, module, *, fetch_payload=None, responses=None):
    """替换网络、站点与调度器，并装入 fetch 状态与 action 响应。"""
    _patch_sleep(monkeypatch, module)
    scheduler = StubScheduler()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    monkeypatch.setattr(module, "scheduler_sdk", scheduler)
    ActionRequestUtils.reset(
        responses=responses or {},
        fetch_payload=fetch_payload
        if fetch_payload is not None
        else make_fetch_payload(),
    )
    return scheduler


def _lock_plugin(plugin):
    """给插件装上已持有的运行锁，模拟任务执行中。"""
    lock = asyncio.Lock()
    asyncio.run(lock.acquire())
    plugin._lock = lock
    return lock


# ---------------------------------------------------------------------------
# 元数据 / 版本一致性
# ---------------------------------------------------------------------------


def test_metadata_matches_directory():
    """插件 ID、目录名与市场元数据必须一致。"""
    module = load_plugin_module()
    cls = module.SiQiRestaurantV3

    assert cls.__name__.lower() == PLUGIN_INIT.parent.name
    assert PLUGIN_INIT.parent.name == "siqirestaurantv3"
    assert cls.plugin_version == "1.0.5"
    assert cls.plugin_config_prefix == "siqirestaurantv3_"
    assert cls.plugin_order == 32
    assert cls.plugin_author == "tycgd"
    assert cls.auth_level == 1
    assert cls.plugin_name == "思齐开餐馆"

    package = json.loads((PLUGIN_INIT.parents[2] / "package.v3.json").read_text("utf-8"))
    entry = package[cls.__name__]
    assert entry["version"] == cls.plugin_version
    assert entry["icon"] == cls.plugin_icon
    assert entry["name"] == cls.plugin_name
    assert entry["author"] == "tycgd"
    assert entry["level"] == 1


def test_version_consistency():
    """plugin_version、package.v3.json 与 history 顶部键三处一致。"""
    module = load_plugin_module()
    cls = module.SiQiRestaurantV3
    package = json.loads((PLUGIN_INIT.parents[2] / "package.v3.json").read_text("utf-8"))
    entry = package[cls.__name__]

    assert cls.plugin_version == "1.0.5"
    assert entry["version"] == cls.plugin_version
    assert next(iter(entry["history"])) == f"v{cls.plugin_version}"


# ---------------------------------------------------------------------------
# init 默认值与幂等
# ---------------------------------------------------------------------------


def test_init_defaults(monkeypatch):
    """空配置初始化落到规格约定的默认值。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    plugin = build_plugin(module)

    plugin.init_plugin({})

    assert plugin._enabled is False
    assert plugin._notify is True
    assert plugin._cookie == ""
    assert plugin._schedule_mode == "cron"
    assert plugin._cron == "12 10 * * *"
    assert plugin._interval_minutes == 30
    assert plugin._retry_enabled is True
    assert plugin._retry_cron == "12 15 * * *"
    assert plugin._dry_run is False
    assert plugin._auto_start_session is True
    assert plugin._auto_purchase is True
    assert plugin._buy_budget_per_day == 5000
    assert plugin._keep_ingredient_stock == 3
    assert plugin._serve_strategy == "match"
    assert plugin._min_serve_delay_seconds == 2
    assert plugin._max_customers_per_run == 0
    assert plugin._max_iterations == 60
    assert plugin._auto_hire_staff is True
    assert plugin._auto_visit is False
    assert plugin._visit_targets == ""
    assert plugin._visit_count_limit == 3
    assert plugin._daily["date"] == _today(module)
    assert plugin._daily["completed"] is False


def test_init_idempotent(monkeypatch):
    """重复 init 不丢配置值，且已持久化的每日统计保持。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    plugin = build_plugin(module)
    config = make_config(
        auto_purchase=False,
        serve_strategy="cheapest",
        buy_budget_per_day=1234,
        max_iterations=42,
        dry_run=True,
    )

    plugin.init_plugin(config)
    snapshot = (
        plugin._auto_purchase,
        plugin._serve_strategy,
        plugin._buy_budget_per_day,
        plugin._max_iterations,
        plugin._dry_run,
    )

    plugin.save_data(
        "daily",
        {
            "date": _today(module),
            "sessions": 2,
            "customers_served": 7,
            "history": [],
        },
    )
    plugin.init_plugin(config)

    assert snapshot == (
        plugin._auto_purchase,
        plugin._serve_strategy,
        plugin._buy_budget_per_day,
        plugin._max_iterations,
        plugin._dry_run,
    )
    assert plugin._daily["sessions"] == 2
    assert plugin._daily["customers_served"] == 7


# ---------------------------------------------------------------------------
# 认证 / fetch / 非 JSON
# ---------------------------------------------------------------------------


def _run_execute(plugin):
    """同步执行一次 ``__execute``，返回结果容器。"""
    result = getattr(plugin, "_SiQiRestaurantV3__new_result")()
    asyncio.run(getattr(plugin, "_SiQiRestaurantV3__execute")(result))
    return result


def test_execute_without_cookie_auth_failed():
    """没有 Cookie 时 __execute 置 auth_failed 且不发任何请求。"""
    module = load_plugin_module()
    ActionRequestUtils.reset(fetch_payload=make_fetch_payload())
    plugin = build_plugin(module)
    plugin._cookie = ""

    result = _run_execute(plugin)

    assert result["status"] == "auth_failed"
    assert ActionRequestUtils.calls == []


def test_execute_cookie_without_secure_pass_auth_failed():
    """Cookie 缺少 c_secure_pass 时同样认证失败且不发请求。"""
    module = load_plugin_module()
    ActionRequestUtils.reset(fetch_payload=make_fetch_payload())
    plugin = build_plugin(module)
    plugin._cookie = "foo=bar"

    result = _run_execute(plugin)

    assert result["status"] == "auth_failed"
    assert ActionRequestUtils.calls == []


def test_fetch_state_returns_dict():
    """__fetch_state 正常返回 dict。"""
    module = load_plugin_module()
    payload = make_fetch_payload(session_status="open")
    ActionRequestUtils.reset(fetch_payload=payload)
    plugin = build_plugin(module)
    client = ActionRequestUtils()

    data = asyncio.run(
        getattr(plugin, "_SiQiRestaurantV3__fetch_state")(client)
    )

    assert isinstance(data, dict)
    assert data["success"] is True
    assert data["state"]["session_status"] == "open"


def test_fetch_state_non_json_returns_none():
    """非 JSON（HTML 登录页）时 __fetch_state 返回 None。"""
    module = load_plugin_module()
    ActionRequestUtils.reset(fetch_payload=None)
    plugin = build_plugin(module)
    client = ActionRequestUtils()

    assert asyncio.run(
        getattr(plugin, "_SiQiRestaurantV3__fetch_state")(client)
    ) is None


def test_execute_non_json_marks_failed(monkeypatch):
    """非 JSON 状态返回时 __execute 置 failed，且不产生 POST。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    ActionRequestUtils.reset(fetch_payload=None)
    plugin = build_plugin(module)

    result = _run_execute(plugin)

    assert result["status"] == "failed"
    assert ActionRequestUtils.actions == []


# ---------------------------------------------------------------------------
# dry_run / start_session
# ---------------------------------------------------------------------------


def test_dry_run_only_get(monkeypatch):
    """演练模式只 GET fetch，calls 中无任何 POST action。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(customers_total=3),
    )
    plugin = build_plugin(module)
    plugin._dry_run = True

    result = asyncio.run(plugin.run_restaurant_task())

    assert result["status"] == "completed"
    assert "演练" in result["message"]
    assert ActionRequestUtils.actions == []
    assert all(call[0] == "GET" for call in ActionRequestUtils.calls)


def test_start_session_only_ready_with_customers(monkeypatch):
    """仅 ready 且 customers_total>0 才 POST start_session，并 sessions+1。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="ready", customers_total=2, customers_remaining=0
        ),
    )
    plugin = build_plugin(module)

    result = asyncio.run(plugin.run_restaurant_task())

    assert result["status"] == "completed"
    assert ActionRequestUtils.actions.count("start_session") == 1
    assert plugin._daily["sessions"] == 1


def test_start_session_not_when_closed_or_zero(monkeypatch):
    """closed 或 customers_total=0 时不 POST start_session。"""
    module = load_plugin_module()

    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(session_status="closed", customers_total=3),
    )
    plugin = build_plugin(module)
    asyncio.run(plugin.run_restaurant_task())
    assert "start_session" not in ActionRequestUtils.actions

    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(session_status="ready", customers_total=0),
    )
    plugin = build_plugin(module)
    asyncio.run(plugin.run_restaurant_task())
    assert "start_session" not in ActionRequestUtils.actions


def test_start_session_failure_refetch_open_not_failure(monkeypatch):
    """start_session 返回 success=False 但重 fetch 后为 open，不视为失败。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=[
            make_fetch_payload(
                session_status="ready", customers_total=2, customers_remaining=0
            ),
            make_fetch_payload(
                session_status="open", customers_total=2, customers_remaining=0
            ),
            make_fetch_payload(
                session_status="open", customers_total=2, customers_remaining=0
            ),
            make_fetch_payload(
                session_status="open", customers_total=2, customers_remaining=0
            ),
        ],
        responses={
            "start_session": FakeResponse(
                payload={"success": False, "message": "今日已开店"}
            )
        },
    )
    plugin = build_plugin(module)

    result = asyncio.run(plugin.run_restaurant_task())

    assert "start_session" in ActionRequestUtils.actions
    assert result["status"] == "completed"
    assert not any(
        record.get("action") == "warn" for record in plugin._daily["history"]
    ), plugin._daily["history"]


# ---------------------------------------------------------------------------
# 纯函数
# ---------------------------------------------------------------------------


def _static(module, name):
    """取插件内部静态纯函数。"""
    return getattr(module.SiQiRestaurantV3, f"_SiQiRestaurantV3__{name}")


def test_normalize_ingredients_dict_and_list_equal():
    """dict 与 list 两种库存格式归一结果一致。"""
    module = load_plugin_module()
    normalize = _static(module, "normalize_ingredients")

    dict_form = {"tomato": 3, "egg": {"quantity": 2}}
    list_form = [{"key": "tomato", "quantity": 3}, ["egg", 2]]

    assert normalize(dict_form) == {"tomato": 3, "egg": 2}
    assert normalize(list_form) == normalize(dict_form)


def test_normalize_ingredients_tolerant():
    """非法值/缺字段/负数量都容错为 0 并跳过坏项。"""
    module = load_plugin_module()
    normalize = _static(module, "normalize_ingredients")

    result = normalize({"a": "oops", "b": -5, "c": None, "d": {"qty": "2"}})

    assert result == {"a": 0, "b": 0, "c": 0, "d": 2}
    assert normalize([None, "x", [], {"nokey": 1}]) == {}


def _demand_dishes():
    return {
        "tomato_egg": {
            "key": "tomato_egg",
            "ingredients": [
                {"key": "tomato", "quantity": 1},
                {"key": "egg", "quantity": 2},
            ],
        }
    }


def test_dish_and_ingredient_demand():
    """两位顾客点同菜 → 食材需求翻倍；served/left 不计。"""
    module = load_plugin_module()
    dish_demand = _static(module, "dish_demand")
    ingredient_demand = _static(module, "ingredient_demand")
    dishes = _demand_dishes()
    customers = [
        {"id": 1, "status": "waiting", "dish_key": "tomato_egg"},
        {"id": 2, "status": "waiting", "dish_key": "tomato_egg"},
        {"id": 3, "status": "served", "dish_key": "tomato_egg"},
        {"id": 4, "status": "left", "dish_key": "tomato_egg"},
        {"id": 5, "status": "waiting", "dish_key": "unknown"},
    ]

    demand = dish_demand(customers, dishes)

    assert demand == {"tomato_egg": 2}
    assert ingredient_demand(demand, dishes) == {"tomato": 2, "egg": 4}


def test_purchase_plan_budget_and_keep():
    """保底/预算/已花费截断，不超预算。"""
    module = load_plugin_module()
    purchase_plan = _static(module, "purchase_plan")

    plan, total = purchase_plan({"a": 5}, {}, 0, {"a": 10}, 100, 0)
    assert plan[0]["quantity"] == 5 and total == 50

    plan, total = purchase_plan({"a": 5}, {}, 0, {"a": 10}, 100, 80)
    assert total == 20  # 只剩 20 预算，最多买 2
    assert total <= 100 - 80

    plan, total = purchase_plan({"a": 1}, {"a": 2}, 3, {"a": 10}, 100, 0)
    assert plan[0]["quantity"] == 2  # 目标 need+keep=4，已有 2


def test_purchase_plan_skips_zero_price_and_enough():
    """价格为 0、库存已够或预算已耗尽都不购买。"""
    module = load_plugin_module()
    purchase_plan = _static(module, "purchase_plan")

    assert purchase_plan({"a": 5}, {}, 0, {"a": 0}, 100, 0) == ([], 0)
    assert purchase_plan({"a": 1}, {"a": 10}, 0, {"a": 10}, 100, 0) == ([], 0)
    assert purchase_plan({"a": 5}, {}, 0, {"a": 10}, 100, 200) == ([], 0)


def test_choose_dish_match_and_extremes():
    """match 返回顾客 dish_key；缺 dish_key 时 cheapest/expensive 取价格极值。"""
    module = load_plugin_module()
    choose = _static(module, "choose_dish")
    dishes = {
        "tomato_egg": {"price": 120, "ingredients": [{"key": "tomato", "quantity": 1}]},
        "egg_soup": {"price": 60, "ingredients": [{"key": "egg", "quantity": 1}]},
    }
    stock = {"tomato": 5, "egg": 5}

    assert choose({"dish_key": "tomato_egg"}, "match", dishes, stock) == "tomato_egg"
    assert choose({}, "cheapest", dishes, stock) == "egg_soup"
    assert choose({}, "expensive", dishes, stock) == "tomato_egg"


def test_choose_dish_no_candidates_returns_empty():
    """无任何可用候选且未指定 dish_key 时返回空串。"""
    module = load_plugin_module()
    choose = _static(module, "choose_dish")

    dishes = {"x": {"price": 10, "ingredients": [{"key": "rare", "quantity": 1}]}}
    assert choose({}, "cheapest", dishes, {}) == ""
    assert choose({}, "cheapest", {}, {}) == ""


def test_missing_ingredients():
    """缺口计算正确。"""
    module = load_plugin_module()
    missing = _static(module, "missing_ingredients")

    dish = {
        "ingredients": [
            {"key": "tomato", "quantity": 2},
            {"key": "egg", "quantity": 1},
        ]
    }

    assert missing(dish, {"tomato": 1, "egg": 5}) == {"tomato": 1}
    assert missing(dish, {"tomato": 2, "egg": 1}) == {}


# ---------------------------------------------------------------------------
# serve_loop
# ---------------------------------------------------------------------------


def _serve_loop_of(plugin):
    return getattr(plugin, "_SiQiRestaurantV3__serve_loop")


def _waiting(cid, dish="tomato_egg", deadline=None):
    return {
        "id": cid,
        "dish_key": dish,
        "status": "waiting",
        "deadline": time.time() + 1000 if deadline is None else deadline,
    }


def test_serve_loop_purchase_then_serve(monkeypatch):
    """正常流程先 purchase_ingredient 再 serve_customer，参数正确，成功计数。"""
    module = load_plugin_module()
    ActionRequestUtils.reset(
        fetch_payload=make_fetch_payload(
            session_status="open", customers_remaining=0, queued_customers=0
        )
    )
    _patch_sleep(monkeypatch, module)
    plugin = build_plugin(module)
    plugin._auto_purchase = True
    plugin._keep_ingredient_stock = 0
    plugin._buy_budget_per_day = 5000
    plugin._state["customer_plan"] = [_waiting(7)]
    plugin._state["dishes"] = _demand_dishes()
    plugin._state["ingredients"] = {}
    plugin._state["ingredient_meta"] = {
        "tomato": {"price": 10},
        "egg": {"price": 5},
    }

    served = asyncio.run(_serve_loop_of(plugin)(ActionRequestUtils()))

    actions = ActionRequestUtils.actions
    assert served == 1
    assert actions.count("purchase_ingredient") == 2
    assert actions.count("serve_customer") == 1
    first_serve = actions.index("serve_customer")
    assert actions.index("purchase_ingredient") < first_serve

    serve_bodies = [
        body for action, body in ActionRequestUtils.calls if action == "serve_customer"
    ]
    assert serve_bodies[0]["customer_id"] == "7"
    assert serve_bodies[0]["dish_key"] == "tomato_egg"


def test_serve_loop_respects_max_customers_per_run(monkeypatch):
    """max_customers_per_run=N → serve_customer 恰好 N 次。"""
    module = load_plugin_module()
    ActionRequestUtils.reset(
        fetch_payload=make_fetch_payload(
            session_status="open", customers_remaining=0, queued_customers=0
        )
    )
    _patch_sleep(monkeypatch, module)
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._max_customers_per_run = 3
    plugin._state["dishes"] = {
        "tomato_egg": {
            "key": "tomato_egg",
            "ingredients": [{"key": "tomato", "quantity": 1}],
        }
    }
    plugin._state["ingredients"] = {"tomato": 99}
    plugin._state["customer_plan"] = [_waiting(i) for i in range(1, 8)]

    served = asyncio.run(_serve_loop_of(plugin)(ActionRequestUtils()))

    assert served == 3
    assert ActionRequestUtils.actions.count("serve_customer") == 3


def test_serve_loop_handles_expired_customer_once(monkeypatch):
    """过期顾客 → customer_left 恰好一次、customers_left+1、不重复。"""
    module = load_plugin_module()
    ActionRequestUtils.reset(
        fetch_payload=make_fetch_payload(
            session_status="open", customers_remaining=0, queued_customers=0
        )
    )
    _patch_sleep(monkeypatch, module)
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._state["dishes"] = _demand_dishes()
    plugin._state["ingredients"] = {"tomato": 5, "egg": 5}
    plugin._state["customer_plan"] = [
        _waiting(11, deadline=time.time() - 100)
    ]

    served = asyncio.run(_serve_loop_of(plugin)(ActionRequestUtils()))

    assert served == 0
    assert ActionRequestUtils.actions.count("customer_left") == 1
    assert plugin._daily["customers_left"] == 1


def test_serve_loop_failure_bounded_by_max_iterations(monkeypatch):
    """serve 恒失败 → POST 次数受 max_iterations 约束（不死循环），状态不崩溃。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="open",
            customers_remaining=5,
            queued_customers=0,
            customer_plan=[_waiting(21)],
            inventory_ingredients={"tomato": 5, "egg": 5},
        ),
        responses={
            "serve_customer": [
                FakeResponse(payload={"success": False, "message": "上菜失败"})
                for _ in range(10)
            ]
        },
    )
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._auto_start_session = False
    plugin._max_iterations = 3

    result = asyncio.run(plugin.run_restaurant_task())

    assert result["status"] != "error"
    attempts = ActionRequestUtils.actions.count("serve_customer")
    assert 1 <= attempts <= plugin._max_iterations


def test_serve_loop_continues_after_single_failure_spec(monkeypatch):
    """单个顾客上菜失败应记录后继续（不中断整轮），第 2 位仍被成功上菜。

    构造 2 位顾客，第 1 位 serve 返回 success=False；断言第 2 位仍被 serve 且
    served==1、未崩溃（无异常，返回 int）。
    """
    module = load_plugin_module()
    ActionRequestUtils.reset(
        fetch_payload=make_fetch_payload(
            session_status="open", customers_remaining=0, queued_customers=0
        ),
        responses={
            "serve_customer": [
                FakeResponse(payload={"success": False, "message": "上菜失败"}),
                FakeResponse(payload={"success": True}),
                FakeResponse(payload={"success": True}),
            ]
        },
    )
    _patch_sleep(monkeypatch, module)
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._state["dishes"] = {
        "tomato_egg": {
            "key": "tomato_egg",
            "ingredients": [{"key": "tomato", "quantity": 1}],
        }
    }
    plugin._state["ingredients"] = {"tomato": 99}
    plugin._state["customer_plan"] = [_waiting(41), _waiting(42)]

    served = asyncio.run(_serve_loop_of(plugin)(ActionRequestUtils()))

    # 第一个失败后仍尝试第二个 → 成功上菜 1 位，且两位顾客都被尝试过。
    assert served == 1
    assert isinstance(served, int)
    attempted = [
        body["customer_id"]
        for action, body in ActionRequestUtils.calls
        if action == "serve_customer"
    ]
    assert attempted == ["41", "42"]


def test_serve_loop_polls_when_only_queued(monkeypatch):
    """仅排队顾客时轮询后继续，循环次数受 max_iterations 约束。"""
    module = load_plugin_module()
    sleeps: list = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(module.asyncio, "sleep", fake_sleep)
    ActionRequestUtils.reset(
        fetch_payload=make_fetch_payload(
            session_status="open", customers_remaining=0, queued_customers=3
        )
    )
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._max_iterations = 2

    served = asyncio.run(_serve_loop_of(plugin)(ActionRequestUtils()))

    assert served == 0
    assert ActionRequestUtils.calls.count(("GET", {})) == 2
    assert sleeps == [plugin.POLL_SECONDS, plugin.POLL_SECONDS]


def test_min_serve_delay_sleep(monkeypatch):
    """min_serve_delay_seconds 作为下限：每客 sleep ∈ [max(min, cook/stations), 120]。"""
    module = load_plugin_module()
    sleeps = _patch_sleep(monkeypatch, module)
    ActionRequestUtils.reset(
        fetch_payload=make_fetch_payload(
            session_status="open", customers_remaining=0, queued_customers=0
        )
    )
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._min_serve_delay_seconds = 7
    plugin._state["unlocked"] = {"stations": 1}
    plugin._state["dishes"] = _demand_dishes()
    plugin._state["ingredients"] = {"tomato": 5, "egg": 5}
    plugin._state["customer_plan"] = [_waiting(31)]

    served = asyncio.run(_serve_loop_of(plugin)(ActionRequestUtils()))

    assert served == 1
    assert len(sleeps) == 1
    dish = plugin._state["dishes"]["tomato_egg"]
    stations = max(1, plugin._state["unlocked"]["stations"])
    floor = max(plugin._min_serve_delay_seconds, float(dish.get("cook_time") or 0) / stations)
    for delay in sleeps:
        assert delay >= floor + plugin.PREP_DELAY_MIN + plugin.PLATE_DELAY_MIN
        assert delay <= 240


def test_serve_delay_uses_cook_time_over_stations(monkeypatch):
    """cook_time=8、灶台=2 → 无抖动时 base == 4（并行烹饪）。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._min_serve_delay_seconds = 0
    plugin._state["unlocked"] = {"stations": 2}
    monkeypatch.setattr(module.random, "uniform", lambda a, b: a)
    serve_delay = getattr(plugin, "_SiQiRestaurantV3__serve_delay")

    # 烹饪 8/2=4，拖拽 1.5，上菜 2 → 7.5
    assert serve_delay({"cook_time": 8}) == 7.5


def test_serve_delay_floor_min(monkeypatch):
    """min=5、cook=4、灶台=1 → 结果不小于 min（下限生效）。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._min_serve_delay_seconds = 5
    plugin._state["unlocked"] = {"stations": 1}
    monkeypatch.setattr(module.random, "uniform", lambda a, b: a)
    serve_delay = getattr(plugin, "_SiQiRestaurantV3__serve_delay")

    # 下限 5 + 拖拽 1.5 + 上菜 2 = 8.5
    assert serve_delay({"cook_time": 4}) >= 8.5


def test_serve_delay_includes_prep_and_plate(monkeypatch):
    """拖拽食材与上菜取上界：cook=4/1 灶 → 4+3+4=11。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._min_serve_delay_seconds = 0
    plugin._state["unlocked"] = {"stations": 1}
    monkeypatch.setattr(module.random, "uniform", lambda a, b: b)
    serve_delay = getattr(plugin, "_SiQiRestaurantV3__serve_delay")

    assert serve_delay({"cook_time": 4}) == 11.0


def test_serve_delay_varies_with_cook_time(monkeypatch):
    """同灶台、同随机下，cook_time 9 与 4 的延迟差约 5 秒。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._min_serve_delay_seconds = 0
    plugin._state["unlocked"] = {"stations": 1}
    monkeypatch.setattr(module.random, "uniform", lambda a, b: (a + b) / 2.0)
    serve_delay = getattr(plugin, "_SiQiRestaurantV3__serve_delay")

    diff = serve_delay({"cook_time": 9}) - serve_delay({"cook_time": 4})
    assert abs(diff - 5.0) < 0.01


def test_serve_pacing_called_per_serve(monkeypatch):
    """成功上菜 N 次 → sleep 恰好 N 次且参数均 >0。"""
    module = load_plugin_module()
    sleeps = _patch_sleep(monkeypatch, module)
    ActionRequestUtils.reset(
        fetch_payload=make_fetch_payload(
            session_status="open", customers_remaining=0, queued_customers=0
        )
    )
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._max_customers_per_run = 3
    plugin._state["unlocked"] = {"stations": 1}
    plugin._state["dishes"] = {
        "tomato_egg": {
            "key": "tomato_egg",
            "cook_time": 4,
            "ingredients": [{"key": "tomato", "quantity": 1}],
        }
    }
    plugin._state["ingredients"] = {"tomato": 99}
    plugin._state["customer_plan"] = [_waiting(i) for i in range(1, 6)]

    served = asyncio.run(_serve_loop_of(plugin)(ActionRequestUtils()))

    assert served == 3
    assert len(sleeps) == 3
    assert all(delay > 0 for delay in sleeps)


def test_purchase_action_delay(monkeypatch):
    """每次采购成功后都会 await 一个 ACTION_DELAY 区间内的短间隔。"""
    module = load_plugin_module()
    sleeps = _patch_sleep(monkeypatch, module)
    ActionRequestUtils.reset()
    plugin = build_plugin(module)
    plugin._auto_purchase = True
    plugin._keep_ingredient_stock = 0
    plugin._buy_budget_per_day = 5000
    plugin._state["dishes"] = _demand_dishes()
    plugin._state["ingredients"] = {}
    plugin._state["ingredient_meta"] = {
        "tomato": {"price": 10},
        "egg": {"price": 5},
    }

    spent = asyncio.run(
        getattr(plugin, "_SiQiRestaurantV3__purchase_ingredients")(
            ActionRequestUtils(), [_waiting(7)]
        )
    )

    assert ActionRequestUtils.actions.count("purchase_ingredient") == 2
    assert spent > 0
    assert len(sleeps) == 2
    assert all(
        plugin.ACTION_DELAY_MIN <= delay <= plugin.ACTION_DELAY_MAX
        for delay in sleeps
    )


def test_serve_success_deducts_local_ingredients(monkeypatch):
    """serve 回包不含 inventory 时，本地库存仍按配方扣减（P1-2）。"""
    module = load_plugin_module()
    # fetch 返回 None：避免轮末 refetch 把库存覆盖为空
    ActionRequestUtils.reset(fetch_payload=None)
    _patch_sleep(monkeypatch, module)
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._state["dishes"] = _demand_dishes()
    plugin._state["ingredients"] = {"tomato": 5, "egg": 5}
    plugin._state["customer_plan"] = [_waiting(1)]
    plugin._max_iterations = 1

    served = asyncio.run(_serve_loop_of(plugin)(ActionRequestUtils()))

    assert served == 1
    assert plugin._state["ingredients"] == {"tomato": 4, "egg": 3}


def test_execute_status_partial_when_remaining_after_serve(monkeypatch):
    """剩余>0 且本轮 served>0 → 收尾状态 partial（P1-3）。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="open",
            customers_remaining=2,
            queued_customers=0,
            customer_plan=[_waiting(1)],
            inventory_ingredients={"tomato": 5, "egg": 5},
        ),
    )
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._auto_start_session = False
    plugin._auto_hire_staff = False
    plugin._auto_visit = False
    plugin._max_iterations = 1

    result = _run_execute(plugin)

    assert result["status"] == "partial", result
    assert "停止原因" in result["message"]


def test_execute_status_failed_when_no_serve_and_remaining(monkeypatch):
    """剩余>0 且本轮 served==0 → 收尾状态 failed（P1-3）。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="open",
            customers_remaining=2,
            queued_customers=0,
            customer_plan=[_waiting(1)],
            inventory_ingredients={},
        ),
    )
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._auto_start_session = False
    plugin._auto_hire_staff = False
    plugin._auto_visit = False
    plugin._max_iterations = 1

    result = _run_execute(plugin)

    assert result["status"] == "failed", result
    assert "停止原因" in result["message"]


def test_action_order_serve_before_hire(monkeypatch):
    """N1：解锁+魔力充足时，serve_customer 出现在 hire_staff 之前。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="open",
            customers_remaining=0,
            queued_customers=0,
            magic=1000,
            customer_plan=[_waiting(1)],
            inventory_ingredients={"tomato": 5, "egg": 5},
            staff={
                "inventory": {"chef": 0, "server": 0},
                "assignments": [],
                "assignments_map": [],
                "assigned_counts": {"chef": 0, "server": 0},
                "capacity": {"chef": 1, "server": 0},
                "available": {"chef": 0, "server": 0},
                "next_prices": {"chef": 100, "server": None},
                "unlocked": True,
            },
        ),
    )
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._auto_start_session = False
    plugin._auto_visit = False
    plugin._auto_hire_staff = True

    asyncio.run(plugin.run_restaurant_task())

    actions = ActionRequestUtils.actions
    assert "serve_customer" in actions, actions
    assert "hire_staff" in actions, actions
    assert actions.index("serve_customer") < actions.index("hire_staff"), actions


def test_api_serve_now_ready_does_not_start_session(monkeypatch):
    """N3：api_serve_now 只提交后台一次性任务，不同步执行、不触发 start_session。"""
    module = load_plugin_module()
    scheduler = _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="ready",
            customers_total=2,
            customers_remaining=0,
            queued_customers=0,
        ),
    )
    plugin = build_plugin(module)

    result = asyncio.run(plugin.api_serve_now({"count": 3}))

    assert ActionRequestUtils.calls == []
    assert scheduler.added
    assert "api_serve_now" in scheduler.added[0][0]
    assert set(result) == {"success", "message", "data"}
    assert result["success"] is True


def test_run_quick_serve_task_limited_and_no_start(monkeypatch):
    """run_quick_serve_task：最多上 MAX_QUICK_SERVES 位、不开店、silent=True。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="open",
            customers_total=5,
            customers_remaining=0,
            queued_customers=0,
            customer_plan=[_waiting(i) for i in range(1, 6)],
            inventory_ingredients={"tomato": 99, "egg": 99},
        ),
    )
    plugin = build_plugin(module)
    plugin._auto_purchase = False
    plugin._auto_start_session = False

    result = asyncio.run(plugin.run_quick_serve_task())

    served_calls = ActionRequestUtils.actions.count("serve_customer")
    assert 1 <= served_calls <= plugin.MAX_QUICK_SERVES, served_calls
    assert "start_session" not in ActionRequestUtils.actions
    assert result["silent"] is True


def test_expired_customer_ids_only_waiting():
    """__expired_customer_ids：queued 不返回、waiting 超时才返回（P2-7）。"""
    module = load_plugin_module()
    expired = _static(module, "expired_customer_ids")
    now = 1_000_000_000.0
    plan = [
        {"id": 1, "status": "waiting", "deadline": now - 10},
        {"id": 2, "status": "queued", "deadline": now - 10},
        {"id": 3, "status": "waiting", "deadline": now + 10},
        {"id": 4, "status": "waiting", "deadline": None},
    ]

    assert expired(plan, now) == [1]


def test_deadline_ts_units_and_iso():
    """__deadline_ts：秒/毫秒/ISO/非法 四种（P1-5）。"""
    module = load_plugin_module()
    deadline_ts = _static(module, "deadline_ts")

    assert deadline_ts(1700000000) == 1700000000.0
    assert deadline_ts("1700000000") == 1700000000.0
    assert deadline_ts(1700000000000) == 1700000000.0
    assert deadline_ts("1700000000000") == 1700000000.0

    iso = "2026-11-09T15:00:00+08:00"
    expected = datetime.fromisoformat(iso).timestamp()
    assert abs(deadline_ts(iso) - expected) < 1e-6

    assert deadline_ts("not-a-date") is None
    assert deadline_ts(None) is None
    assert deadline_ts("") is None


# ---------------------------------------------------------------------------
# 下馆子
# ---------------------------------------------------------------------------


def test_remaining_visits_unit():
    """插件上限与站点 remaining 取较小值；limit<=0 表示关闭下馆子直接返回 0。"""
    module = load_plugin_module()
    remaining = _static(module, "remaining_visits")

    assert remaining(0, 3, None) == 3
    assert remaining(2, 3, None) == 1
    assert remaining(0, 3, 5) == 3
    assert remaining(0, 3, 1) == 1
    assert remaining(0, 0, 5) == 0
    assert remaining(0, 0, None) == 0
    assert remaining(0, 3, "bad") == 3
    assert remaining(0, 3, -1) == 0


def test_parse_visit_targets():
    """逗号/中文逗号/分号/空白分隔去重。"""
    module = load_plugin_module()
    parse = _static(module, "parse_visit_targets")

    assert parse("alice,bob，carol;dave alice") == [
        "alice",
        "bob",
        "carol",
        "dave",
    ]
    assert parse("") == []


def test_do_visits_sends_confirm_yes_and_limit(monkeypatch):
    """按剩余次数上限下馆子，confirm='yes'，目标正确。"""
    module = load_plugin_module()
    ActionRequestUtils.reset(
        responses={
            "visit_restaurant": [
                FakeResponse(payload={"success": True}) for _ in range(5)
            ]
        }
    )
    plugin = build_plugin(module)
    plugin._visit_count_limit = 3
    plugin._visit_targets = "alice,bob,carol"
    plugin._daily["visits"] = 0
    plugin._state["visits"] = {"remaining": 2, "limit": 3}

    done = asyncio.run(
        getattr(plugin, "_SiQiRestaurantV3__do_visits")(ActionRequestUtils())
    )

    bodies = [
        body for action, body in ActionRequestUtils.calls if action == "visit_restaurant"
    ]
    assert done == 2
    assert len(bodies) == 2
    assert all(body.get("confirm") == "yes" for body in bodies)
    assert [body["target_username"] for body in bodies] == ["alice", "bob"]
    assert plugin._daily["visits"] == 2


def test_do_visits_no_targets_no_visit(monkeypatch):
    """无目标时只拉列表，不调用 visit_restaurant；剩余为 0 时完全不动。"""
    module = load_plugin_module()
    ActionRequestUtils.reset(
        responses={
            "list_visitable_restaurants": FakeResponse(
                payload={"visits": {"records": []}}
            )
        }
    )
    plugin = build_plugin(module)
    plugin._visit_targets = ""
    plugin._daily["visits"] = 0
    plugin._state["visits"] = {"remaining": 3, "limit": 3}

    done = asyncio.run(
        getattr(plugin, "_SiQiRestaurantV3__do_visits")(ActionRequestUtils())
    )

    assert done == 0
    assert "visit_restaurant" not in ActionRequestUtils.actions

    ActionRequestUtils.reset()
    plugin._daily["visits"] = 3
    plugin._state["visits"] = {"remaining": 3, "limit": 3}
    done = asyncio.run(
        getattr(plugin, "_SiQiRestaurantV3__do_visits")(ActionRequestUtils())
    )
    assert done == 0
    assert ActionRequestUtils.calls == []


# ---------------------------------------------------------------------------
# 雇佣
# ---------------------------------------------------------------------------


def test_hire_plan_locked_and_unlocked():
    """未解锁返回 {}；解锁后按 gap 与魔力预算给数量。"""
    module = load_plugin_module()
    hire = _static(module, "hire_plan")

    assert hire({"unlocked": False}, 1000) == {}

    staff = {
        "unlocked": True,
        "capacity": {"chef": 3, "server": 2},
        "available": {"chef": 1, "server": 2},
        "next_prices": {"chef": 100, "server": 50},
    }
    assert hire(staff, 500) == {"chef": 2}
    assert hire(staff, 150) == {"chef": 1}
    assert hire(staff, 0) == {}

    no_price = dict(staff, next_prices={"chef": None, "server": None})
    assert hire(no_price, 1000) == {}


# ---------------------------------------------------------------------------
# 每日归零 / 日期对齐
# ---------------------------------------------------------------------------


def test_roll_daily_across_days():
    """跨自然日重置当日计数，同日保持不变。"""
    module = load_plugin_module()
    roll = _static(module, "roll_daily")

    daily = {
        "date": "2026-10-27",
        "sessions": 1,
        "customers_served": 7,
        "customers_left": 2,
        "magic_spent": 600,
        "magic_earned": 300,
        "dishes": {"tomato_egg": 4},
        "visits": 2,
        "last_result": "旧结果",
        "history": [{"detail": "旧记录"}],
    }

    state = roll(daily, "2026-10-28")

    assert state["date"] == "2026-10-28"
    assert state["sessions"] == 0
    assert state["customers_served"] == 0
    assert state["customers_left"] == 0
    assert state["magic_spent"] == 0
    assert state["visits"] == 0
    assert not state["dishes"]
    assert not state["history"]

    same_day = {"date": "2026-10-28", "sessions": 3, "customers_served": 5}
    assert roll(same_day, "2026-10-28")["sessions"] == 3


def test_sync_daily_date_aligns():
    """__sync_daily_date 与 server cur_date 对齐，跨日归零，空日期不动。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._daily = {
        "date": "2026-10-27",
        "sessions": 5,
        "customers_served": 9,
        "customers_left": 1,
        "magic_spent": 100,
        "magic_earned": 0,
        "dishes": {},
        "visits": 0,
        "last_result": "",
        "history": [],
    }
    sync = getattr(plugin, "_SiQiRestaurantV3__sync_daily_date")

    sync("2026-10-28")
    assert plugin._daily["date"] == "2026-10-28"
    assert plugin._daily["sessions"] == 0

    plugin._daily["sessions"] = 2
    sync("2026-10-28")
    assert plugin._daily["sessions"] == 2

    sync("")
    assert plugin._daily["date"] == "2026-10-28"


# ---------------------------------------------------------------------------
# API / 命令 / 服务
# ---------------------------------------------------------------------------


def test_api_endpoints_all_envelopes(monkeypatch):
    """7 个 API 端点全部返回恰好 {success,message,data} 三键。"""
    module = load_plugin_module()
    _patch_sleep(monkeypatch, module)
    scheduler = StubScheduler()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    monkeypatch.setattr(module, "scheduler_sdk", scheduler)
    ActionRequestUtils.reset(
        fetch_payload=make_fetch_payload(
            session_status="ready",
            customers_total=0,
            visits={
                "count": 0,
                "limit": 3,
                "remaining": 0,
                "records": [],
                "reached_limit": False,
                "pending_visitors": {"count": 0, "list": []},
            },
        )
    )
    plugin = build_plugin(module)
    payloads = {
        "/run": None,
        "/status": None,
        "/serve_now": {},
        "/sync": None,
        "/set_option": {"key": "dry_run", "value": "true"},
        "/visit": None,
        "/hire": None,
    }

    apis = plugin.get_api()
    assert len(apis) == 7
    for api in apis:
        plugin._lock = None
        payload = payloads[api["path"]]
        if payload is None:
            response = asyncio.run(api["endpoint"]())
        else:
            response = asyncio.run(api["endpoint"](payload))
        assert set(response) == {"success", "message", "data"}, api["path"]
        assert isinstance(response["message"], str), api["path"]
        assert isinstance(response["data"], dict), api["path"]


def test_run_api_registers_once_job_no_request(monkeypatch):
    """``/run`` 只注册一次性任务，不在接口内发请求。"""
    module = load_plugin_module()
    scheduler = StubScheduler()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    monkeypatch.setattr(module, "scheduler_sdk", scheduler)
    ActionRequestUtils.reset(fetch_payload=make_fetch_payload())
    plugin = build_plugin(module)

    result = asyncio.run(plugin.run_once_api())

    assert result["success"] is True
    assert ActionRequestUtils.calls == []
    assert scheduler.added
    args = scheduler.added[0][0]
    assert "api_run_once" in args


def test_api_locked_returns_running_shell(monkeypatch):
    """运行锁被占用时返回 running 外壳。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    ActionRequestUtils.reset(fetch_payload=make_fetch_payload())
    plugin = build_plugin(module)
    _lock_plugin(plugin)

    result = asyncio.run(plugin.run_once_api())

    assert set(result) == {"success", "message", "data"}
    assert result["success"] is False
    assert result["data"].get("status") == "running"
    assert ActionRequestUtils.calls == []


def test_set_option_whitelist_writes_full_config(monkeypatch):
    """白名单键合法写入并整体回写配置，未改动键原样保留。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._config_snapshot = make_config(
        schedule_mode="cron",
        cron="7 */2 * * *",
        cookie="c_secure_pass=token",
        interval_minutes=61,
    )

    result = asyncio.run(
        plugin.api_set_option({"key": "dry_run", "value": "true"})
    )

    assert result["success"] is True
    assert plugin._dry_run is True
    assert plugin._updated_configs
    saved = plugin._updated_configs[-1]
    assert saved["cookie"] == "c_secure_pass=token"
    assert saved["cron"] == "7 */2 * * *"
    assert saved["schedule_mode"] == "cron"
    assert saved["interval_minutes"] == 61
    assert saved["dry_run"] is True

    result = asyncio.run(
        plugin.api_set_option({"key": "serve_strategy", "value": "cheapest"})
    )
    assert result["success"] is True
    assert plugin._serve_strategy == "cheapest"


def test_set_option_rejects_blocked_and_unknown(monkeypatch):
    """blocked/未知键拒绝并提示“配置页”，不触发回写。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    for key in (
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
        "whatever",
    ):
        result = asyncio.run(plugin.api_set_option({"key": key, "value": "1"}))
        assert result["success"] is False, key
        assert "配置页" in result["message"], (key, result["message"])

    assert plugin._updated_configs == []


def test_stop_service_removes_jobs(monkeypatch):
    """stop_service 移除 ONCE_JOB_IDS 全部任务并清空锁。"""
    module = load_plugin_module()
    scheduler = StubScheduler()
    monkeypatch.setattr(module, "scheduler_sdk", scheduler)
    plugin = build_plugin(module)
    plugin._lock = object()

    plugin.stop_service()
    plugin.stop_service()

    removed_ids = [args[1] for args, _ in scheduler.removed]
    expected = list(module.SiQiRestaurantV3.ONCE_JOB_IDS)
    assert removed_ids[: len(expected)] == expected
    assert plugin._lock is None


def test_command_filters_action(monkeypatch):
    """远程命令只响应 siqi_restaurant action。"""
    module = load_plugin_module()
    scheduler = StubScheduler()
    monkeypatch.setattr(module, "scheduler_sdk", scheduler)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    plugin = build_plugin(module)
    event_type = getattr(module.EventType, "PluginAction", None)
    matched = SimpleNamespace(
        event_type=event_type, event_data={"action": "siqi_restaurant"}
    )
    other = SimpleNamespace(event_type=event_type, event_data={"action": "other"})

    asyncio.run(plugin.run_once_command(matched))
    assert len(scheduler.added) == 1

    asyncio.run(plugin.run_once_command(other))
    assert len(scheduler.added) == 1


# ---------------------------------------------------------------------------
# 每日调度 / 补查 / 当日闸门
# ---------------------------------------------------------------------------


def test_get_service_daily_and_retry(monkeypatch):
    """cron 模式默认返回主任务 + 补查两个服务，id 与 trigger 均正确。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    plugin = build_plugin(module)
    plugin._enabled = True
    plugin._schedule_mode = "cron"
    plugin._cron = "12 10 * * *"
    plugin._retry_enabled = True
    plugin._retry_cron = "12 15 * * *"

    services = plugin.get_service()

    assert len(services) == 2
    cls_name = module.SiQiRestaurantV3.__name__
    assert services[0]["id"] == cls_name
    assert services[1]["id"] == f"{cls_name}_retry"
    assert services[0]["trigger"] is not None
    assert services[1]["trigger"] is not None


def test_get_service_retry_disabled(monkeypatch):
    """_retry_enabled=False 时只返回主服务。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    plugin = build_plugin(module)
    plugin._enabled = True
    plugin._schedule_mode = "cron"
    plugin._cron = "12 10 * * *"
    plugin._retry_enabled = False

    services = plugin.get_service()

    assert len(services) == 1


def test_daily_gate_skips_without_network(monkeypatch):
    """当日已营业完成时，缓存命中直接静默跳过且不发任何请求。"""
    module = load_plugin_module()
    _stub_runtime(monkeypatch, module)
    plugin = build_plugin(module)
    today = _today(module)
    plugin._state["cur_date"] = today
    plugin._state["session_status"] = "closed"
    plugin._daily["date"] = today
    plugin._daily["sessions"] = 1

    result = _run_execute(plugin)

    assert result["status"] == "completed"
    assert result.get("silent") is True
    assert ActionRequestUtils.calls == []


def test_daily_gate_skips_after_fetch(monkeypatch):
    """缓存状态未同步时，fetch 后发现当日已完成同样静默跳过。"""
    module = load_plugin_module()
    today = _today(module)
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="closed",
            cur_date=today,
            customers_total=5,
            customers_served=5,
            customers_remaining=0,
        ),
    )
    plugin = build_plugin(module)
    plugin._state["cur_date"] = ""  # 缓存状态日期非今天，首道闸门不命中
    plugin._daily["date"] = today
    plugin._daily["sessions"] = 1

    result = _run_execute(plugin)

    assert result["status"] == "completed"
    assert result.get("silent") is True
    actions = ActionRequestUtils.actions
    assert "start_session" not in actions
    assert "visit_restaurant" not in actions
    assert "hire_staff" not in actions


def test_ensure_session_not_repeat_same_day(monkeypatch):
    """当日已开过店（sessions>=1 且同日）时，ready 也不再 POST start_session。"""
    module = load_plugin_module()
    today = _today(module)
    _stub_runtime(
        monkeypatch,
        module,
        fetch_payload=make_fetch_payload(
            session_status="ready",
            customers_total=3,
            customers_remaining=0,
        ),
    )
    plugin = build_plugin(module)
    plugin._state["cur_date"] = today
    plugin._state["session_status"] = "ready"
    plugin._daily["date"] = today
    plugin._daily["sessions"] = 1

    _run_execute(plugin)

    assert "start_session" not in ActionRequestUtils.actions


def test_finish_task_silent_no_notification(monkeypatch):
    """result["silent"] 为真时不发通知，否则正常通知。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    calls: list = []
    plugin.post_message = lambda **kwargs: calls.append(kwargs)
    plugin._notify = True
    finish = getattr(plugin, "_SiQiRestaurantV3__finish_task")

    finish(
        {
            "status": "completed",
            "message": "今日已营业完成，跳过本次触发",
            "silent": True,
            "data": {},
        }
    )
    assert calls == []

    finish({"status": "completed", "message": "正常完成", "data": {}})
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# 详情页
# ---------------------------------------------------------------------------

PAGE_COMPONENT_WHITELIST = {
    "VCard",
    "VCardTitle",
    "VCardText",
    "VRow",
    "VCol",
    "VAlert",
    "VChip",
    "VIcon",
    "VAvatar",
    "VTable",
    "VSpacer",
    "VBtn",
    "VLabel",
    "div",
    "span",
    "thead",
    "tbody",
    "tr",
    "th",
    "td",
}


def _iter_nodes(node):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_nodes(item)


def _collect_components(node, found):
    for item in _iter_nodes(node):
        component = item.get("component")
        if isinstance(component, str):
            found.add(component)
    return found


def _page_button_events(page):
    for node in _iter_nodes(page):
        events = node.get("events")
        if not isinstance(events, dict):
            continue
        for handler in events.values():
            if isinstance(handler, dict):
                yield node, handler


def test_page_components_whitelisted():
    """详情页组件类型都在宿主已注册白名单内。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._state.update(
        {
            "popularity": 107,
            "magic": 1200,
            "customers_served": 3,
            "customers_total": 14,
            "customers_remaining": 8,
            "session_status": "open",
            "ingredients": {"tomato": 2, "egg": 4},
            "logs": [
                {
                    "log_date": "2026-11-09",
                    "customers_served": 3,
                    "customers_total": 14,
                    "popularity_change": 5,
                    "net_earnings": 200,
                }
            ],
        }
    )

    found = _collect_components(plugin.get_page(), set())

    assert found
    assert found <= PAGE_COMPONENT_WHITELIST, found - PAGE_COMPONENT_WHITELIST


def test_page_hero_gradient():
    """Hero 渐变由原生 div 承载，避免白底白字。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    hero = plugin.get_page()[0]

    assert hero["component"] == "div"
    hero_style = hero["props"]["style"]
    assert "linear-gradient" in hero_style
    assert "color: #ffffff" in hero_style


def test_page_button_api_prefix():
    """详情页所有按钮事件都指向本插件的 API 路径。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    page = plugin.get_page()

    apis = [
        handler["api"]
        for _, handler in _page_button_events(page)
        if isinstance(handler.get("api"), str)
    ]

    assert apis, "详情页必须提供可点击的操作按钮"
    for api in apis:
        assert api.startswith("plugin/SiQiRestaurantV3/"), api
        assert "apikey=" not in api, api


def _log_table_of(plugin):
    """取插件内部经营记录表构造函数。"""
    return getattr(plugin, "_SiQiRestaurantV3__log_table")


def _log_header_texts(table):
    """从 VTable 的 thead 提取表头文字。"""
    for node in _iter_nodes(table):
        if node.get("component") == "thead":
            return [th.get("text") for th in node["content"][0]["content"]]
    return []


def _log_body_rows(table):
    """从 VTable 的 tbody 提取数据行。"""
    for node in _iter_nodes(table):
        if node.get("component") == "tbody":
            return list(node.get("content") or [])
    return []


def test_log_table_plugin_history_headers():
    """daily.history（time/action/detail）按 3 列渲染，不再错位为 4 列。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    history = [
        {"time": "10:00:01", "action": "open", "detail": "已开启今日营业"},
        {"time": "10:05:02", "action": "serve", "detail": "上菜 tomato_egg 给顾客 1"},
        {"time": "10:06:03", "action": "run", "detail": "本轮完成"},
    ]

    table = _log_table_of(plugin)(history)

    assert _log_header_texts(table) == ["时间", "动作", "详情"]
    rows = _log_body_rows(table)
    assert len(rows) == 3
    # 逆序展示：最新一条在最前
    assert [cell["text"] for cell in rows[0]["content"]] == [
        "10:06:03",
        "run",
        "本轮完成",
    ]
    chip_texts = [
        node.get("text")
        for node in _iter_nodes(table)
        if node.get("component") == "VChip"
    ]
    assert any("3 条" in str(text) for text in chip_texts)
    assert not any("1 条" in str(text) for text in chip_texts)


def test_log_table_site_logs_headers():
    """site logs（log_date/customers_served）保持 4 列渲染。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    history = [
        {
            "log_date": "2026-11-09",
            "customers_served": 3,
            "customers_total": 14,
            "popularity_change": 5,
            "net_earnings": 200,
        }
    ]

    table = _log_table_of(plugin)(history)

    assert _log_header_texts(table) == ["日期", "顾客(服务/总)", "受欢迎变化", "净收益"]
    rows = _log_body_rows(table)
    assert len(rows) == 1
    assert [cell["text"] for cell in rows[0]["content"]] == [
        "2026-11-09",
        "3/14",
        "5",
        "200",
    ]


def test_form_cookie_is_password(monkeypatch):
    """配置页 Cookie 字段使用密码框显示，且回填原值以便保存不清空。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    plugin = build_plugin(module)

    form, model = plugin.get_form()

    assert isinstance(form, list)
    assert isinstance(model, dict)
    cookie_field = None
    for node in _iter_nodes(form):
        props = node.get("props") or {}
        if node.get("component") == "VTextField" and props.get("model") == "cookie":
            cookie_field = node
            break

    assert cookie_field is not None, "配置页应包含 Cookie 字段"
    assert cookie_field["props"].get("type") == "password"
    assert cookie_field["props"].get("value") == model["cookie"]


# ---------------------------------------------------------------------------
# 数据新鲜度 / 未启用提示 / 旧配置迁移
# ---------------------------------------------------------------------------


def _page_text(node):
    """把页面节点里所有 ``text`` 字段拼成一段文本，便于断言。"""
    texts: list = []

    def walk(value):
        if isinstance(value, dict):
            if isinstance(value.get("text"), str):
                texts.append(value["text"])
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(node)
    return "\n".join(texts)


def _find_card(page, marker):
    """从页面顶层卡片中找到含指定标记的那张。"""
    for node in page:
        if isinstance(node, dict) and marker in _page_text(node):
            return node
    return {}


def _set_stale_state(plugin, module):
    """把运行态设为「昨日已打烊、今日尚未经营」的旧快照。"""
    yesterday = (datetime.now(_timezone(module)) - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    plugin._enabled = True
    plugin._state.update(
        {
            "cur_date": yesterday,
            "session_status": "closed",
            "customers_served": 14,
            "customers_total": 14,
            "customers_remaining": 0,
        }
    )
    plugin._daily["date"] = _today(module)
    return yesterday


def test_page_stale_shows_today_not_operated():
    """数据日期为昨天时页面提示今日尚未经营，且不再展示今日顾客标签。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    _set_stale_state(plugin, module)

    text = _page_text(plugin.get_page())

    assert "今日尚未经营" in text
    assert "数据日期" in text
    assert "今日顾客（已服务/总数）" not in text


def test_page_fresh_shows_today_label():
    """cur_date 为今天时保留今日顾客（已服务/总数）标签。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._state["cur_date"] = _today(module)

    text = _page_text(plugin.get_page())

    assert "今日顾客（已服务/总数）" in text


def test_page_disabled_has_warning():
    """插件未启用时页面顶部出现未启用警示。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._enabled = False

    text = _page_text(plugin.get_page())

    assert "插件未启用" in text


def test_page_stale_overview_not_closed_chip():
    """stale 时今日概览状态徽章为待同步而非昨日已打烊。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    _set_stale_state(plugin, module)

    overview = _find_card(plugin.get_page(), "📊 今日概览")
    text = _page_text(overview)

    assert "待同步" in text
    assert "已打烊" not in text


def test_init_migrates_legacy_schedule(monkeypatch):
    """旧版 interval/30 默认调度在 init 时迁移为每天 10:12 营业 + 15:12 补查。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    plugin = build_plugin(module)
    calls: list = []

    def recorder(config):
        calls.append(dict(config))
        plugin._config_snapshot = dict(config)

    monkeypatch.setattr(plugin, "update_config", recorder)

    legacy = {
        "enabled": True,
        "schedule_mode": "interval",
        "interval_minutes": 30,
        "cron": "7 */2 * * *",
    }
    plugin.init_plugin(legacy)

    assert plugin._schedule_mode == "cron"
    assert plugin._cron == "12 10 * * *"
    assert plugin._retry_enabled is True
    assert plugin._retry_cron == "12 15 * * *"
    assert len(calls) == 1
    assert calls[0]["schedule_mode"] == "cron"

    # 再 init 含 retry_enabled 的新配置，不应重复迁移
    plugin.init_plugin(
        {
            "enabled": True,
            "schedule_mode": "cron",
            "cron": "12 10 * * *",
            "retry_enabled": True,
            "retry_cron": "12 15 * * *",
        }
    )
    assert len(calls) == 1


def test_init_does_not_migrate_custom_interval(monkeypatch):
    """自定义间隔或自定义 cron 不触发旧配置迁移。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)

    plugin = build_plugin(module)
    calls: list = []
    monkeypatch.setattr(plugin, "update_config", lambda cfg: calls.append(dict(cfg)))
    plugin.init_plugin(
        {
            "enabled": True,
            "schedule_mode": "interval",
            "interval_minutes": 61,
            "cron": "7 */2 * * *",
        }
    )
    assert plugin._schedule_mode == "interval"
    assert plugin._interval_minutes == 61
    assert calls == []

    plugin2 = build_plugin(module)
    calls2: list = []
    monkeypatch.setattr(plugin2, "update_config", lambda cfg: calls2.append(dict(cfg)))
    plugin2.init_plugin(
        {
            "enabled": True,
            "schedule_mode": "interval",
            "interval_minutes": 30,
            "cron": "0 8 * * *",
        }
    )
    assert plugin2._schedule_mode == "interval"
    assert calls2 == []


def test_api_status_has_freshness_fields():
    """/status 返回数据包含 enabled/today/state_date/is_stale 新鲜度字段。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    response = asyncio.run(plugin.api_status())
    data = response["data"]

    for key in ("enabled", "today", "state_date", "is_stale"):
        assert key in data
    assert data["enabled"] is True
    assert data["today"] == _today(module)


# ---------------------------------------------------------------------------
# standalone runner
# ---------------------------------------------------------------------------

_MISSING = object()

# 与实现已知不一致、按“应然”书写但不计入 standalone 通过率的用例。
XFAIL_TESTS = set()


class _MiniMonkeyPatch:
    """无 pytest 时的最小 monkeypatch 替代，仅支持 setattr。"""

    def __init__(self) -> None:
        self._undo: list = []

    def setattr(self, target, name, value) -> None:
        original = getattr(target, name, _MISSING)
        self._undo.append((target, name, original))
        setattr(target, name, value)

    def undo(self) -> None:
        for target, name, original in reversed(self._undo):
            if original is _MISSING:
                try:
                    delattr(target, name)
                except AttributeError:
                    pass
            else:
                setattr(target, name, original)
        self._undo.clear()


def _run_standalone() -> int:
    """在没有 pytest 的宿主镜像里直接运行本文件的用例。"""
    failures = 0
    skipped = 0
    checks = [
        (name, func)
        for name, func in sorted(globals().items())
        if name.startswith("test_") and callable(func)
    ]
    for name, func in checks:
        if name in XFAIL_TESTS:
            skipped += 1
            print(f"SKIP {name}（已知实现缺陷，按应然断言）")
            continue
        monkeypatch = _MiniMonkeyPatch()
        try:
            if func.__code__.co_argcount:
                func(monkeypatch)
            else:
                func()
            print(f"PASS {name}")
        except Exception as error:  # noqa: BLE001
            failures += 1
            print(f"FAIL {name}: {error}")
        finally:
            monkeypatch.undo()
    runnable = len(checks) - skipped
    print(f"{runnable - failures}/{runnable} passed, {skipped} skipped")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_run_standalone())
