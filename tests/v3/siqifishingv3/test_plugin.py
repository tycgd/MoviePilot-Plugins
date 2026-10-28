"""SiQiFishingV3 纯逻辑测试：不连接宿主数据库、调度器与真实站点。"""

import asyncio
import importlib.util
import inspect
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

PLUGIN_INIT = (
    Path(__file__).resolve().parents[3]
    / "plugins.v3"
    / "siqifishingv3"
    / "__init__.py"
)


def load_plugin_module():
    """按文件路径加载插件模块，避免依赖插件市场安装结果。"""
    spec = importlib.util.spec_from_file_location(
        "siqifishingv3_under_test", PLUGIN_INIT
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    """模拟 HTTPX2 响应对象。"""

    def __init__(self, *, status_code: int = 200, text: str = "", payload=None) -> None:
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self):
        """返回预置 JSON，模拟非 JSON 响应时抛 ValueError。"""
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class ActionRequestUtils:
    """按 action 分发预置响应的假客户端，GET 返回内嵌 FISHING_DATA 的页面。"""

    actions: list = []
    bodies: list = []
    calls: list = []
    responses: dict = {}
    html: str = ""

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    @classmethod
    def reset(cls, responses=None, html: str = "") -> None:
        """清空调用记录并装入新的响应队列与页面 HTML。"""
        cls.actions = []
        cls.bodies = []
        cls.calls = []
        cls.responses = dict(responses or {})
        cls.html = html or ""

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
        return FakeResponse(text=ActionRequestUtils.html)


def _timezone(module):
    """读取插件使用的东八区时区，缺失时回退到 UTC+8。"""
    return getattr(module.SiQiFishingV3, "TZ_CN", timezone(timedelta(hours=8)))


def _today(module) -> str:
    """返回插件时区下的自然日字符串。"""
    return datetime.now(_timezone(module)).strftime("%Y-%m-%d")


def make_config(**overrides) -> dict:
    """构造一份完整的插件配置快照，用于校验 set_option 的整体回写。"""
    config = {
        "enabled": True,
        "notify": False,
        "cookie": "c_secure_pass=token",
        "schedule_mode": "interval",
        "cron": "7 */2 * * *",
        "interval_minutes": 61,
        "max_casts_per_run": 6,
        "daily_cast_limit": 40,
        "cast_interval": 0,
        "dry_run": False,
        "lure_mode": "auto",
        "lure_key": "normal",
        "rod_mode": "auto",
        "rod_key": "mushroom",
        "auto_buy": False,
        "buy_lure_key": "normal",
        "buy_budget_per_day": 0,
        "keep_lures": 20,
        "slider_mode": "perfect",
        "slider_center": 0.5,
        "slider_jitter": 0.03,
        "auto_task": False,
        "auto_guess": False,
        "guess_value": 0,
        "slot_spread": False,
        "per_slot_casts": 8,
        "run_once": False,
    }
    config.update(overrides)
    return config


def build_plugin(module):
    """绕过 _PluginBase.__init__ 构造最小实例。"""
    plugin = object.__new__(module.SiQiFishingV3)
    plugin._enabled = True
    plugin._cookie = "c_secure_pass=token"
    plugin._schedule_mode = "interval"
    plugin._cron = "7 */2 * * *"
    plugin._interval_minutes = 61
    plugin._notify = False

    plugin._dry_run = False
    plugin._max_casts_per_run = 6
    plugin._daily_cast_limit = 40
    plugin._cast_interval = 0

    plugin._lure_mode = "auto"
    plugin._lure_key = "normal"
    plugin._auto_buy = False
    plugin._buy_lure_key = "normal"
    plugin._buy_budget_per_day = 0
    plugin._keep_lures = 20

    plugin._rod_mode = "auto"
    plugin._rod_key = "mushroom"

    plugin._slider_mode = "perfect"
    plugin._slider_center = 0.5
    plugin._slider_jitter = 0.03

    plugin._auto_task = False
    plugin._auto_guess = False
    plugin._guess_value = 0

    plugin._slot_spread = False
    plugin._per_slot_casts = 8

    plugin._lock = None
    plugin._daily = {
        "date": _today(module),
        "casts": 0,
        "catches": 0,
        "misses": 0,
        "miss_rewards": 0,
        "new_discoveries": 0,
        "spent": 0,
        "slots": {},
        "last_result": "",
        "history": [],
    }
    plugin._state = {
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


def make_data(
    *,
    lures=None,
    rods=None,
    selected_lure: str = "normal",
    selected_rod: str = "mushroom",
    attempts_used: int = 0,
    attempts_limit: int = 50,
    catches: int = 0,
    guess_status: str = "",
    tasks=None,
    quiz_status: str = "not_started",
    codex=None,
) -> dict:
    """构造一份与站点页面一致的 FISHING_DATA。"""
    return {
        "user": {"seedbonus": 1000},
        "config": {
            "guessRewardAttempts": 3,
            "lures": {},
            "rods": {},
        },
        "state": {
            "selected_lure": selected_lure,
            "selected_rod": selected_rod,
            "daily_attempts_used": attempts_used,
            "daily_attempts_limit": attempts_limit,
            "daily_catch_count": catches,
            "daily_guess_status": guess_status,
        },
        "inventory": {
            "lures": {"normal": 99} if lures is None else dict(lures),
            "rods": (
                {"mushroom": {"unlocked": True}} if rods is None else dict(rods)
            ),
        },
        "tasks": dict(tasks or {}),
        "knowledge_quiz": {
            "status": quiz_status,
            "current_question": {"question": "1 + 1 = ?", "options": ["1", "2"]},
        },
        "codex": list(codex or []),
    }


def fishing_html(data: dict) -> str:
    """把 FISHING_DATA 内联进页面脚本。"""
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"<html><script>const FISHING_DATA = {payload};</script></html>"


def catch_response(name: str = "鲫鱼", new: bool = False) -> FakeResponse:
    """构造一次成功钓到鱼的响应。"""
    return FakeResponse(
        payload={
            "success": True,
            "result": {
                "status": "catch",
                "timing": "perfect",
                "fish": {"name": name, "rarity_label": "普通"},
                "is_new_discovery": new,
            },
        }
    )


def miss_response() -> FakeResponse:
    """构造一次空钩响应。"""
    return FakeResponse(
        payload={"success": True, "result": {"status": "miss", "timing": "normal"}}
    )


def miss_reward_response() -> FakeResponse:
    """构造一次空钩补偿响应。"""
    return FakeResponse(
        payload={
            "success": True,
            "result": {"status": "miss_reward", "timing": "normal"},
        }
    )


class StubSiteOper:
    """隔离站点数据库查询，始终返回未配置站点。"""

    @staticmethod
    def get_by_domain(domain):
        return None

    @staticmethod
    async def async_get_by_domain(domain):
        return None


def _run_task(module, monkeypatch, *, data=None, responses=None, configure=None):
    """装载假客户端并执行一次 run_fishing_task。"""
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    ActionRequestUtils.reset(
        responses=responses or {},
        html=fishing_html(data if data is not None else make_data()),
    )
    plugin = build_plugin(module)
    plugin._cast_interval = 0
    if configure is not None:
        configure(plugin)
    result = asyncio.run(plugin.run_fishing_task())
    return plugin, result


def _current_slot(module) -> int:
    """取得插件当前所在时段编号，兼容 hour 参数与无参实现。"""
    hour = datetime.now(_timezone(module)).hour
    func = getattr(module.SiQiFishingV3, "_SiQiFishingV3__time_slot")
    try:
        return int(func(hour))
    except TypeError:
        return int(func())


def test_metadata_matches_directory():
    """插件 ID、目录名与市场元数据必须一致。"""
    module = load_plugin_module()
    cls = module.SiQiFishingV3

    assert cls.__name__.lower() == PLUGIN_INIT.parent.name
    assert PLUGIN_INIT.parent.name == "siqifishingv3"
    assert cls.plugin_version == "1.1.0"
    assert cls.plugin_config_prefix == "siqifishingv3_"
    assert cls.plugin_author == "tycgd"
    assert cls.auth_level == 1
    assert cls.plugin_name == "思齐钓鱼"

    package = json.loads((PLUGIN_INIT.parents[2] / "package.v3.json").read_text("utf-8"))
    entry = package[cls.__name__]
    assert entry["version"] == cls.plugin_version
    assert entry["icon"] == cls.plugin_icon
    assert entry["name"] == cls.plugin_name
    assert entry["author"] == "tycgd"
    assert entry["level"] == 1


def test_no_abstract_methods_left():
    """抽象方法必须全部实现，否则宿主无法实例化。"""
    module = load_plugin_module()
    assert module.SiQiFishingV3.__abstractmethods__ == frozenset()


def test_contract_methods_present():
    """V3 生命周期协程与静态纯函数必须齐备。"""
    module = load_plugin_module()
    cls = module.SiQiFishingV3

    for name in (
        "init_plugin",
        "get_state",
        "get_api",
        "get_form",
        "get_page",
        "get_command",
        "get_service",
        "stop_service",
    ):
        assert callable(getattr(cls, name))

    for name in (
        "run_fishing_task",
        "run_once_api",
        "run_once_command",
        "api_status",
    ):
        assert inspect.iscoroutinefunction(getattr(cls, name))

    for name in (
        "__parse_state",
        "__extract_state",
        "__pick_lure",
        "__pick_rod",
        "__slider_value",
        "__allowed_casts",
        "__buy_quantity",
        "__roll_daily",
        "__time_slot",
        "__safe_int",
        "__safe_float",
        "__cookie_to_dict",
        "__game_ok",
        "__timing_label",
    ):
        assert callable(getattr(cls, f"_SiQiFishingV3{name}"))


def test_version_consistency():
    """plugin_version、package.v3.json 与 history 顶部键三处一致。"""
    module = load_plugin_module()
    cls = module.SiQiFishingV3
    package = json.loads((PLUGIN_INIT.parents[2] / "package.v3.json").read_text("utf-8"))
    entry = package[cls.__name__]

    assert cls.plugin_version == "1.1.0"
    assert entry["version"] == cls.plugin_version
    assert next(iter(entry["history"])) == f"v{cls.plugin_version}"


def test_service_id_and_invalid_cron():
    """周期服务 ID 使用运行类名，非法 Cron 返回空列表。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._enabled = True
    plugin._schedule_mode = "interval"

    (service,) = plugin.get_service()
    assert service["id"] == "SiQiFishingV3"

    plugin._schedule_mode = "cron"
    plugin._cron = "definitely not a cron"
    assert plugin.get_service() == []


def test_parse_state_extracts_fishing_data():
    """页面内联的 FISHING_DATA 能被完整解析。"""
    module = load_plugin_module()
    parse = getattr(module.SiQiFishingV3, "_SiQiFishingV3__parse_state")
    data = make_data(lures={"normal": 3, "insect": 1})

    parsed = parse(fishing_html(data))

    assert parsed == data
    assert parsed["inventory"]["lures"]["insect"] == 1


def test_parse_state_rejects_bad_html():
    """缺少标记或 JSON 损坏时返回 None。"""
    module = load_plugin_module()
    parse = getattr(module.SiQiFishingV3, "_SiQiFishingV3__parse_state")

    assert parse("") is None
    assert parse("<html><body>login</body></html>") is None
    assert parse("const FISHING_DATA = {broken") is None


def test_run_task_without_cookie_auth_failed(monkeypatch):
    """缺少 Cookie 时直接认证失败，不发起任何请求。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    ActionRequestUtils.reset(html=fishing_html(make_data()))

    plugin = build_plugin(module)
    plugin._cookie = ""

    result = asyncio.run(plugin.run_fishing_task())

    assert result["status"] == "auth_failed"
    assert ActionRequestUtils.calls == []


def test_run_task_cookie_without_secure_pass_auth_failed(monkeypatch):
    """Cookie 缺少 c_secure_pass 时同样认证失败且不发请求。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    ActionRequestUtils.reset(html=fishing_html(make_data()))

    plugin = build_plugin(module)
    plugin._cookie = "foo=bar"

    result = asyncio.run(plugin.run_fishing_task())

    assert result["status"] == "auth_failed"
    assert ActionRequestUtils.calls == []


def test_slider_perfect_center():
    """完美模式且无抖动时固定落在 0.5。"""
    module = load_plugin_module()
    slider = getattr(module.SiQiFishingV3, "_SiQiFishingV3__slider_value")

    assert slider("perfect", 0.5, 0.0) == 0.5


def test_slider_perfect_jitter_bounds():
    """完美模式带抖动时仍必须落在甜蜜区。"""
    module = load_plugin_module()
    slider = getattr(module.SiQiFishingV3, "_SiQiFishingV3__slider_value")

    samples = [slider("perfect", 0.5, 0.1) for _ in range(100)]

    assert all(0.45 <= value <= 0.55 for value in samples), samples
    assert all(isinstance(value, float) for value in samples)


def test_slider_range_bounds():
    """甜蜜区随机模式采样必须落在 0.45~0.55。"""
    module = load_plugin_module()
    slider = getattr(module.SiQiFishingV3, "_SiQiFishingV3__slider_value")

    samples = [slider("range", 0.5, 0.3) for _ in range(100)]

    assert all(0.45 <= value <= 0.55 for value in samples), samples


def test_slider_custom_bounds():
    """自定义模式按中心±抖动采样，并夹取到 0~1。"""
    module = load_plugin_module()
    slider = getattr(module.SiQiFishingV3, "_SiQiFishingV3__slider_value")

    assert slider("custom", 1.5, 0.0) == 1.0
    assert slider("custom", -0.2, 0.0) == 0.0
    samples = [slider("custom", 0.5, 0.03) for _ in range(100)]
    assert all(0.47 <= value <= 0.53 for value in samples), samples


def test_pick_lure_strategy():
    """自动模式按鱼饵权重挑选有库存的最高分项。"""
    module = load_plugin_module()
    pick = getattr(module.SiQiFishingV3, "_SiQiFishingV3__pick_lure")

    assert pick({"normal": 5, "insect": 2}, "auto", "normal") == "insect"
    assert pick({"normal": 5}, "auto", "normal") == "normal"
    assert pick({}, "auto", "normal") == ""


def test_pick_lure_fixed_without_stock():
    """固定模式指定的鱼饵无库存时返回空串。"""
    module = load_plugin_module()
    pick = getattr(module.SiQiFishingV3, "_SiQiFishingV3__pick_lure")

    assert pick({"normal": 0, "insect": 3}, "fixed", "normal") == ""
    assert pick({"normal": 2}, "fixed", "normal") == "normal"


def test_pick_rod_auto_unlocked():
    """自动模式在已解锁鱼竿中挑选最强竿。"""
    module = load_plugin_module()
    pick = getattr(module.SiQiFishingV3, "_SiQiFishingV3__pick_rod")
    rods = {
        "mushroom": {"unlocked": True},
        "love": {"unlocked": True},
        "star": {"unlocked": True},
    }

    assert pick(rods, "auto", "mushroom") == "star"


def test_pick_rod_all_locked():
    """所有鱼竿均未解锁时返回空串。"""
    module = load_plugin_module()
    pick = getattr(module.SiQiFishingV3, "_SiQiFishingV3__pick_rod")
    rods = {
        "mushroom": {"unlocked": False},
        "love": {"unlocked": False},
        "star": {"unlocked": False},
    }

    assert pick(rods, "auto", "mushroom") == ""


def test_allowed_casts_truncations():
    """单次、每日、时段、站点剩余四重上限同时生效。"""
    module = load_plugin_module()
    allowed = getattr(module.SiQiFishingV3, "_SiQiFishingV3__allowed_casts")

    assert allowed(5, 0, 100, 0, 100, 100) == 5
    assert allowed(100, 7, 10, 0, 100, 100) == 3
    assert allowed(100, 0, 100, 6, 8, 100) == 2
    assert allowed(100, 0, 100, 0, 100, 4) == 4
    assert allowed(100, 50, 10, 0, 100, 100) == 0


def test_buy_quantity_boundaries():
    """补饵数量同时受保底库存与每日预算约束。"""
    module = load_plugin_module()
    buy = getattr(module.SiQiFishingV3, "_SiQiFishingV3__buy_quantity")

    assert buy(12, 10, 300, 3000, 0) == 0
    assert buy(2, 10, 300, 3000, 0) == 8
    assert buy(2, 10, 300, 900, 0) == 3
    assert buy(2, 10, 300, 3000, 2400) == 2
    assert buy(2, 10, 300, 0, 0) == 0


def test_dry_run_skips_casting(monkeypatch):
    """演练模式只同步状态，不发送 perform_fishing。"""
    plugin, result = _run_task(
        load_plugin_module(),
        monkeypatch,
        configure=lambda p: setattr(p, "_dry_run", True),
    )

    assert "perform_fishing" not in ActionRequestUtils.actions
    text = json.dumps(result, ensure_ascii=False)
    assert "演练" in text or any(
        "演练" in json.dumps(item, ensure_ascii=False)
        for item in plugin._daily.get("history") or []
    )


def test_no_lure_stops_partial(monkeypatch):
    """没有任何鱼饵时一次都不抛，结果为 partial。"""
    plugin, result = _run_task(
        load_plugin_module(),
        monkeypatch,
        data=make_data(lures={}, selected_lure=""),
    )

    assert "perform_fishing" not in ActionRequestUtils.actions
    assert result["status"] == "partial"


def test_daily_limit_truncates_to_one(monkeypatch):
    """本地每日计数只剩 1 次时最多只抛一竿。"""
    def configure(plugin):
        plugin._daily["casts"] = 4
        plugin._daily_cast_limit = 5
        plugin._max_casts_per_run = 10

    _run_task(
        load_plugin_module(),
        monkeypatch,
        responses={"perform_fishing": [catch_response() for _ in range(10)]},
        configure=configure,
    )

    assert ActionRequestUtils.actions.count("perform_fishing") == 1


def test_site_remaining_zero_skips(monkeypatch):
    """站点剩余次数为 0 时不得抛竿（回归）。"""
    _run_task(
        load_plugin_module(),
        monkeypatch,
        data=make_data(attempts_used=10, attempts_limit=10),
        responses={"perform_fishing": [catch_response() for _ in range(10)]},
    )

    assert ActionRequestUtils.actions.count("perform_fishing") == 0


def test_slot_spread_limit(monkeypatch):
    """开启时段分散后每个时段最多抛 per_slot_casts 次。"""
    module = load_plugin_module()

    def configure(plugin):
        plugin._slot_spread = True
        plugin._per_slot_casts = 1
        plugin._max_casts_per_run = 5
        plugin._daily["slots"] = {}

    _run_task(
        module,
        monkeypatch,
        responses={"perform_fishing": [catch_response() for _ in range(10)]},
        configure=configure,
    )

    assert ActionRequestUtils.actions.count("perform_fishing") == 1


def test_outcome_counts(monkeypatch):
    """catch / miss / miss_reward 三种结果分别计数。"""
    plugin, _ = _run_task(
        load_plugin_module(),
        monkeypatch,
        responses={
            "perform_fishing": [
                catch_response(),
                miss_response(),
                miss_reward_response(),
            ]
        },
        configure=lambda p: (
            setattr(p, "_max_casts_per_run", 3),
            setattr(p, "_auto_task", False),
        ),
    )

    assert ActionRequestUtils.actions.count("perform_fishing") == 3
    assert plugin._daily["catches"] == 1
    assert plugin._daily.get("misses") == 1
    assert plugin._daily.get("miss_rewards") == 1


def test_new_discovery_count(monkeypatch):
    """新发现鱼种单独累计。"""
    plugin, result = _run_task(
        load_plugin_module(),
        monkeypatch,
        responses={
            "perform_fishing": [
                catch_response("新鱼", new=True),
                catch_response("旧鱼", new=False),
            ]
        },
        configure=lambda p: setattr(p, "_max_casts_per_run", 2),
    )

    assert plugin._daily["catches"] == 2
    assert plugin._daily["new_discoveries"] == 1
    data = result.get("data") or {}
    if "new_discoveries" in data:
        assert data["new_discoveries"] == 1


def test_only_ready_tasks_delivered(monkeypatch):
    """只交付 progress.ready 为真的任务。"""
    tasks = {
        "1": {"progress": {"ready": True}},
        "2": {"progress": {"ready": False}},
        "3": {"progress": {"ready": True}},
    }
    _run_task(
        load_plugin_module(),
        monkeypatch,
        data=make_data(tasks=tasks),
        responses={
            "perform_fishing": [catch_response()],
            "deliver_task": [FakeResponse(payload={"success": True}) for _ in range(3)],
        },
        configure=lambda p: (
            setattr(p, "_max_casts_per_run", 1),
            setattr(p, "_auto_task", True),
        ),
    )

    delivered = {
        str(body.get("task_id"))
        for action, body in zip(ActionRequestUtils.actions, ActionRequestUtils.bodies)
        if action == "deliver_task"
    }
    assert delivered == {"1", "3"}


def test_guess_only_first_and_before_cast(monkeypatch):
    """竞猜只在当日尚未抛竿时提交，且先于抛竿。"""
    module = load_plugin_module()

    def configure(plugin):
        plugin._auto_guess = True
        plugin._guess_value = 3
        plugin._max_casts_per_run = 1

    _run_task(
        module,
        monkeypatch,
        data=make_data(attempts_used=0, quiz_status="not_started"),
        responses={
            "submit_guess": FakeResponse(payload={"success": True}),
            "perform_fishing": [catch_response()],
        },
        configure=configure,
    )

    assert "submit_guess" in ActionRequestUtils.actions
    assert ActionRequestUtils.actions.index("submit_guess") < ActionRequestUtils.actions.index(
        "perform_fishing"
    )

    ActionRequestUtils.reset(
        responses={"perform_fishing": [catch_response()]},
        html=fishing_html(make_data(attempts_used=2, quiz_status="not_started")),
    )
    plugin = build_plugin(module)
    plugin._auto_guess = True
    plugin._guess_value = 3
    plugin._max_casts_per_run = 1
    asyncio.run(plugin.run_fishing_task())

    assert "submit_guess" not in ActionRequestUtils.actions


def test_run_api_schedules_once_job(monkeypatch):
    """/run 只注册一次性任务，不在接口内直接抛竿。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    ActionRequestUtils.reset(html=fishing_html(make_data()))
    scheduled = []
    monkeypatch.setattr(
        module.scheduler_sdk,
        "add_plugin_once_job",
        lambda *args, **kwargs: scheduled.append((args, kwargs)),
    )

    plugin = build_plugin(module)
    result = asyncio.run(plugin.run_once_api())

    assert result.get("success") is True
    assert scheduled
    first_args = scheduled[0][0]
    job_id = first_args[1] if len(first_args) > 1 else scheduled[0][1].get("job_id")
    assert job_id in ("api_run_once", "run_once")
    assert ActionRequestUtils.calls == []

    paths = {api["path"] for api in plugin.get_api()}
    assert "/run" in paths


def test_command_filters_action(monkeypatch):
    """远程命令只响应自己的 action。"""
    module = load_plugin_module()
    scheduled = []
    monkeypatch.setattr(
        module.scheduler_sdk,
        "add_plugin_once_job",
        lambda *args, **kwargs: scheduled.append((args, kwargs)),
    )

    plugin = build_plugin(module)
    event_type = getattr(module.EventType, "PluginAction", None)
    matched = SimpleNamespace(
        event_type=event_type, event_data={"action": "siqi_fishing"}
    )
    other = SimpleNamespace(event_type=event_type, event_data={"action": "something_else"})

    asyncio.run(plugin.run_once_command(matched))
    assert len(scheduled) == 1

    asyncio.run(plugin.run_once_command(other))
    assert len(scheduled) == 1


def test_init_plugin_run_once_resets_flag(monkeypatch):
    """init_plugin 处理 run_once 后调用 update_config 关闭开关。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    scheduled = []
    monkeypatch.setattr(
        module.scheduler_sdk,
        "add_plugin_once_job",
        lambda *args, **kwargs: scheduled.append((args, kwargs)),
    )

    plugin = build_plugin(module)
    updates = []
    plugin.update_config = lambda config: updates.append(config)

    plugin.init_plugin({"enabled": True, "run_once": True})

    assert any(config.get("run_once") is False for config in updates), updates
    assert scheduled


def test_stop_service_removes_jobs(monkeypatch):
    """停用时逐个移除三个一次性任务 ID，并清空运行锁。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    removed = []

    def _remove(*args, **kwargs):
        if args:
            removed.append(args[-1])
        else:
            removed.append(kwargs.get("job_id"))

    monkeypatch.setattr(
        module.scheduler_sdk, "remove_plugin_once_job", _remove
    )

    plugin._lock = object()
    plugin.stop_service()
    plugin.stop_service()

    expected = ["run_once", "api_run_once", "command_run_once"]
    assert removed[:3] == expected
    assert plugin._lock is None


def cast_with_data(used: int, limit: int = 50, *, name: str = "鲫鱼") -> FakeResponse:
    """构造带全量 data 的抛竿响应，用于验证站点次数递减。"""
    return FakeResponse(
        payload={
            "success": True,
            "data": make_data(attempts_used=used, attempts_limit=limit),
            "result": {
                "status": "catch",
                "timing": "perfect",
                "fish": {"name": name, "rarity_label": "普通"},
                "is_new_discovery": False,
            },
        }
    )


def test_site_remaining_partial_truncation(monkeypatch):
    """站点只剩 2 次时，即使单次上限 6 也只能抛 2 竿。"""
    plugin, _ = _run_task(
        load_plugin_module(),
        monkeypatch,
        data=make_data(attempts_used=48, attempts_limit=50),
        responses={"perform_fishing": [cast_with_data(49), cast_with_data(50)]},
        configure=lambda p: setattr(p, "_max_casts_per_run", 6),
    )

    assert ActionRequestUtils.actions.count("perform_fishing") == 2
    assert plugin._state["attempts_used"] == 50


def test_auto_buy_within_budget(monkeypatch):
    """开启补饵且预算充足时补到保底库存，并记录花费。"""
    buy_payload = make_data(lures={"normal": 3, "insect": 0})
    buy_payload["success"] = True

    plugin, _ = _run_task(
        load_plugin_module(),
        monkeypatch,
        data=make_data(lures={"normal": 0, "insect": 0}),
        responses={
            "purchase_lure": [FakeResponse(payload=buy_payload)],
            "perform_fishing": [cast_with_data(1)],
        },
        configure=lambda p: (
            setattr(p, "_auto_buy", True),
            setattr(p, "_buy_budget_per_day", 1000),
            setattr(p, "_keep_lures", 3),
            setattr(p, "_max_casts_per_run", 1),
        ),
    )

    bodies = [body for action, body in ActionRequestUtils.calls if action == "purchase_lure"]
    assert bodies, "预算充足时应触发补饵"
    assert bodies[0]["lure_key"] == "normal"
    assert bodies[0]["quantity"] == "3"
    assert plugin._daily["spent"] == 900


def test_auto_buy_respects_zero_budget(monkeypatch):
    """auto_buy 打开但预算为 0 时不得花钱。"""
    _, _ = _run_task(
        load_plugin_module(),
        monkeypatch,
        data=make_data(lures={"normal": 0}),
        responses={"perform_fishing": [cast_with_data(1)]},
        configure=lambda p: (
            setattr(p, "_auto_buy", True),
            setattr(p, "_buy_budget_per_day", 0),
            setattr(p, "_keep_lures", 5),
        ),
    )

    assert "purchase_lure" not in ActionRequestUtils.actions


def test_select_lure_switches_to_best(monkeypatch):
    """自动策略下切换到库存中的高分鱼饵。"""
    _run_task(
        load_plugin_module(),
        monkeypatch,
        data=make_data(lures={"normal": 5, "insect": 2}),
        responses={"perform_fishing": [cast_with_data(1)]},
        configure=lambda p: setattr(p, "_max_casts_per_run", 1),
    )

    bodies = [body for action, body in ActionRequestUtils.calls if action == "select_lure"]
    assert bodies, "应请求切换鱼饵"
    assert bodies[0]["lure_key"] == "insect"


def test_api_status_wraps_data():
    """状态接口返回 success + data 包裹结构。"""
    plugin = build_plugin(load_plugin_module())

    result = asyncio.run(plugin.api_status())

    assert result["success"] is True
    assert set(result["data"]) == {"daily", "state"}


def test_non_json_response_marks_failed(monkeypatch):
    """抛竿返回非 JSON 时按失败处理并停止。"""
    _, result = _run_task(
        load_plugin_module(),
        monkeypatch,
        responses={"perform_fishing": [FakeResponse(text="<html>error</html>")]},
    )

    assert result["status"] == "failed"
    assert ActionRequestUtils.actions.count("perform_fishing") == 1


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


def _collect_components(node, found):
    """递归收集页面 JSON 中出现的全部 component 名称。"""
    if isinstance(node, dict):
        component = node.get("component")
        if isinstance(component, str):
            found.add(component)
        for value in node.values():
            _collect_components(value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_components(item, found)
    return found


def test_page_components_whitelisted():
    """详情页只能使用宿主已验证注册的 Vuetify 组件。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._state.update(
        {
            "attempts_used": 5,
            "attempts_limit": 10,
            "catches": 2,
            "lures": {"normal": 12},
            "selected_lure": "normal",
            "codex_total": 10,
            "codex_unlocked": 3,
            "magic": 100,
            "tasks": {"1": {"progress": {"ready": True}}},
            "tasks_ready": 1,
        }
    )
    plugin._daily["history"] = [
        {"time": "12:00:00", "action": "catch", "detail": "鲫鱼（普通/完美）"}
    ]

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


def test_page_renders_without_data():
    """空数据时详情页仍能渲染并给出占位文案。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._state.update(
        {
            "attempts_used": 0,
            "attempts_limit": 0,
            "catches": 0,
            "lures": {},
            "selected_lure": "",
            "tasks": {},
            "last_message": "",
        }
    )
    plugin._daily.update(
        {
            "casts": 0,
            "catches": 0,
            "new_discoveries": 0,
            "spent": 0,
            "history": [],
            "last_result": "",
        }
    )

    page = plugin.get_page()

    assert isinstance(page, list)
    assert page
    page_text = json.dumps(page, ensure_ascii=False)
    assert "暂无执行记录" in page_text
    assert "尚未执行" in page_text


def test_roll_daily_resets_across_days():
    """跨自然日重置当日计数，同日保持不变。"""
    module = load_plugin_module()
    roll = getattr(module.SiQiFishingV3, "_SiQiFishingV3__roll_daily")

    daily = {
        "date": "2026-10-27",
        "casts": 9,
        "catches": 4,
        "misses": 3,
        "miss_rewards": 2,
        "new_discoveries": 1,
        "spent": 600,
        "slots": {"1": 9},
        "history": [{"detail": "旧记录"}],
        "last_result": "旧结果",
    }
    result = roll(daily, "2026-10-28")
    state = result if isinstance(result, dict) else daily
    assert state["date"] == "2026-10-28"
    assert state["casts"] == 0
    assert state.get("catches", 0) == 0
    assert state.get("spent", 0) == 0
    assert not state.get("slots")

    same_day = {"date": "2026-10-28", "casts": 5, "catches": 2, "spent": 300}
    result = roll(same_day, "2026-10-28")
    state = result if isinstance(result, dict) else same_day
    assert state["casts"] == 5
    assert state["catches"] == 2


def test_notification_status_text():
    """通知文案包含成功与认证失败的状态标识。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._notify = True
    sent = {}
    plugin.post_message = lambda **kwargs: sent.update(kwargs)
    notify = getattr(module.SiQiFishingV3, "_SiQiFishingV3__send_notification")

    notify(plugin, {"status": "completed", "message": "ok", "data": {}})
    assert "✅ 成功" in sent.get("text", "")

    sent.clear()
    notify(plugin, {"status": "auth_failed", "message": "no cookie", "data": {}})
    assert "🔐 认证失败" in sent.get("text", "")


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


def _stub_runtime(monkeypatch, module, *, data=None, responses=None) -> StubScheduler:
    """替换网络、站点与调度器，并预置页面 HTML 与 action 响应。"""
    scheduler = StubScheduler()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module, "SiteOper", StubSiteOper)
    monkeypatch.setattr(module, "scheduler_sdk", scheduler)
    ActionRequestUtils.reset(
        responses=responses or {},
        html=fishing_html(data if data is not None else make_data()),
    )
    return scheduler


def _lock_plugin(plugin):
    """给插件装上已持有的运行锁，模拟任务执行中。"""
    lock = asyncio.Lock()
    asyncio.run(lock.acquire())
    plugin._lock = lock
    return lock


def _iter_nodes(node):
    """深度优先遍历页面 JSON 中的全部字典节点。"""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_nodes(item)


def _page_button_events(page):
    """产出页面中所有携带 events 的节点及其事件处理器。"""
    for node in _iter_nodes(page):
        events = node.get("events")
        if not isinstance(events, dict):
            continue
        for handler in events.values():
            if isinstance(handler, dict):
                yield node, handler


def _option_buttons(page, key: str, value=None):
    """找出点击参数引用了指定配置键（可选指定取值）的按钮节点。"""
    found: list = []
    seen: set = set()
    for node, handler in _page_button_events(page):
        params = handler.get("params")
        if not isinstance(params, dict):
            continue
        text = json.dumps(params, ensure_ascii=False)
        if key not in text:
            continue
        if value is not None and str(value) not in text:
            continue
        if id(node) not in seen:
            seen.add(id(node))
            found.append(node)
    return found


def _rich_page(plugin, **state) -> list:
    """填充一组足以渲染全部卡片的运行态，再返回详情页结构。"""
    plugin._state.update(
        {
            "attempts_used": 5,
            "attempts_limit": 50,
            "catches": 2,
            "magic": 1200,
            "lures": {"normal": 12, "insect": 3, "camouflage": 0},
            "rods": {"mushroom": {"unlocked": True}, "star": {"unlocked": True}},
            "selected_lure": "normal",
            "selected_rod": "mushroom",
            "codex_total": 10,
            "codex_unlocked": 3,
            "tasks": {"1": {"progress": {"ready": True}}},
            "tasks_ready": 1,
        }
    )
    plugin._state.update(state)
    plugin._daily["history"] = [
        {"time": "12:00:00", "action": "catch", "detail": "鲫鱼（普通/完美）"}
    ]
    return plugin.get_page()


def test_page_button_api_prefix():
    """详情页所有按钮事件都指向本插件的 API，且不得携带明文 apikey。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    page = _rich_page(plugin)

    apis = [
        handler["api"]
        for _, handler in _page_button_events(page)
        if isinstance(handler.get("api"), str)
    ]

    assert apis, "详情页必须提供可点击的操作按钮"
    for api in apis:
        assert api, "按钮 API 不能为空"
        assert api.startswith("plugin/SiQiFishingV3/"), api
        assert "apikey=" not in api, api


def test_page_button_events_structure():
    """按钮事件的 api/method/params 三个字段必须齐备且类型正确。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    page = _rich_page(plugin)

    handlers = list(_page_button_events(page))

    assert len(handlers) >= 4, "快捷操作与策略调节卡片至少提供 4 个按钮"
    for _, handler in handlers:
        assert isinstance(handler.get("api"), str) and handler["api"]
        assert handler.get("method") in ("get", "post"), handler
        assert isinstance(handler.get("params"), dict), handler


def test_get_api_new_endpoints():
    """四个新接口均以 POST + bear 暴露，/run 保持可用。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    apis = {api["path"]: api for api in plugin.get_api()}

    for path in ("/fish_now", "/sync", "/set_option", "/deliver"):
        assert path in apis, sorted(apis)
        assert apis[path]["methods"] == ["POST"], path
        assert apis[path]["auth"] == "bear", path
        assert callable(apis[path]["endpoint"]), path

    assert "/run" in apis
    assert apis["/run"]["methods"] == ["POST"]


def test_set_option_rejects_unknown_key():
    """配置页专属键与未知键必须拒绝，并提示去配置页修改。"""
    module = load_plugin_module()
    normalize = getattr(module.SiQiFishingV3, "_SiQiFishingV3__normalize_option")

    for key in (
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
        "whatever",
    ):
        ok, _, message = normalize(key, "1")
        assert ok is False, key
        assert "配置页" in str(message), (key, message)


def test_set_option_rejects_out_of_range():
    """数值越界必须拒绝。"""
    module = load_plugin_module()
    normalize = getattr(module.SiQiFishingV3, "_SiQiFishingV3__normalize_option")

    for key, value in (
        ("daily_cast_limit", 0),
        ("daily_cast_limit", 100),
        ("keep_lures", -1),
        ("buy_budget_per_day", -5),
    ):
        ok, _, message = normalize(key, value)
        assert ok is False, (key, value)
        assert str(message).strip(), (key, value)


def test_set_option_rejects_bad_enum():
    """枚举取值非法时必须拒绝。"""
    module = load_plugin_module()
    normalize = getattr(module.SiQiFishingV3, "_SiQiFishingV3__normalize_option")

    for key, value in (
        ("slider_mode", "wild"),
        ("lure_key", "diamond"),
        ("rod_fixed", "bamboo"),
    ):
        ok, _, message = normalize(key, value)
        assert ok is False, (key, value)
        assert str(message).strip(), (key, value)


def test_set_option_rejects_bad_bool():
    """布尔键只接受明确的真假字面量。"""
    module = load_plugin_module()
    normalize = getattr(module.SiQiFishingV3, "_SiQiFishingV3__normalize_option")

    for key, value in (("auto_buy", "yes"), ("dry_run", 2)):
        ok, _, message = normalize(key, value)
        assert ok is False, (key, value)
        assert str(message).strip(), (key, value)


def test_set_option_bool_normalization():
    """四种真值与三种假值都能归一化为 bool。"""
    module = load_plugin_module()
    normalize = getattr(module.SiQiFishingV3, "_SiQiFishingV3__normalize_option")

    for raw in ("true", "True", "1", True):
        ok, value, message = normalize("dry_run", raw)
        assert ok is True, (raw, message)
        assert value is True, raw

    for raw in ("false", "0", False):
        ok, value, message = normalize("dry_run", raw)
        assert ok is True, (raw, message)
        assert value is False, raw


def test_set_option_persists_full_config():
    """改单个选项时以原配置为基底整体回写，未改动的键原样保留。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._config_snapshot = make_config(
        schedule_mode="cron", cron="7 */2 * * *", cookie="c_secure_pass=token"
    )

    result = asyncio.run(plugin.api_set_option({"key": "dry_run", "value": "true"}))

    assert result.get("success") is True, result
    assert plugin._updated_configs, "必须整体回写配置"
    saved = plugin._updated_configs[-1]
    assert saved["cookie"] == "c_secure_pass=token"
    assert saved["cron"] == "7 */2 * * *"
    assert saved["schedule_mode"] == "cron"
    assert saved["dry_run"] is True


def test_set_option_applies_memory_and_idempotent():
    """改选项后内存属性立即生效，重复提交同样返回成功。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    first = asyncio.run(plugin.api_set_option({"key": "dry_run", "value": "true"}))
    assert first.get("success") is True, first
    assert plugin._dry_run is True

    second = asyncio.run(plugin.api_set_option({"key": "dry_run", "value": "true"}))
    assert second.get("success") is True, second
    assert plugin._dry_run is True


def test_set_option_compound_lure_key():
    """lure_fixed 会展开成 lure_mode=fixed 与 lure_key 两键。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._state["lures"] = {"normal": 5, "insect": 3}

    result = asyncio.run(plugin.api_set_option({"key": "lure_fixed", "value": "insect"}))

    assert result.get("success") is True, result
    assert plugin._lure_mode == "fixed"
    assert plugin._lure_key == "insect"
    saved = plugin._updated_configs[-1]
    assert saved["lure_mode"] == "fixed"
    assert saved["lure_key"] == "insect"


def test_set_option_rod_fixed_requires_unlock():
    """未解锁鱼竿不得通过快捷操作切换。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._state["rods"] = {
        "mushroom": {"unlocked": True},
        "star": {"unlocked": False},
    }

    result = asyncio.run(plugin.api_set_option({"key": "rod_fixed", "value": "star"}))

    assert result.get("success") is False, result
    assert plugin._rod_mode == "auto"
    assert plugin._rod_key == "mushroom"
    assert plugin._updated_configs == []


def test_set_option_config_read_failure_returns_false():
    """读取配置失败时必须直接返回失败，不得用空配置回写。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    def _boom():
        raise RuntimeError("db down")

    plugin.get_config = _boom

    result = asyncio.run(plugin.api_set_option({"key": "dry_run", "value": "true"}))

    assert result.get("success") is False, result
    assert plugin._updated_configs == []
    assert plugin._dry_run is False


def test_set_option_update_config_failure_keeps_memory():
    """写配置失败时不得改动内存中的选项状态。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    def _boom(config):
        raise RuntimeError("write failed")

    plugin.update_config = _boom

    result = asyncio.run(plugin.api_set_option({"key": "dry_run", "value": "true"}))

    assert result.get("success") is False, result
    assert plugin._dry_run is False
    assert plugin._lure_mode == "auto"


def test_fish_now_returns_running_when_locked(monkeypatch):
    """运行锁被占用时立即返回 running，且不发起任何请求。"""
    module = load_plugin_module()
    _stub_runtime(monkeypatch, module, data=make_data(lures={"normal": 3}))
    plugin = build_plugin(module)
    _lock_plugin(plugin)

    result = asyncio.run(plugin.api_fish_now({"casts": 1}))

    assert result.get("status") == "running", result
    assert ActionRequestUtils.calls == []


def test_fish_now_single_cast_never_buys(monkeypatch):
    """快捷钓一竿只抛竿，不补饵、不竞猜、不交付任务。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        data=make_data(
            lures={"normal": 1},
            tasks={"1": {"progress": {"ready": True}}},
        ),
        responses={
            "perform_fishing": [cast_with_data(1) for _ in range(5)],
            "purchase_lure": [FakeResponse(payload={"success": True})],
            "submit_guess": [FakeResponse(payload={"success": True})],
            "deliver_task": [FakeResponse(payload={"success": True})],
        },
    )
    plugin = build_plugin(module)
    plugin._auto_buy = True
    plugin._buy_budget_per_day = 5000
    plugin._keep_lures = 10
    plugin._auto_task = True
    plugin._auto_guess = True

    asyncio.run(plugin.api_fish_now({"casts": 1}))

    assert "purchase_lure" not in ActionRequestUtils.actions
    assert "submit_guess" not in ActionRequestUtils.actions
    assert "deliver_task" not in ActionRequestUtils.actions
    assert ActionRequestUtils.actions.count("perform_fishing") == 1


def test_fish_now_casts_capped(monkeypatch):
    """快捷钓一竿的 casts 参数最多放大到 min(3, 单次上限)。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        data=make_data(lures={"normal": 30}),
        responses={
            "perform_fishing": [cast_with_data(index) for index in range(1, 8)],
        },
    )
    plugin = build_plugin(module)

    asyncio.run(plugin.api_fish_now({"casts": 99}))

    casts = ActionRequestUtils.actions.count("perform_fishing")
    assert casts == min(3, plugin._max_casts_per_run), casts


def test_sync_no_casting(monkeypatch):
    """同步接口只 GET 页面并刷新运行态，不发送任何 action。"""
    module = load_plugin_module()
    _stub_runtime(monkeypatch, module, data=make_data(lures={"normal": 4}))
    plugin = build_plugin(module)
    plugin._state["updated_at"] = None

    asyncio.run(plugin.api_sync())

    assert ActionRequestUtils.actions == []
    assert plugin._state["updated_at"]
    actions = [item.get("action") for item in plugin._daily["history"]]
    assert "sync" in actions, actions


def test_sync_locked_returns_running(monkeypatch):
    """运行锁被占用时同步接口同样返回 running 且不发请求。"""
    module = load_plugin_module()
    _stub_runtime(monkeypatch, module, data=make_data(lures={"normal": 4}))
    plugin = build_plugin(module)
    _lock_plugin(plugin)

    result = asyncio.run(plugin.api_sync())

    assert result.get("status") == "running", result
    assert ActionRequestUtils.calls == []


def test_deliver_no_ready_returns_zero(monkeypatch):
    """没有可交付任务时返回 delivered=0 且不请求交付接口。"""
    module = load_plugin_module()
    _stub_runtime(monkeypatch, module, data=make_data(tasks={}))
    plugin = build_plugin(module)

    result = asyncio.run(plugin.api_deliver())

    assert result["data"]["delivered"] == 0, result
    assert "deliver_task" not in ActionRequestUtils.actions


def test_deliver_only_ready_tasks(monkeypatch):
    """交付接口只处理 progress.ready 为真的任务。"""
    module = load_plugin_module()
    _stub_runtime(
        monkeypatch,
        module,
        data=make_data(
            tasks={
                "1": {"progress": {"ready": True}},
                "2": {"progress": {"ready": False}},
            }
        ),
        responses={
            "deliver_task": [FakeResponse(payload={"success": True}) for _ in range(3)],
        },
    )
    plugin = build_plugin(module)

    result = asyncio.run(plugin.api_deliver())

    assert result["data"]["delivered"] == 1, result
    bodies = [
        body for action, body in ActionRequestUtils.calls if action == "deliver_task"
    ]
    assert len(bodies) == 1
    assert str(bodies[0].get("task_id")) == "1"


def test_page_lure_buttons_disabled_without_stock():
    """零库存鱼饵的快捷按钮必须禁用，有库存的保持可点。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    page = _rich_page(plugin, lures={"normal": 5, "insect": 0})

    empty = _option_buttons(page, "lure_fixed", "insect")
    assert empty, "零库存鱼饵也应渲染为禁用按钮"
    for node in empty:
        assert (node.get("props") or {}).get("disabled") is True, node

    stocked = _option_buttons(page, "lure_fixed", "normal")
    assert stocked, "有库存鱼饵必须提供可点按钮"
    for node in stocked:
        assert (node.get("props") or {}).get("disabled") is not True, node


def test_page_rod_buttons_only_unlocked():
    """未解锁鱼竿不得出现可点的快捷按钮。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    page = _rich_page(
        plugin,
        rods={
            "mushroom": {"unlocked": True},
            "star": {"unlocked": True},
            "love": {"unlocked": False},
        },
    )

    assert _option_buttons(page, "rod_fixed", "star"), "已解锁鱼竿应可点"
    assert not _option_buttons(page, "rod_fixed", "love"), "未解锁鱼竿不得可点"
    assert "🔒" in json.dumps(page, ensure_ascii=False)


def test_page_running_chip_when_locked():
    """任务执行中时详情页给出运行中提示。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    _rich_page(plugin)
    _lock_plugin(plugin)

    page_text = json.dumps(plugin.get_page(), ensure_ascii=False)

    assert "任务执行中" in page_text


def test_page_quick_action_cards():
    """详情页新增快捷操作与策略调节两张卡片。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    page_text = json.dumps(_rich_page(plugin), ensure_ascii=False)

    assert "⚡ 快捷操作" in page_text
    assert "🎛 策略调节" in page_text


def test_option_label_covers_whitelist():
    """白名单键都能给出非空中文标签。"""
    module = load_plugin_module()
    label = getattr(module.SiQiFishingV3, "_SiQiFishingV3__option_label")

    for key in (
        "slider_mode",
        "lure_mode",
        "lure_key",
        "rod_mode",
        "rod_key",
        "auto_buy",
        "dry_run",
        "daily_cast_limit",
        "buy_budget_per_day",
    ):
        text = label(key)
        assert isinstance(text, str) and text.strip(), key

    assert isinstance(label("unknown_key"), str)


_MISSING = object()


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
    checks = [
        (name, func)
        for name, func in sorted(globals().items())
        if name.startswith("test_") and callable(func)
    ]
    for name, func in checks:
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
    print(f"{len(checks) - failures}/{len(checks)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_run_standalone())
