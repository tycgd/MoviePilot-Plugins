"""SiQiCleanBeachV3 纯逻辑测试：不连接宿主数据库与真实站点。"""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

PLUGIN_INIT = (
    Path(__file__).resolve().parents[3]
    / "plugins.v3"
    / "siqicleanbeachv3"
    / "__init__.py"
)


def load_plugin_module():
    """按文件路径加载插件模块，避免依赖插件市场安装结果。"""
    spec = importlib.util.spec_from_file_location(
        "siqicleanbeachv3_under_test", PLUGIN_INIT
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


class FakeRequestUtils:
    """替换 AsyncRequestUtils，记录调用并按需返回固定响应。"""

    response = FakeResponse()
    calls: list = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        FakeRequestUtils.calls.append(kwargs)

    async def post_res(self, url, **kwargs):
        return FakeRequestUtils.response

    async def get_res(self, url, **kwargs):
        return FakeRequestUtils.response


class ActionRequestUtils:
    """按 action 分发预置响应的假客户端，供搬砖与清理流程使用。"""

    actions: list = []
    responses: dict = {}
    inventory_html: str = ""

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    @classmethod
    def reset(cls, responses=None, inventory_html: str = "") -> None:
        cls.actions = []
        cls.responses = dict(responses or {})
        cls.inventory_html = inventory_html

    async def post_res(self, url, **kwargs):
        action = (kwargs.get("data") or {}).get("action", "")
        ActionRequestUtils.actions.append(action)
        queued = ActionRequestUtils.responses.get(action)
        if queued is None:
            return FakeResponse(payload={"success": True})
        if isinstance(queued, list):
            if not queued:
                return FakeResponse(payload={"success": False, "message": "无预置响应"})
            return queued.pop(0)
        return queued

    async def get_res(self, url, **kwargs):
        return FakeResponse(text=ActionRequestUtils.inventory_html)


def build_plugin(module):
    """绕过 _PluginBase.__init__ 构造最小实例。"""
    plugin = object.__new__(module.SiQiCleanBeachV3)
    plugin._enabled = True
    plugin._cookie = ""
    plugin._schedule_mode = "interval"
    plugin._cron = "1 */2 * * *"
    plugin._interval_minutes = 121
    plugin._notify = False
    plugin._lock = None
    plugin._brick_enabled = True
    plugin._brick_max_per_run = 50
    plugin._bricks = {
        "daily": 0,
        "pocket": 0,
        "remaining": 50,
        "last_update": None,
        "next_reset": 0,
    }
    plugin._inventory = {}
    plugin._stats = {
        "last_update": None,
        "last_result": "",
        "total_items_count": 0,
        "history": [],
    }
    plugin._data = {}
    plugin.save_data = lambda key, value: plugin._data.__setitem__(key, value)
    plugin.get_data = lambda key, default=None: plugin._data.get(key, default)
    plugin.update_config = lambda config: None
    return plugin


def test_metadata_matches_directory():
    """插件 ID、目录名、版本与市场索引必须一致。"""
    import json

    module = load_plugin_module()
    cls = module.SiQiCleanBeachV3
    assert cls.__name__.lower() == PLUGIN_INIT.parent.name
    assert PLUGIN_INIT.parent.name == "siqicleanbeachv3"
    assert cls.plugin_version == "1.1.0"
    assert cls.plugin_config_prefix == "siqicleanbeachv3_"

    package = json.loads((PLUGIN_INIT.parents[2] / "package.v3.json").read_text("utf-8"))
    entry = package[cls.__name__]
    assert entry["version"] == cls.plugin_version, "package.v3.json 版本与 plugin_version 不一致"
    assert f"v{cls.plugin_version}" in entry["history"], "history 缺少当前版本记录"
    assert entry["icon"] == cls.plugin_icon
    assert entry["name"] == cls.plugin_name


def test_no_abstract_methods_left():
    """抽象方法必须全部实现，否则宿主无法实例化。"""
    module = load_plugin_module()
    assert module.SiQiCleanBeachV3.__abstractmethods__ == frozenset()


def test_contract_methods_present():
    """V3 生命周期方法必须齐备，网络相关方法为协程。"""
    module = load_plugin_module()
    cls = module.SiQiCleanBeachV3
    for name in (
        "init_plugin",
        "get_state",
        "get_api",
        "get_form",
        "get_page",
        "stop_service",
    ):
        assert callable(getattr(cls, name))
    assert asyncio.iscoroutinefunction(cls.run_clean_task)
    assert asyncio.iscoroutinefunction(cls.run_once_api)
    assert asyncio.iscoroutinefunction(cls.run_once_command)
    assert asyncio.iscoroutinefunction(cls.move_bricks_api)


def test_cookie_parse_and_safe_int():
    """Cookie 解析与整数兜底逻辑保持原行为。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    parse = module.SiQiCleanBeachV3._SiQiCleanBeachV3__cookie_to_dict
    safe_int = module.SiQiCleanBeachV3._SiQiCleanBeachV3__safe_int

    assert parse("a=1; b = 2 ;bad; c=x=y") == {"a": "1", "b": "2", "c": "x=y"}
    assert parse("") == {}
    assert safe_int("7", 121) == 7
    assert safe_int("bad", 121) == 121
    assert safe_int(0, 121, min_value=1) == 1
    assert plugin._interval_minutes == 121


def test_fetch_inventory_parses_items(monkeypatch):
    """物品清单解析结果剔除砖块。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", FakeRequestUtils)
    module.SiQiCleanBeachV3.UA = "test-ua"

    FakeRequestUtils.response = FakeResponse(
        text="""
        <div class="inventory-grid">
          <div class="inventory-item">
            <div class="item-name">木材</div>
            <div class="item-count">12</div>
            <span class="item-icon">🪵</span>
          </div>
          <div class="inventory-item">
            <div class="item-name">砖块</div>
            <div class="item-count">99</div>
            <span class="item-icon">🧱</span>
          </div>
        </div>
        """
    )
    plugin = build_plugin(module)
    fetch = module.SiQiCleanBeachV3._SiQiCleanBeachV3__fetch_inventory

    inventory = asyncio.run(fetch(plugin, "c_secure_pass=token"))

    assert "木材" in inventory
    assert inventory["木材"]["count"] == 12
    assert inventory["木材"]["icon"] == "🪵"
    assert "砖块" not in inventory
    assert FakeRequestUtils.calls[0]["cookies"] == {"c_secure_pass": "token"}


def test_collect_trash_reads_nested_count(monkeypatch):
    """收集数量同时兼容 data.collected 与文案中的数字。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", FakeRequestUtils)

    plugin = build_plugin(module)
    collect = module.SiQiCleanBeachV3._SiQiCleanBeachV3__collect_trash

    FakeRequestUtils.response = FakeResponse(
        payload={"success": True, "data": {"collected": 5}}
    )
    assert asyncio.run(collect(plugin, "c_secure_pass=token")) == {
        "success": True,
        "trash": 5,
    }

    FakeRequestUtils.response = FakeResponse(
        payload={"success": False, "message": "共收集了 3 个垃圾"}
    )
    assert asyncio.run(collect(plugin, "c_secure_pass=token")) == {
        "success": True,
        "trash": 3,
    }


def test_enter_beach_reports_http_error(monkeypatch):
    """非 200 响应转换为失败结果而不是抛异常。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", FakeRequestUtils)

    FakeRequestUtils.response = FakeResponse(status_code=502)
    plugin = build_plugin(module)
    enter = module.SiQiCleanBeachV3._SiQiCleanBeachV3__enter_beach

    assert asyncio.run(enter(plugin, "c_secure_pass=token")) == {
        "success": False,
        "message": "HTTP 502",
    }


def test_run_clean_task_requires_cookie(monkeypatch):
    """缺少 c_secure_pass 时直接返回认证失败，不发请求。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", FakeRequestUtils)

    class StubSiteOper:
        @staticmethod
        def get_by_domain(domain):
            return None

        @staticmethod
        async def async_get_by_domain(domain):
            return None

    monkeypatch.setattr(module, "SiteOper", StubSiteOper)

    plugin = build_plugin(module)
    plugin._update_stats = lambda: None
    FakeRequestUtils.calls.clear()

    result = asyncio.run(plugin.run_clean_task())

    assert result["status"] == "auth_failed"
    assert FakeRequestUtils.calls == []


def test_service_id_is_class_name():
    """周期服务 ID 使用运行类名，分身之间不冲突。"""
    module = load_plugin_module()
    plugin = build_plugin(module)

    (service,) = plugin.get_service()

    assert service["id"] == "SiQiCleanBeachV3"
    assert service["func"] == plugin.run_clean_task


def test_stop_service_removes_all_once_jobs(monkeypatch):
    """停用时移除全部一次性任务 ID，且可重复调用。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    removed = []
    monkeypatch.setattr(
        module.scheduler_sdk,
        "remove_plugin_once_job",
        lambda plugin_id, job_id: removed.append(job_id),
    )

    plugin.stop_service()
    plugin.stop_service()

    expected = ["run_once", "api_run_once", "command_run_once", "brick_run_once"]
    assert removed == expected + expected


def _sync_payload(daily_bricks: int, next_reset: int = 1793203200) -> dict:
    """构造 sync_game_state 的实测响应结构。"""
    return {
        "success": True,
        "daily_bricks": daily_bricks,
        "last_brick_time": 0,
        "last_daily_reset": "2026-10-28",
        "server_now": 1793150958,
        "next_brick_reset_ts": next_reset,
    }


def _move_payload(daily_bricks: int) -> dict:
    """构造一次成功 move_brick 的响应结构。"""
    return {
        "success": True,
        "message": "搬砖成功",
        "bricks_moved": 1,
        "daily_bricks": daily_bricks,
        "last_daily_reset": "2026-10-28",
        "server_now": 1793150958,
        "next_brick_reset_ts": 1793203200,
    }


def test_move_bricks_respects_max_per_run(monkeypatch):
    """单次上限为 3 时只发送 3 次 move_brick。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module.SiQiCleanBeachV3, "BRICK_MOVE_INTERVAL", 0)
    ActionRequestUtils.reset({
        "sync_game_state": FakeResponse(payload=_sync_payload(0)),
        "move_brick": [
            FakeResponse(payload=_move_payload(1)),
            FakeResponse(payload=_move_payload(2)),
            FakeResponse(payload=_move_payload(3)),
        ],
    })

    plugin = build_plugin(module)
    plugin._brick_max_per_run = 3
    move_bricks = module.SiQiCleanBeachV3._SiQiCleanBeachV3__move_bricks

    result = asyncio.run(move_bricks(plugin, "c_secure_pass=token"))

    assert ActionRequestUtils.actions[0] == "sync_game_state"
    assert ActionRequestUtils.actions.count("move_brick") == 3
    assert result["moved"] == 3
    assert result["success"] is True
    assert result["daily_bricks"] == 3
    assert result["remaining"] == 47
    assert plugin._data["bricks"]["daily"] == 3
    assert plugin._data["bricks"]["remaining"] == 47


def test_move_bricks_skips_when_daily_limit_reached(monkeypatch):
    """服务端已搬满 50 块时一次 move_brick 都不发送。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module.SiQiCleanBeachV3, "BRICK_MOVE_INTERVAL", 0)
    ActionRequestUtils.reset({
        "sync_game_state": FakeResponse(payload=_sync_payload(50)),
    })

    plugin = build_plugin(module)
    move_bricks = module.SiQiCleanBeachV3._SiQiCleanBeachV3__move_bricks

    result = asyncio.run(move_bricks(plugin, "c_secure_pass=token"))

    assert "move_brick" not in ActionRequestUtils.actions
    assert result["moved"] == 0
    assert result["daily_bricks"] == 50
    assert result["remaining"] == 0
    assert result["success"] is True


def test_move_bricks_stops_on_failed_move(monkeypatch):
    """第二次搬砖失败后立即停止并保留服务端消息。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module.SiQiCleanBeachV3, "BRICK_MOVE_INTERVAL", 0)
    ActionRequestUtils.reset({
        "sync_game_state": FakeResponse(payload=_sync_payload(10)),
        "move_brick": [
            FakeResponse(payload=_move_payload(11)),
            FakeResponse(payload={"success": False, "message": "今日搬砖次数已达上限"}),
        ],
    })

    plugin = build_plugin(module)
    move_bricks = module.SiQiCleanBeachV3._SiQiCleanBeachV3__move_bricks

    result = asyncio.run(move_bricks(plugin, "c_secure_pass=token"))

    assert ActionRequestUtils.actions.count("move_brick") == 2
    assert result["moved"] == 1
    assert result["success"] is False
    assert result["message"] == "今日搬砖次数已达上限"


def test_run_clean_task_skips_bricks_when_disabled(monkeypatch):
    """brick_enabled=False 时清理任务完全不触发搬砖。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    ActionRequestUtils.reset({
        "enter_beach": FakeResponse(payload={"success": True}),
        "collect_all_trash": FakeResponse(
            payload={"success": True, "data": {"collected": 2}}
        ),
    })

    plugin = build_plugin(module)
    plugin._cookie = "c_secure_pass=token"
    plugin._brick_enabled = False

    result = asyncio.run(plugin.run_clean_task())

    assert "sync_game_state" not in ActionRequestUtils.actions
    assert "move_brick" not in ActionRequestUtils.actions
    assert result["status"] == "completed"
    assert result["data"]["bricks"] == 0
    assert result["data"]["trash"] == 2


def test_run_clean_task_moves_bricks_before_beach(monkeypatch):
    """开启搬砖时先搬砖再进沙滩，并写入 data.bricks。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module.SiQiCleanBeachV3, "BRICK_MOVE_INTERVAL", 0)
    ActionRequestUtils.reset({
        "sync_game_state": FakeResponse(payload=_sync_payload(49)),
        "move_brick": [FakeResponse(payload=_move_payload(50))],
        "enter_beach": FakeResponse(payload={"success": True}),
        "collect_all_trash": FakeResponse(
            payload={"success": True, "data": {"collected": 1}}
        ),
    })

    plugin = build_plugin(module)
    plugin._cookie = "c_secure_pass=token"

    result = asyncio.run(plugin.run_clean_task())

    assert ActionRequestUtils.actions[0] == "sync_game_state"
    assert ActionRequestUtils.actions[1] == "move_brick"
    assert "enter_beach" in ActionRequestUtils.actions
    assert ActionRequestUtils.actions.index("move_brick") < ActionRequestUtils.actions.index("enter_beach")
    assert result["data"]["bricks"] == 1
    assert "搬砖1块" in plugin._stats["last_result"]


def test_init_plugin_restores_brick_state(monkeypatch):
    """init_plugin 可重复调用，并恢复持久化的搬砖状态。"""
    module = load_plugin_module()

    class StubSiteOper:
        """隔离站点数据库查询。"""

        @staticmethod
        def get_by_domain(domain):
            return None

        @staticmethod
        async def async_get_by_domain(domain):
            return None

    monkeypatch.setattr(module, "SiteOper", StubSiteOper)

    plugin = build_plugin(module)
    plugin._data["bricks"] = {
        "daily": 7,
        "pocket": 12,
        "remaining": 43,
        "last_update": 1,
        "next_reset": 0,
    }

    plugin.init_plugin({"enabled": True, "brick_enabled": False, "brick_max_per_run": 999})

    assert plugin._brick_enabled is False
    assert plugin._brick_max_per_run == 50
    assert plugin._bricks["daily"] == 7
    assert plugin._bricks["pocket"] == 12
    assert plugin._bricks["remaining"] == 43

    plugin.init_plugin({"enabled": True})

    assert plugin._brick_enabled is True
    assert plugin._brick_max_per_run == 50
    assert plugin._bricks["daily"] == 7
    assert plugin._bricks["pocket"] == 12


def test_get_page_shows_brick_cards():
    """详情页展示今日搬砖与口袋砖块卡片。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._bricks = {
        "daily": 5,
        "pocket": 9,
        "remaining": 45,
        "last_update": None,
        "next_reset": 0,
    }

    page_text = json.dumps(plugin.get_page(), ensure_ascii=False)

    assert "今日搬砖" in page_text
    assert "5/50" in page_text
    assert "口袋砖块" in page_text
    assert "9块" in page_text


def test_notification_mentions_bricks():
    """通知文本在 data.bricks 大于 0 时追加搬砖行。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    sent = {}
    plugin.post_message = lambda **kwargs: sent.update(kwargs)
    notify = module.SiQiCleanBeachV3._SiQiCleanBeachV3__send_notification

    notify(plugin, {"status": "completed", "message": "ok", "data": {"bricks": 3}})
    assert "🧱 搬砖：3 块" in sent["text"]

    sent.clear()
    notify(plugin, {"status": "completed", "message": "ok", "data": {"bricks": 0}})
    assert "搬砖" not in sent["text"]


def test_move_bricks_api_schedules_once_job(monkeypatch):
    """/bricks 接口提交一次性任务，且不受自动搬砖开关约束。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    scheduled = []
    monkeypatch.setattr(
        module.scheduler_sdk,
        "add_plugin_once_job",
        lambda *args, **kwargs: scheduled.append((args, kwargs)),
    )

    result = asyncio.run(plugin.move_bricks_api())

    assert result["success"] is True
    assert result["moved"] == 0
    assert scheduled and scheduled[0][0][1] == "brick_run_once"

    plugin._brick_enabled = False
    result = asyncio.run(plugin.move_bricks_api())

    assert result["success"] is True
    assert len(scheduled) == 2
    assert scheduled[1][0][1] == "brick_run_once"


class _MiniMonkeyPatch:
    """无 pytest 时的最小 monkeypatch 替代，仅支持 setattr。"""

    def __init__(self) -> None:
        self._undo: list = []

    def setattr(self, target, name, value) -> None:
        self._undo.append((target, name, getattr(target, name)))
        setattr(target, name, value)

    def undo(self) -> None:
        for target, name, original in reversed(self._undo):
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

