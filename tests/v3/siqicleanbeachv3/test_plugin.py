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
    bodies: list = []
    responses: dict = {}
    inventory_html: str = ""

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    @classmethod
    def reset(cls, responses=None, inventory_html: str = "") -> None:
        cls.actions = []
        cls.bodies = []
        cls.responses = dict(responses or {})
        cls.inventory_html = inventory_html

    async def post_res(self, url, **kwargs):
        data = kwargs.get("data") or {}
        action = data.get("action", "")
        ActionRequestUtils.actions.append(action)
        ActionRequestUtils.bodies.append(dict(data))
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
    plugin._craft_enabled = True
    plugin._craft_target_pills = 1
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
    plugin._craft = {
        "last_craft": [],
        "pills_total": 0,
        "last_message": "",
        "last_update": None,
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
    assert cls.plugin_version == "1.3.0"
    assert cls.plugin_config_prefix == "siqicleanbeachv3_"
    assert cls.plugin_author == "tycgd"
    assert cls.author_url == "https://github.com/tycgd"
    assert cls.auth_level == 1

    package = json.loads((PLUGIN_INIT.parents[2] / "package.v3.json").read_text("utf-8"))
    entry = package[cls.__name__]
    assert entry["version"] == cls.plugin_version, "package.v3.json 版本与 plugin_version 不一致"
    assert f"v{cls.plugin_version}" in entry["history"], "history 缺少当前版本记录"
    assert entry["icon"] == cls.plugin_icon
    assert entry["name"] == cls.plugin_name
    assert entry["author"] == "tycgd"
    assert entry["level"] == 1
    assert entry["version"] == "1.3.0"


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
    assert asyncio.iscoroutinefunction(cls.craft_api)


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

    expected = [
        "run_once",
        "api_run_once",
        "command_run_once",
        "brick_run_once",
        "craft_run_once",
    ]
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


CRAFT_RECIPES_JS = """const recipes = [
    {"0":"1","recipe_id":"1","result_quantity":"1","result_name":"木工件","ingredients":[
        {"recipe_id":"1","quantity":"5","name":"砖块"},
        {"recipe_id":"1","quantity":"1","name":"木材"},
        {"recipe_id":"1","quantity":"1","name":"塑料袋"}]},
    {"0":"2","recipe_id":"2","result_quantity":"1","result_name":"塑料件","ingredients":[
        {"recipe_id":"2","quantity":"5","name":"砖块"},
        {"recipe_id":"2","quantity":"1","name":"塑料袋"},
        {"recipe_id":"2","quantity":"1","name":"瓶子"}]},
    {"0":"3","recipe_id":"3","result_quantity":"1","result_name":"简易工具","ingredients":[
        {"recipe_id":"3","quantity":"2","name":"螺丝"},
        {"recipe_id":"3","quantity":"2","name":"木工件"}]},
    {"0":"4","recipe_id":"4","result_quantity":"1","result_name":"能量碎片","ingredients":[
        {"recipe_id":"4","quantity":"1","name":"旧电池"},
        {"recipe_id":"4","quantity":"2","name":"塑料件"}]},
    {"0":"5","recipe_id":"5","result_quantity":"1","result_name":"魔丸胚胎","ingredients":[
        {"recipe_id":"5","quantity":"1","name":"破铜片"},
        {"recipe_id":"5","quantity":"1","name":"简易工具"},
        {"recipe_id":"5","quantity":"1","name":"能量碎片"}]},
    {"0":"6","recipe_id":"6","result_quantity":"1","result_name":"魔丸","ingredients":[
        {"recipe_id":"6","quantity":"10","name":"砖块"},
        {"recipe_id":"6","quantity":"2","name":"魔丸胚胎"}]}
];"""


def _rich_inventory() -> dict:
    """构造刚好够炼出 1 颗魔丸的库存。"""
    return {
        "砖块": 50,
        "木材": 4,
        "塑料袋": 8,
        "瓶子": 4,
        "螺丝": 4,
        "旧电池": 2,
        "破铜片": 2,
        "木工件": 0,
        "塑料件": 0,
        "简易工具": 0,
        "能量碎片": 0,
        "魔丸胚胎": 0,
        "魔丸": 0,
    }


def _inventory_page(counts: dict, recipes_js: str = CRAFT_RECIPES_JS) -> str:
    """拼装包含库存网格与炼造配方的 mowan.php 页面。"""
    items = "".join(
        f'<div class="inventory-item"><span class="item-icon">x</span>'
        f'<div class="item-name">{name}</div>'
        f'<div class="item-count">{count}</div></div>'
        for name, count in counts.items()
    )
    return (
        f'<div class="inventory-grid">{items}</div>'
        f'<div class="recipe-grid"></div><script>{recipes_js}</script>'
    )


def test_parse_recipes_from_inline_js():
    """内联 JS 中 const recipes 可解析出 6 条配方且材料正确。"""
    module = load_plugin_module()
    parse = module.SiQiCleanBeachV3._SiQiCleanBeachV3__parse_recipes

    recipes = parse(CRAFT_RECIPES_JS)

    assert [recipe["recipe_id"] for recipe in recipes] == [1, 2, 3, 4, 5, 6]
    assert [recipe["result_name"] for recipe in recipes] == [
        "木工件", "塑料件", "简易工具", "能量碎片", "魔丸胚胎", "魔丸"
    ]
    assert recipes[0]["ingredients"] == [
        {"name": "砖块", "quantity": 5},
        {"name": "木材", "quantity": 1},
        {"name": "塑料袋", "quantity": 1},
    ]
    assert recipes[4]["ingredients"] == [
        {"name": "破铜片", "quantity": 1},
        {"name": "简易工具", "quantity": 1},
        {"name": "能量碎片", "quantity": 1},
    ]
    assert recipes[5]["result_quantity"] == 1
    assert parse("<html></html>") == []
    assert parse("const recipes = [broken;") == []


def test_plan_craft_uses_bricks_exactly():
    """库存充足时按材料到成品顺序规划，砖块恰好用掉 50。"""
    module = load_plugin_module()
    parse = module.SiQiCleanBeachV3._SiQiCleanBeachV3__parse_recipes
    planner = module.SiQiCleanBeachV3._SiQiCleanBeachV3__plan_craft

    recipes = parse(CRAFT_RECIPES_JS)
    inventory = _rich_inventory()
    plan = planner(object.__new__(module.SiQiCleanBeachV3), inventory, recipes, 1)

    assert [recipe_id for recipe_id, _ in plan] == [1, 3, 2, 4, 5, 6]
    assert plan == [(1, 4), (3, 2), (2, 4), (4, 2), (5, 2), (6, 1)]

    by_id = {recipe["recipe_id"]: recipe for recipe in recipes}
    positions = {recipe_id: index for index, (recipe_id, _) in enumerate(plan)}
    for index, (recipe_id, _) in enumerate(plan):
        for ingredient in by_id[recipe_id]["ingredients"]:
            producer = next(
                (
                    recipe["recipe_id"]
                    for recipe in recipes
                    if recipe["result_name"] == ingredient["name"]
                ),
                None,
            )
            if producer in positions:
                assert positions[producer] < index

    sim = dict(inventory)
    for recipe_id, make in plan:
        recipe = by_id[recipe_id]
        for ingredient in recipe["ingredients"]:
            sim[ingredient["name"]] -= ingredient["quantity"] * make
        sim[recipe["result_name"]] += make * recipe["result_quantity"]

    assert sim["魔丸"] == 1
    assert sim["砖块"] == 0


def test_craft_skips_when_bricks_insufficient(monkeypatch):
    """砖块只有 1 块时规划为空，不发送任何 craft_item。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module.SiQiCleanBeachV3, "CRAFT_INTERVAL", 0)

    inventory = _rich_inventory()
    inventory["砖块"] = 1
    ActionRequestUtils.reset(inventory_html=_inventory_page(inventory))

    plugin = build_plugin(module)
    craft = module.SiQiCleanBeachV3._SiQiCleanBeachV3__craft_items

    result = asyncio.run(craft(plugin, "c_secure_pass=token"))

    assert result["crafted"] == []
    assert "craft_item" not in ActionRequestUtils.actions


def test_plan_craft_drops_wasteful_intermediates(monkeypatch):
    """砖块不足以炼出魔丸时丢弃中间件规划，避免白吃砖块。"""
    module = load_plugin_module()
    parse = module.SiQiCleanBeachV3._SiQiCleanBeachV3__parse_recipes
    planner = module.SiQiCleanBeachV3._SiQiCleanBeachV3__plan_craft

    inventory = _rich_inventory()
    inventory["砖块"] = 31
    plan = planner(
        object.__new__(module.SiQiCleanBeachV3),
        inventory,
        parse(CRAFT_RECIPES_JS),
        1,
    )

    assert plan == []


def test_plan_craft_reuses_existing_intermediates():
    """已有中间件时只补缺口，仅规划胚胎与魔丸。"""
    module = load_plugin_module()
    parse = module.SiQiCleanBeachV3._SiQiCleanBeachV3__parse_recipes
    planner = module.SiQiCleanBeachV3._SiQiCleanBeachV3__plan_craft

    inventory = {
        "砖块": 10,
        "破铜片": 2,
        "简易工具": 2,
        "能量碎片": 2,
        "魔丸胚胎": 0,
        "魔丸": 0,
    }
    plan = planner(
        object.__new__(module.SiQiCleanBeachV3),
        inventory,
        parse(CRAFT_RECIPES_JS),
        1,
    )

    assert plan == [(5, 2), (6, 1)]


def test_craft_stops_on_midway_failure(monkeypatch):
    """第 2 条 craft_item 失败后停止，保留已成功条目与失败消息。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module.SiQiCleanBeachV3, "CRAFT_INTERVAL", 0)
    ActionRequestUtils.reset(
        {
            "craft_item": [
                FakeResponse(payload={"success": True, "message": "炼造成功"}),
                FakeResponse(payload={"success": False, "message": "材料不足"}),
            ]
        },
        inventory_html=_inventory_page(_rich_inventory()),
    )

    plugin = build_plugin(module)
    craft = module.SiQiCleanBeachV3._SiQiCleanBeachV3__craft_items

    result = asyncio.run(craft(plugin, "c_secure_pass=token"))

    assert ActionRequestUtils.actions.count("craft_item") == 2
    assert ActionRequestUtils.bodies[0]["recipe_id"] == 1
    assert ActionRequestUtils.bodies[0]["quantity"] == 4
    assert ActionRequestUtils.bodies[1]["recipe_id"] == 3
    assert result["success"] is False
    assert result["crafted"] == [{"name": "木工件", "qty": 4}]
    assert result["message"] == "材料不足"


def test_craft_disabled_skips_in_clean_task(monkeypatch):
    """craft_enabled=False 时清理任务完全不触发炼造。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    ActionRequestUtils.reset(
        {
            "enter_beach": FakeResponse(payload={"success": True}),
            "collect_all_trash": FakeResponse(
                payload={"success": True, "data": {"collected": 2}}
            ),
        },
        inventory_html=_inventory_page(_rich_inventory()),
    )

    plugin = build_plugin(module)
    plugin._cookie = "c_secure_pass=token"
    plugin._craft_enabled = False

    result = asyncio.run(plugin.run_clean_task())

    assert "craft_item" not in ActionRequestUtils.actions
    assert result["status"] == "completed"
    assert result["data"]["craft"] == {}


def test_rename_and_version_consistency():
    """显示名改为思齐炼魔丸，两处版本与 history 顶部保持一致。"""
    module = load_plugin_module()
    cls = module.SiQiCleanBeachV3
    package = json.loads((PLUGIN_INIT.parents[2] / "package.v3.json").read_text("utf-8"))
    entry = package[cls.__name__]

    assert cls.plugin_name == "思齐炼魔丸"
    assert cls.plugin_version == "1.3.0"
    assert entry["version"] == "1.3.0"
    assert entry["name"] == "思齐炼魔丸"
    assert "炼造" in entry["description"]
    assert next(iter(entry["history"])) == "v1.3.0"
    assert "详情页" in entry["history"]["v1.3.0"]
    assert "炼造" in entry["history"]["v1.2.0"]
    assert "更名" in entry["history"]["v1.2.0"]


def test_get_page_shows_craft_cards():
    """详情页在搬砖卡片旁展示魔丸与魔丸胚胎数量。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._inventory = {
        "魔丸": {"name": "魔丸", "count": 4, "icon": "⚗️"},
        "魔丸胚胎": {"name": "魔丸胚胎", "count": 2, "icon": "🥚"},
    }

    page_text = json.dumps(plugin.get_page(), ensure_ascii=False)

    assert "魔丸" in page_text
    assert "4颗" in page_text
    assert "魔丸胚胎" in page_text
    assert "2个" in page_text


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


def test_page_uses_only_whitelisted_components():
    """详情页只能使用宿主已验证注册的 Vuetify 组件。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._inventory = {
        "木材": {"name": "木材", "count": 12, "icon": "🪵"},
        "魔丸": {"name": "魔丸", "count": 4, "icon": "⚗️"},
    }
    plugin._stats["history"] = [
        {
            "time": 1793150958,
            "result": "收集2个垃圾",
            "total_count": 16,
            "status": "completed",
        },
        {
            "time": 1793150959,
            "result": "未能收集垃圾",
            "total_count": 16,
            "status": "partial",
        },
        {
            "time": 1793150960,
            "result": "请求失败",
            "total_count": 0,
            "status": "failed",
        },
    ]

    found = _collect_components(plugin.get_page(), set())

    assert found
    assert found <= PAGE_COMPONENT_WHITELIST, found - PAGE_COMPONENT_WHITELIST


def test_page_renders_without_data():
    """空库存、空历史且未配置 Cookie 时详情页仍能正常渲染。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    plugin._inventory = {}
    plugin._stats = {
        "last_update": None,
        "last_result": "",
        "total_items_count": 0,
        "history": [],
    }
    plugin._cookie = ""
    plugin._bricks = {}

    page = plugin.get_page()

    assert isinstance(page, list)
    assert page
    page_text = json.dumps(page, ensure_ascii=False)
    assert "暂无物品数据" in page_text
    assert "暂无执行记录" in page_text


def test_run_clean_task_crafts_after_collect(monkeypatch):
    """清理任务在收集与刷新后执行炼造，并持久化炼造状态。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    monkeypatch.setattr(module.SiQiCleanBeachV3, "CRAFT_INTERVAL", 0)
    ActionRequestUtils.reset(
        {
            "enter_beach": FakeResponse(payload={"success": True}),
            "collect_all_trash": FakeResponse(
                payload={"success": True, "data": {"collected": 3}}
            ),
        },
        inventory_html=_inventory_page(_rich_inventory()),
    )

    plugin = build_plugin(module)
    plugin._cookie = "c_secure_pass=token"

    result = asyncio.run(plugin.run_clean_task())

    assert result["status"] == "completed"
    assert result["data"]["craft"]["pills"] == 1
    assert result["data"]["craft"]["success"] is True
    assert ActionRequestUtils.actions.index("enter_beach") < ActionRequestUtils.actions.index(
        "craft_item"
    )
    assert "炼造魔丸1颗" in plugin._stats["last_result"]
    assert plugin._data["craft"]["pills_total"] == 1
    assert plugin._data["craft"]["last_craft"][-1] == {"name": "魔丸", "qty": 1}
    assert plugin._data["craft"]["last_update"]


def test_craft_api_schedules_once_job(monkeypatch):
    """/craft 提交一次性任务，且停用时会遍历到 craft_run_once。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    scheduled = []
    monkeypatch.setattr(
        module.scheduler_sdk,
        "add_plugin_once_job",
        lambda *args, **kwargs: scheduled.append((args, kwargs)),
    )

    result = asyncio.run(plugin.craft_api())

    assert result["success"] is True
    assert scheduled and scheduled[0][0][1] == "craft_run_once"
    assert "craft_run_once" in module.SiQiCleanBeachV3.ONCE_JOB_IDS
    assert "/craft" in [api["path"] for api in plugin.get_api()]


def test_craft_skips_when_recipes_unparsable(monkeypatch):
    """配方解析失败时降级跳过炼造，不抛异常也不发请求。"""
    module = load_plugin_module()
    monkeypatch.setattr(module, "AsyncRequestUtils", ActionRequestUtils)
    ActionRequestUtils.reset(
        inventory_html=_inventory_page(_rich_inventory(), "const recipes = [;")
    )

    plugin = build_plugin(module)
    craft = module.SiQiCleanBeachV3._SiQiCleanBeachV3__craft_items

    result = asyncio.run(craft(plugin, "c_secure_pass=token"))

    assert result["success"] is False
    assert "配方" in result["message"]
    assert "craft_item" not in ActionRequestUtils.actions


def test_notification_mentions_crafted_pills():
    """通知文本在 data.craft.pills 大于 0 时追加炼造行。"""
    module = load_plugin_module()
    plugin = build_plugin(module)
    sent = {}
    plugin.post_message = lambda **kwargs: sent.update(kwargs)
    notify = module.SiQiCleanBeachV3._SiQiCleanBeachV3__send_notification

    notify(
        plugin,
        {
            "status": "completed",
            "message": "ok",
            "data": {"craft": {"pills": 2}},
        },
    )
    assert "⚗️ 炼造魔丸：2 颗" in sent["text"]

    sent.clear()
    notify(
        plugin,
        {
            "status": "completed",
            "message": "ok",
            "data": {"craft": {"pills": 0}},
        },
    )
    assert "炼造魔丸" not in sent["text"]


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

