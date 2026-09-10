"""identity-guard 本地行为回归测试（FakeHost，不需要 MaiBot 真机、不需要 Napcat）。

覆盖链路：入站载荷解析 -> 建档/改名记录 -> 撞名检测 -> 群内提醒 -> 命令查询 -> 落盘回读。

用法:
    python test_identity_guard.py
退出码: 0=全部通过, 1=有失败
"""
import asyncio
import json
import re
import sys
import tempfile
import time
from pathlib import Path

_PLUGIN_DIR = str(Path(__file__).resolve().parent)
if _PLUGIN_DIR not in sys.path:
    sys.path.insert(0, _PLUGIN_DIR)

from maibot_sdk.context import PluginContext, PluginPaths  # noqa: E402

from identity_store import IdentityStore, normalize_name, similarity  # noqa: E402
from plugin import (  # noqa: E402
    create_plugin,
    extract_display_name,
    extract_group_id,
    extract_user_id,
)

BOT_ID = "999999"  # FakeHost 模拟的机器人自身 QQ
GROUP = "10086"
STREAM = "s-10086"


def make_message(user_id="10001", card="", nick="", text="大家好",
                 group_id=GROUP, stream_id=STREAM):
    """构造一条入站群消息载荷（按 MaiBot message_info 结构）。"""
    return {
        "session_id": stream_id,
        "processed_plain_text": text,
        "message_info": {
            "group_info": {"group_id": group_id, "group_name": "测试群"},
            "user_info": {
                "user_id": user_id,
                "user_nickname": nick or f"昵称{user_id}",
                "user_cardname": card,
            },
        },
    }


class FakeHost:
    """记录插件对宿主的调用，并按能力返回假数据。"""

    def __init__(self) -> None:
        self.sent_text: list = []
        self.api_calls: list = []
        self.stream_lookups: list = []

    async def rpc_call(self, method, plugin_id, payload, timeout_ms=None):
        if method != "cap.call":
            raise RuntimeError(f"FakeHost 不支持的 RPC: {method}")
        cap = (payload or {}).get("capability", "")
        args = (payload or {}).get("args") or {}
        if cap == "send.text":
            self.sent_text.append(args.get("text", ""))
            return True
        if cap == "api.call":
            self.api_calls.append(args)
            return {"success": True, "data": {"user_id": BOT_ID}}
        if cap == "chat.get_stream_by_group_id":
            self.stream_lookups.append(args)
            return {"stream_id": STREAM}
        return True


class Runner:
    """极简测试框架。"""

    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        if cond:
            self.passed += 1
            print(f"  [OK] {name}")
        else:
            self.failed += 1
            print(f"  [FAIL] {name}{(' -> ' + detail) if detail else ''}")

    def eq(self, name: str, actual, expected) -> None:
        self.check(name, actual == expected, f"期望 {expected!r}, 实际 {actual!r}")


async def new_plugin(host: FakeHost, tmp: Path, **cfg_overrides):
    """造一个已加载的插件实例（data_dir 指向临时目录）。"""
    plugin = create_plugin()
    plugin._set_context(PluginContext(
        "org.mai-mai.identity-guard", host.rpc_call,
        PluginPaths(data_dir=str(tmp / "data"), runtime_dir=str(tmp / "runtime")),
    ))
    plugin.set_plugin_config(plugin.get_default_config())
    plugin.config.guard.alert_cooldown_seconds = 0
    plugin.config.guard.group_alert_cooldown_seconds = 0
    plugin.config.guard.admin_ids = ["10000"]
    for key, value in cfg_overrides.items():
        setattr(plugin.config.guard, key, value)
    await plugin.on_load()
    return plugin


async def speak(plugin, host, user_id, card="", nick="", text="大家好", group_id=GROUP):
    """模拟一条入站消息 + 等后台发完提醒。"""
    await plugin.observe_message(message=make_message(user_id, card, nick, text, group_id))
    await asyncio.sleep(0.05)
    return host.sent_text


# ----------------------------------------------------------------------
# 用例
# ----------------------------------------------------------------------
async def t_parse(r: Runner):
    print("\n[1] 载荷解析")
    msg = make_message("10001", card="群名片A", nick="昵称A")
    r.eq("群号提取", extract_group_id(msg), GROUP)
    r.eq("QQ 号提取", extract_user_id(msg), "10001")
    r.eq("群名片优先于昵称", extract_display_name(msg), "群名片A")
    r.eq("无名片时回退昵称", extract_display_name(make_message("10002", nick="昵称B")), "昵称B")

    # 扁平载荷（user_info 不在 message_info 下）
    flat = {"group_id": "777", "user_id": "10003", "user_nickname": "扁平"}
    r.eq("扁平载荷群号", extract_group_id(flat), "777")
    r.eq("扁平载荷 QQ", extract_user_id(flat), "10003")
    r.eq("扁平载荷名字", extract_display_name(flat), "扁平")


async def t_normalize(r: Runner):
    print("\n[2] 名字归一化与相似度")
    r.eq("去空格", normalize_name(" 张 三 "), "张三")
    r.eq("去 emoji 与符号", normalize_name("张三✨⭐"), "张三")
    r.eq("英文小写化", normalize_name("Zhang-San"), "zhangsan")
    r.eq("伪装加空格仍判一致", similarity(normalize_name("张三"), normalize_name("张 三")), 1.0)
    r.check("相似名字高相似度", similarity("张三丰", "张三峰") >= 0.6,
            f"实际 {similarity('张三丰', '张三峰'):.3f}")
    r.check("无关名字低相似度", similarity("张三", "隔壁老王") < 0.5,
            f"实际 {similarity('张三', '隔壁老王'):.3f}")


async def t_store_basic(r: Runner, tmp: Path):
    print("\n[3] 档案库：建档 / 改名 / 撞名")
    store = IdentityStore(tmp / "s.json")
    store.load()
    obs = store.record_observation(GROUP, "10001", "张三")
    r.check("新成员建档", obs.is_new_member)
    r.check("新成员无冲突", not obs.has_conflict)

    obs = store.record_observation(GROUP, "10002", "李四")
    r.check("第二个成员无冲突", not obs.has_conflict)

    # 10002 改成与 10001 完全相同的名字 -> 冲突
    obs = store.record_observation(GROUP, "10002", "张三")
    r.check("改名被识别", obs.name_changed)
    r.eq("旧名字保留", obs.old_name, "李四")
    r.check("撞名冲突被检出", obs.has_conflict)
    if obs.conflicts:
        r.eq("冲突指向原主人", obs.conflicts[0].other_user_id, "10001")
        r.check("完全一致标记", obs.conflicts[0].exact)

    # 同一个人换回自己用过的名字 -> 不算冲突
    obs = store.record_observation(GROUP, "10002", "李四")
    r.check("换回自己的旧名字不算冒充", not obs.has_conflict)

    # 加空格伪装
    obs = store.record_observation(GROUP, "10003", "张 三")
    r.check("加空格伪装仍被检出", obs.has_conflict, f"conflicts={obs.conflicts}")

    profile = store.get_profile(GROUP, "10002")
    r.check("名片历史含两个名字", profile and len(profile["names"]) == 2,
            f"实际 {profile and [n['name'] for n in profile['names']]}")
    r.check("改名事件已记录", len(store.recent_events(GROUP, 10)) >= 2)
    r.check("按名字可查到人", any(h["user_id"] == "10001" for h in store.find_by_name(GROUP, "张三")))

    # 可信名单：本人改名不告警
    obs = store.record_observation(GROUP, "10001", "张四", trusted_ids=["10001"])
    r.check("可信成员改名不告警", not obs.has_conflict)

    # 落盘回读
    store.save()
    store2 = IdentityStore(tmp / "s.json")
    store2.load()
    r.eq("落盘后档案数一致", store2.stats()["members"], store.stats()["members"])
    r.check("落盘后历史名片保留",
            store2.get_profile(GROUP, "10002")["current"] == "李四")


async def t_hook_alert(r: Runner, tmp: Path):
    print("\n[4] Hook：撞名自动提醒")
    host = FakeHost()
    plugin = await new_plugin(host, tmp)

    await speak(plugin, host, "10001", card="张三")
    r.eq("首次出现不提醒", len(host.sent_text), 0)

    await speak(plugin, host, "10002", card="张三")
    r.eq("新人撞名触发提醒", len(host.sent_text), 1)
    if host.sent_text:
        text = host.sent_text[0]
        r.check("提醒含冒充者 QQ", "10002" in text, text)
        r.check("提醒含被冒充者 QQ", "10001" in text, text)
        r.check("提醒含一致/相似度说明", "完全一致" in text or "相似度" in text, text)

    # 冷却：同一人同一名字不重复刷
    await speak(plugin, host, "10002", card="张三", nick="x")
    r.eq("冷却期内不重复提醒", len(host.sent_text), 1)

    await plugin.on_unload()
    return plugin


async def t_hook_no_alert_cases(r: Runner, tmp: Path):
    print("\n[5] Hook：不该提醒的场景")
    host = FakeHost()
    plugin = await new_plugin(host, tmp)

    await speak(plugin, host, "10001", card="张三")
    # 自己换回自己曾用过的名字
    await speak(plugin, host, "10001", card="张三三")
    await speak(plugin, host, "10001", card="张三三", nick="y")
    r.eq("本人反复改名不提醒", len(host.sent_text), 0)

    # 忽略名单
    plugin.config.guard.ignored_user_ids = ["20000"]
    await speak(plugin, host, "20000", card="张三")
    r.eq("忽略名单不提醒", len(host.sent_text), 0)
    await plugin.on_unload()

    # 关闭提醒开关
    host2 = FakeHost()
    plugin2 = await new_plugin(host2, tmp / "b", alert_enabled=False)
    await speak(plugin2, host2, "10001", card="王五")
    await speak(plugin2, host2, "10002", card="王五")
    r.eq("关闭提醒后静默记录", len(host2.sent_text), 0)
    r.check("关闭提醒后仍建档", bool(plugin2._store.get_profile(GROUP, "10002")))
    await plugin2.on_unload()


async def t_commands(r: Runner, tmp: Path):
    print("\n[6] 命令：/身份 /我是谁 /改名记录 /身份状态")
    host = FakeHost()
    plugin = await new_plugin(host, tmp)
    await speak(plugin, host, "10001", card="张三")
    await speak(plugin, host, "10001", card="老张")
    await speak(plugin, host, "10002", card="李四")
    host.sent_text.clear()

    ok, text, level = await plugin.cmd_identity(
        matched_groups={"arg": "10001"}, raw_message="/身份 10001",
        stream_id=STREAM, user_id="10002", message=make_message("10002"))
    r.check("/身份 命令成功", ok)
    r.check("/身份 输出含当前名片", "老张" in text, text)
    r.check("/身份 输出含历史名片", "张三" in text, text)
    r.check("/身份 已发出消息", "老张" in (host.sent_text[0] if host.sent_text else ""), str(host.sent_text))
    r.eq("/身份 拦截级别", level, 2)

    host.sent_text.clear()
    ok, text, _ = await plugin.cmd_identity(
        matched_groups={}, raw_message="/身份", stream_id=STREAM,
        user_id="10002", message=make_message("10002"))
    r.check("/身份 留空查自己", ok and "10002" in text, text)

    host.sent_text.clear()
    ok, text, _ = await plugin.cmd_whoami(
        raw_message="/我是谁", stream_id=STREAM, user_id="10001",
        message=make_message("10001"))
    r.check("/我是谁 输出自己档案", ok and "QQ 10001" in text, text)

    host.sent_text.clear()
    ok, text, _ = await plugin.cmd_renamelog(
        matched_groups={}, raw_message="/改名记录 5", stream_id=STREAM,
        user_id="10001", message=make_message("10001"))
    r.check("/改名记录 输出变更", ok and "→" in text, text)
    r.check("/改名记录 含旧新名字", "张三" in text and "老张" in text, text)

    host.sent_text.clear()
    ok, text, _ = await plugin.cmd_status(
        raw_message="/身份状态", stream_id=STREAM, user_id="10000",
        message=make_message("10000"))
    r.check("/身份状态 输出统计", ok and "成员档案" in text, text)

    # 名字模糊查询
    host.sent_text.clear()
    ok, text, _ = await plugin.cmd_identity(
        matched_groups={"arg": "老张"}, raw_message="/身份 老张",
        stream_id=STREAM, user_id="10002", message=make_message("10002"))
    r.check("/身份 支持按名字查", ok and "10001" in text, text)

    await plugin.on_unload()


async def t_admin(r: Runner, tmp: Path):
    print("\n[7] 管理命令鉴权与生效")
    host = FakeHost()
    plugin = await new_plugin(host, tmp)
    await speak(plugin, host, "10001", card="张三")
    host.sent_text.clear()

    # 非管理员
    ok, text, _ = await plugin.cmd_trust(
        matched_groups={"arg": "10002"}, raw_message="/身份信任 10002",
        stream_id=STREAM, user_id="10002", message=make_message("10002"))
    r.check("非管理员被拒绝", "没有权限" in text, text)
    r.check("非管理员未写入可信", not plugin._store.is_trusted(GROUP, "10002"))

    # 管理员
    host.sent_text.clear()
    ok, text, _ = await plugin.cmd_trust(
        matched_groups={"arg": "10002"}, raw_message="/身份信任 10002",
        stream_id=STREAM, user_id="10000", message=make_message("10000"))
    r.check("管理员可标记可信", "可信名单" in text, text)
    r.check("可信状态已写入", plugin._store.is_trusted(GROUP, "10002"))

    # 可信成员改名不再提醒（但别人冒用他的名字照样提醒）
    host.sent_text.clear()
    await speak(plugin, host, "10002", card="张三")
    r.eq("可信成员改名不提醒", len(host.sent_text), 0)
    await speak(plugin, host, "10003", card="张三")
    r.check("冒用可信成员名字仍提醒", len(host.sent_text) == 1, str(host.sent_text))

    # 备注
    host.sent_text.clear()
    ok, text, _ = await plugin.cmd_note(
        matched_groups={"arg": "10001", "rest": "群主，已线下核实"},
        raw_message="/身份备注 10001 群主，已线下核实",
        stream_id=STREAM, user_id="10000", message=make_message("10000"))
    r.check("备注写入成功", "备注" in text, text)
    r.eq("备注内容正确", plugin._store.get_profile(GROUP, "10001")["note"], "群主，已线下核实")

    # 取消信任
    host.sent_text.clear()
    await plugin.cmd_trust(
        matched_groups={"arg": "10002"}, raw_message="/身份取消信任 10002",
        stream_id=STREAM, user_id="10000", message=make_message("10000"))
    r.check("取消信任生效", not plugin._store.is_trusted(GROUP, "10002"))

    # 本地控制台放行（需显式给群号，控制台没有群上下文）
    ok, text, _ = await plugin.cmd_trust(
        matched_groups={"arg": "10003", "group": GROUP},
        raw_message=f"/身份信任 10003 {GROUP}",
        is_local_operator=True, stream_id=STREAM, user_id="", message={})
    r.check("本地控制台放行", "可信名单" in text, text)
    r.check("本地控制台写入生效", plugin._store.is_trusted(GROUP, "10003"))

    # 本地控制台没给群号 -> 给出可操作提示而不是静默失败
    ok, text, _ = await plugin.cmd_trust(
        matched_groups={"arg": "10004"}, raw_message="/身份信任 10004",
        is_local_operator=True, stream_id=STREAM, user_id="", message={})
    r.check("本地控制台缺群号有提示", "群号" in text, text)

    await plugin.on_unload()


async def t_tools(r: Runner, tmp: Path):
    print("\n[8] LLM 工具：check_identity / recent_name_changes")
    host = FakeHost()
    plugin = await new_plugin(host, tmp)
    await speak(plugin, host, "10001", card="张三")
    await speak(plugin, host, "10001", card="老张")

    result = await plugin.check_identity(target_user_id="10001", message=make_message("10002"))
    r.check("check_identity 返回成功", result.get("success"))
    r.check("check_identity 含历史名片", "张三" in result.get("content", ""), result.get("content"))

    result = await plugin.check_identity(display_name="老张", message=make_message("10002"))
    r.check("check_identity 支持按名字", "10001" in result.get("content", ""), result.get("content"))

    result = await plugin.check_identity(user_id="88888", message=make_message("10002"))
    r.check("查无档案时给出说明", "还没有" in result.get("content", ""), result.get("content"))

    result = await plugin.recent_name_changes(limit=3, message=make_message("10002"))
    r.check("recent_name_changes 返回记录", "→" in result.get("content", ""), result.get("content"))

    result = await plugin.check_identity(user_id="10001", message={"message_info": {}})
    r.check("非群聊场景给出提示", "仅支持群聊" in result.get("content", ""), result.get("content"))

    await plugin.on_unload()


async def t_patterns(r: Runner, tmp: Path):
    print("\n[9] 命令正则与装饰器绑定")
    host = FakeHost()
    plugin = await new_plugin(host, tmp)

    def pattern_of(method):
        info = getattr(method, "__maibot_component_info__", None)
        return getattr(info, "command_pattern", None) or getattr(info, "pattern", None)

    cases = {
        "cmd_identity": [("/身份", True), ("/身份 10001", True), ("／身份查询 10001", True),
                         ("/身份备注 10001 x", False), ("/身份状态", False)],
        "cmd_whoami": [("/我是谁", True), ("/我的身份", True), ("/我是谁呀", False)],
        "cmd_renamelog": [("/改名记录", True), ("/改名记录 5", True), ("/身份记录", True)],
        "cmd_trust": [("/身份信任 10001", True), ("/身份取消信任 10001", True), ("/身份 10001", False)],
        "cmd_note": [("/身份备注 10001 群主", True), ("/身份备注 10001", True)],
        "cmd_status": [("/身份状态", True), ("/身份统计", True), ("/身份状态 x", False)],
    }
    for method_name, samples in cases.items():
        pattern = pattern_of(getattr(plugin, method_name))
        if not pattern:
            r.check(f"{method_name} 取到正则", False)
            continue
        for sample, expect in samples:
            matched = re.fullmatch(pattern, sample) is not None
            r.check(f"{method_name} 匹配 {sample!r} == {expect}", matched == expect,
                    f"pattern={pattern}")

    hook_info = getattr(plugin.observe_message, "__maibot_component_info__", None)
    if hook_info is not None:
        r.eq("Hook 绑定的是 observe_message",
             getattr(hook_info, "handler_name", "observe_message"), "observe_message")
    for tool_name in ("check_identity", "recent_name_changes"):
        info = getattr(getattr(plugin, tool_name), "__maibot_component_info__", None)
        if info is not None:
            r.eq(f"Tool {tool_name} 绑定正确",
                 getattr(info, "handler_name", tool_name), tool_name)

    await plugin.on_unload()


async def t_persistence(r: Runner, tmp: Path):
    print("\n[10] 落盘与重载")
    host = FakeHost()
    plugin = await new_plugin(host, tmp)
    await speak(plugin, host, "10001", card="张三")
    await speak(plugin, host, "10002", card="李四")
    await plugin.on_unload()

    store_file = Path(plugin._store.path)
    r.check("档案文件已生成", store_file.exists(), str(store_file))
    data = json.loads(store_file.read_text(encoding="utf-8"))
    r.check("JSON 含群数据", GROUP in (data.get("groups") or {}), str(list(data.get("groups") or {})))

    # 换一个实例重新加载，档案应完整回来
    host2 = FakeHost()
    plugin2 = await new_plugin(host2, tmp)
    r.eq("重载后成员数", plugin2._store.stats()["members"], 2)
    r.check("重载后档案可用", plugin2._store.get_profile(GROUP, "10001")["current"] == "张三")
    await plugin2.on_unload()


async def t_audit_regressions(r: Runner, tmp: Path):
    print("\n[11] 审计回归：F1 emoji 中间名 / F3 冷却字典无界")
    from plugin import IdentityGuardPlugin

    # F1：纯 emoji 中间名不能产生检测盲区
    host = FakeHost()
    plugin = await new_plugin(host, tmp)
    await speak(plugin, host, "10001", card="张三")
    await speak(plugin, host, "10002", card="🌟🌟🌟")
    await speak(plugin, host, "10002", card="张三", nick="z")
    r.check("emoji 中间名后撞名仍触发提醒", len(host.sent_text) == 1, str(host.sent_text))
    if host.sent_text:
        r.check("提醒指向正确双方", "10002" in host.sent_text[0] and "10001" in host.sent_text[0],
                host.sent_text[0])
    await plugin.on_unload()

    # F3：群级冷却字典过期条目会被清理
    plugin2 = IdentityGuardPlugin()
    now = time.time()
    for i in range(100):
        plugin2._group_until[f"g{i}"] = now - 999999
    plugin2._alert_until[("g0", "u0", "n")] = now - 999999
    plugin2._prune_cooldown(now)
    r.check("群级冷却过期条目清零", len(plugin2._group_until) == 0,
            f"剩余 {len(plugin2._group_until)}")
    r.check("单人冷却过期条目清零", len(plugin2._alert_until) == 0,
            f"剩余 {len(plugin2._alert_until)}")

    # F4（真机实录 2026-09-10）：自定义 __init__ 漏调 super().__init__() 会让
    # SDK 基类的 _dynamic_api_components 缺失，Runner 注册组件时 AttributeError，
    # 整个插件 Runner 崩溃。FakeHost 之前全绿是因为测试从不调 get_components()。
    from collections import Counter
    plugin3 = IdentityGuardPlugin()
    try:
        comps = plugin3.get_components()
        kinds = Counter(c["type"] for c in comps)
        r.eq("get_components 组件总数", len(comps), 9)
        r.eq("组件类型分布", (kinds.get("HOOK_HANDLER", 0), kinds.get("TOOL", 0),
                             kinds.get("COMMAND", 0)), (1, 2, 6))
    except AttributeError as exc:
        r.check("get_components 可调用（super().__init__ 已补）", False, str(exc))


# ----------------------------------------------------------------------
async def main() -> int:
    r = Runner()
    tmp = Path(tempfile.mkdtemp(prefix="identity-guard-test-"))

    await t_parse(r)
    await t_normalize(r)
    await t_store_basic(r, tmp / "store")
    await t_hook_alert(r, tmp / "hook")
    await t_hook_no_alert_cases(r, tmp / "hook2")
    await t_commands(r, tmp / "cmd")
    await t_admin(r, tmp / "admin")
    await t_tools(r, tmp / "tool")
    await t_patterns(r, tmp / "pattern")
    await t_persistence(r, tmp / "persist")
    await t_audit_regressions(r, tmp / "audit")

    print(f"\n{'=' * 52}")
    print(f"通过 {r.passed} 项，失败 {r.failed} 项")
    print("=" * 52)
    return 1 if r.failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
