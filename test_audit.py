"""审计复现证据：用最小用例证明缺陷真实存在（修复前应 FAIL，修复后应 PASS）。"""
import asyncio
import sys
import tempfile
import time
from pathlib import Path

_PLUGIN_DIR = str(Path(__file__).resolve().parent)
if _PLUGIN_DIR not in sys.path:
    sys.path.insert(0, _PLUGIN_DIR)

from identity_store import IdentityStore  # noqa: E402


def verdict(name: str, ok: bool, detail: str = "") -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' -> ' + detail) if detail else ''}")


def f1_emoji_intermediate_bypass() -> bool:
    """缺陷1：改名中间态是纯 emoji（归一化为空）时，改成受害者名字不会被检出。"""
    print("\n[F1] 纯 emoji 中间名绕过检测")
    store = IdentityStore(Path(tempfile.mkdtemp()) / "f1.json")
    store.load()
    t = time.time()
    # 正主 10001 用「张三」
    store.record_observation("g", "10001", "张三", now=t)
    # 攻击者 10002 改成纯 emoji（归一化后 norm=""）
    obs = store.record_observation("g", "10002", "🌟🌟🌟", now=t + 1)
    # 再改成「张三」——必须被检出
    obs = store.record_observation("g", "10002", "张三", now=t + 2)
    caught = obs.has_conflict and obs.conflicts[0].other_user_id == "10001"
    verdict("emoji 中间名后改成他人名字被检出", caught,
            f"name_changed={obs.name_changed}, conflicts={obs.conflicts}")
    return caught


def f2_tool_param_collision() -> bool:
    """缺陷2：@Tool 参数名 user_id 与 Host 注入的上下文 user_id（调用者）同名冲突。

    证明方式：inspect 真实 check_identity 签名，确认不再有名为 user_id 的参数。
    """
    print("\n[F2] Tool 参数与注入上下文撞名")
    import inspect
    import plugin as plugin_mod
    sig = inspect.signature(plugin_mod.IdentityGuardPlugin.check_identity)
    params = [p for p in sig.parameters if p not in ("self", "kwargs")]
    has_collision = "user_id" in params
    verdict("check_identity 的参数名不占用 user_id", not has_collision,
            f"参数列表={params}")
    return not has_collision


def f3_group_cooldown_unbounded() -> bool:
    """缺陷3：_group_until 字典只增不减（每群一个条目，永不清理）。"""
    print("\n[F3] 群级冷却字典无界增长")
    from plugin import IdentityGuardPlugin
    plugin = IdentityGuardPlugin()
    now = time.time()
    for i in range(500):
        plugin._group_until[f"g{i}"] = now - 999999  # 早已过期的条目
    plugin._prune_cooldown(now)
    clean = len(plugin._group_until) == 0
    verdict("过期群级冷却条目被清理", clean, f"剩余 {len(plugin._group_until)} 条")
    return clean


async def main() -> int:
    results = [f1_emoji_intermediate_bypass(), f2_tool_param_collision(),
               f3_group_cooldown_unbounded()]
    print(f"\n{'=' * 52}")
    print(f"缺陷复现：{sum(results)}/{len(results)} 项通过（修复前应低于全数）")
    print("=" * 52)
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
