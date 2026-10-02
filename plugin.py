"""identity-guard —— 群聊身份识别 / 防改名片冒充插件。

原理
----
QQ 号是改不了的，群名片随手就能改。插件长期记录「谁在什么时候用过什么名片」，
一旦某个账号改名（或新人进群）后与**其他账号**正在使用/曾用过的名片相同或高度相似，
就在群里发一条身份提醒，并提供命令与 LLM 工具供随时核验。

组件
----
- HookHandler `chat.receive.after_process`：旁路记录 + 检测，不拦截正常回复流程
- Tool `check_identity` / `recent_name_changes`：让 LLM 在对话中核验身份
- Command `/身份`、`/我是谁`、`/改名记录`、`/身份信任`、`/身份备注`、`/身份状态`
"""

import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

# Runner 用 spec_from_file_location 加载 plugin.py，插件目录不一定在 sys.path 里
_PLUGIN_DIR = str(Path(__file__).resolve().parent)
if _PLUGIN_DIR not in sys.path:
    sys.path.insert(0, _PLUGIN_DIR)

try:  # 包式加载（Runner）
    from .identity_store import IdentityStore, normalize_name
except ImportError:  # 平铺兜底（脚本直跑 / 离线测试）
    from identity_store import IdentityStore, normalize_name

from maibot_sdk import Command, Field, HookHandler, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

try:  # 老版本 SDK 可能没有这些枚举，回退成字符串字面量
    from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder

    _HOOK_MODE_OBSERVE = HookMode.OBSERVE
    _HOOK_ORDER_LATE = HookOrder.LATE
    _ERROR_POLICY_SKIP = ErrorPolicy.SKIP
except Exception:  # pragma: no cover
    _HOOK_MODE_OBSERVE = "observe"
    _HOOK_ORDER_LATE = "late"
    _ERROR_POLICY_SKIP = "skip"

_BOT_ID_RETRY_BACKOFF = 1800  # get_login_info 失败后的退避秒数
_SEND_TIMEOUT = 10

# ----------------------------------------------------------------------
# 载荷解析：不同版本 / 适配器字段名不一致，一律多路径兼容
# ----------------------------------------------------------------------
_GROUP_KEYS = ("group_id", "group", "gid", "chat_id")
_CARD_KEYS = ("user_cardname", "user_card", "cardname", "card", "group_card", "nickname_in_group")
_NICK_KEYS = ("user_nickname", "nickname", "user_name", "name", "user_displayname")
_UID_KEYS = ("user_id", "sender_id", "sender_user_id", "uid")
_RE_AT_QQ = re.compile(r"qq=(\d{5,12})")
_RE_QQ = re.compile(r"\d{5,12}")


def _first_text(*values: Any) -> str:
    """取第一个非空值并转成字符串。"""
    for val in values:
        if val not in (None, ""):
            text = str(val).strip()
            if text:
                return text
    return ""


def _as_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _message_of(message: Any, kwargs: dict) -> dict:
    """hook/命令的 message 参数可能藏在 kwargs 里。"""
    if isinstance(message, dict) and message:
        return message
    inner = kwargs.get("message")
    if isinstance(inner, dict):
        return inner
    return _as_dict(message)


def _user_node(msg: dict) -> dict:
    """从各种载荷形态里定位发送者信息节点。"""
    info = _as_dict(msg.get("message_info"))
    for key in ("user_info", "user", "sender"):
        node = info.get(key)
        if isinstance(node, dict) and node:
            return node
    for key in ("user_info", "user", "sender"):
        node = msg.get(key)
        if isinstance(node, dict) and node:
            return node
    return {}


def extract_group_id(msg: dict, kwargs: dict | None = None) -> str:
    """提取群号，取不到返回空串（私聊场景返回空）。"""
    kwargs = kwargs or {}
    info = _as_dict(msg.get("message_info"))
    ginfo = _as_dict(info.get("group_info"))
    gid = _first_text(ginfo.get("group_id"), ginfo.get("id"), info.get("group_id"))
    if gid:
        return gid
    for key in _GROUP_KEYS:
        gid = _first_text(msg.get(key), kwargs.get(key))
        if gid:
            return gid
    return ""


def extract_user_id(msg: dict, kwargs: dict | None = None) -> str:
    """提取发送者 QQ 号。"""
    kwargs = kwargs or {}
    node = _user_node(msg)
    uid = _first_text(
        node.get("user_id"), node.get("id"), node.get("uid"),
        *[msg.get(k) for k in _UID_KEYS],
        *[kwargs.get(k) for k in _UID_KEYS],
    )
    return uid


def extract_display_name(msg: dict) -> str:
    """提取当前展示名：群名片优先，回退昵称。"""
    node = _user_node(msg)
    for keys in (_CARD_KEYS, _NICK_KEYS):
        for key in keys:
            val = node.get(key)
            if val not in (None, ""):
                text = str(val).strip()
                if text:
                    return text
    for keys in (_CARD_KEYS, _NICK_KEYS):
        for key in keys:
            val = msg.get(key)
            if val not in (None, ""):
                text = str(val).strip()
                if text:
                    return text
    return ""


def extract_stream_id(msg: dict, kwargs: dict | None = None) -> str:
    """提取会话 stream_id（发消息用）。"""
    kwargs = kwargs or {}
    return _first_text(
        kwargs.get("stream_id"), msg.get("session_id"), msg.get("stream_id"),
        msg.get("chat_id"), kwargs.get("session_id"), kwargs.get("chat_id"),
    )


def format_time(ts: float) -> str:
    """MM-DD HH:MM。"""
    if not ts:
        return "未知"
    return time.strftime("%m-%d %H:%M", time.localtime(ts))


def format_span(ts: float) -> str:
    """相对时间：今天 09:12 / 昨天 21:03 / N 天前 / MM-DD HH:MM。"""
    if not ts:
        return "未知"
    delta = time.time() - ts
    if delta < 0:
        return format_time(ts)
    if delta < 86400 and time.localtime(ts).tm_yday == time.localtime().tm_yday:
        return "今天 " + time.strftime("%H:%M", time.localtime(ts))
    if delta < 172800:
        return "昨天 " + time.strftime("%H:%M", time.localtime(ts))
    days = int(delta // 86400)
    if days < 60:
        return f"{days} 天前"
    return format_time(ts)


# ----------------------------------------------------------------------
# 配置
# ----------------------------------------------------------------------
class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "shield"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(default="1.0.2", description="配置版本")


class GuardSectionConfig(PluginConfigBase):
    """身份识别配置。"""

    __ui_label__ = "身份识别"
    __ui_icon__ = "user-check"
    __ui_order__ = 1

    similarity_threshold: float = Field(
        default=0.86,
        description="疑似冒充的相似度阈值（0~1，归一化后比较）。越低越灵敏、越容易误报",
    )
    min_name_length: int = Field(
        default=2,
        description="参与比对的最短名字长度（归一化后），太短的名字（如单字）不参与告警",
    )
    alert_enabled: bool = Field(default=True, description="检测到疑似冒充时是否在群里发提醒")
    alert_on_rename: bool = Field(default=True, description="老成员改名撞名时提醒")
    alert_on_new_member: bool = Field(default=True, description="新成员使用他人名字时提醒")
    alert_cooldown_seconds: int = Field(
        default=600, description="同一人同一个名字的提醒冷却秒数"
    )
    group_alert_cooldown_seconds: int = Field(
        default=60, description="同一个群两次提醒之间的最小间隔秒数"
    )
    alert_template_rename: str = Field(
        default=(
            "⚠️【身份提醒】{name}（QQ {user_id}）刚把群名片从「{old_name}」改成「{new_name}」，"
            "与{other_desc}{match_desc}。改名片冒充他人是常见骗术，"
            "涉及转账、私聊借钱请务必先核实对方身份。"
        ),
        description="改名撞名的提醒模板。可用字段：name / user_id / old_name / new_name / other_desc / match_desc",
    )
    alert_template_newcomer: str = Field(
        default=(
            "⚠️【身份提醒】刚发言的 {name}（QQ {user_id}）与{other_desc}{match_desc}，"
            "请注意辨别身份，不要仅凭群名片判断对方是谁。"
        ),
        description="新成员撞名的提醒模板。可用字段：name / user_id / other_desc / match_desc",
    )
    trusted_user_ids: list[str] = Field(
        default_factory=list,
        description="可信 QQ 号列表（本人改名不再提醒；别人冒用他们的名字照常提醒）",
    )
    admin_ids: list[str] = Field(
        default_factory=list,
        description="允许使用管理命令（信任/备注/状态）的 QQ 号列表",
    )
    ignored_user_ids: list[str] = Field(
        default_factory=list,
        description="不建档、不告警的 QQ 号（如机器人小号、公告号）",
    )
    max_history_names: int = Field(default=20, description="每人保留的历史名片数量")
    max_events: int = Field(default=300, description="每群保留的改名事件条数")
    max_members: int = Field(default=2000, description="每群保留的成员档案上限")
    auto_save_interval_seconds: int = Field(default=60, description="档案自动落盘间隔秒数")


class IdentityGuardConfig(PluginConfigBase):
    """插件配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    guard: GuardSectionConfig = Field(default_factory=GuardSectionConfig)


# ----------------------------------------------------------------------
# 插件
# ----------------------------------------------------------------------
class IdentityGuardPlugin(MaiBotPlugin):
    """群聊身份识别插件。"""

    config_model = IdentityGuardConfig

    def get_webui_config_schema(self, **kwargs) -> dict:
        """覆写 SDK 的 WebUI 配置 Schema：做可视化模式的显示层补丁。

        Runner 调这个方法拿配置页 Schema 且异常会被吞掉（变成空 Schema、
        配置页整页空白），所以这里自己兜底：补丁失败就原样返回 SDK 输出。
        """

        schema = super().get_webui_config_schema(**kwargs)
        try:
            return _apply_webui_display_polish(schema)
        except Exception:  # noqa: BLE001 —— 显示补丁失败绝不能让配置页变空白
            logging.getLogger(__name__).exception("修正 WebUI 配置 Schema 失败，回退 SDK 原样输出")
            return schema

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """初始化内部状态。

        必须先调 super().__init__()：SDK 基类在那里初始化 `_dynamic_api_components`
        等属性，漏掉会让 Runner 注册组件时 AttributeError，整个插件 Runner 崩溃
        （真机实录 2026-09-10：插件注册失败 → Runner RPC 断开 → 插件系统整体启动失败）。
        状态放 __init__ 而不是 on_load：保证任何组件（尤其是入站 hook）即使
        在生命周期方法之前被触发，也不会因属性不存在而 AttributeError。
        """
        super().__init__(*args, **kwargs)
        self._store: IdentityStore | None = None
        self._tasks: set = set()
        self._dirty = False
        self._alert_until: dict = {}   # (group_id, user_id, norm) -> 冷却截止时间戳
        self._group_until: dict = {}   # group_id -> 冷却截止时间戳
        self._bot_user_ids: set = set()
        self._bot_id_retry_after = 0.0

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def on_load(self) -> None:
        """插件加载：建库、读盘、起自动落盘任务。"""
        self._store = IdentityStore(
            path=Path(self.ctx.paths.data_dir) / "identity_store.json",
            min_name_length=self.config.guard.min_name_length,
            similarity_threshold=self.config.guard.similarity_threshold,
            max_history_names=self.config.guard.max_history_names,
            max_events=self.config.guard.max_events,
            max_members=self.config.guard.max_members,
        )
        self._store.load()
        stats = self._store.stats()
        self.ctx.logger.info(
            "身份识别插件已加载：群 %d 个 / 档案 %d 份 / 改名事件 %d 条（库：%s）",
            stats["groups"], stats["members"], stats["events"], self._store.path,
        )
        self._spawn(self._auto_save_loop())
        self._spawn(self._ensure_bot_ids())

    async def on_unload(self) -> None:
        """插件卸载：取消后台任务并强制落盘。"""
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if self._store:
            self._store.save()
        self.ctx.logger.info("身份识别插件已卸载（已取消 %d 个后台任务）", len(tasks))

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        """配置热重载：同步阈值类参数，无需重建档案。"""
        del config_data, version
        if scope != "self" or not self._store:
            return
        self._store.min_name_length = self.config.guard.min_name_length
        self._store.similarity_threshold = self.config.guard.similarity_threshold
        self._store.max_history_names = self.config.guard.max_history_names
        self._store.max_events = self.config.guard.max_events
        self._store.max_members = self.config.guard.max_members
        self.ctx.logger.info("身份识别配置已更新（相似度阈值 %.2f）",
                             self.config.guard.similarity_threshold)

    # ------------------------------------------------------------------
    # 内部：任务 / 落盘
    # ------------------------------------------------------------------
    def _spawn(self, coro) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _auto_save_loop(self) -> None:
        """定期把脏数据落盘，避免每条消息都写文件。"""
        while True:
            try:
                await asyncio.sleep(max(5, int(self.config.guard.auto_save_interval_seconds)))
                if self._dirty and self._store:
                    await asyncio.to_thread(self._store.save)
                    self._dirty = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ctx.logger.error("身份档案自动落盘失败: %s", exc)

    def _mark_dirty(self, persist_now: bool = False) -> None:
        self._dirty = True
        if persist_now and self._store:
            self._store.save()
            self._dirty = False

    async def _ensure_bot_ids(self) -> None:
        """缓存 bot 自身 QQ 号，用于跳过自己的消息（失败退避，不刷屏）。"""
        if self._bot_user_ids or time.time() < self._bot_id_retry_after:
            return
        try:
            resp = await self.ctx.api.call("adapter.napcat.system.get_login_info")
        except Exception as exc:
            self._bot_id_retry_after = time.time() + _BOT_ID_RETRY_BACKOFF
            self.ctx.logger.info(
                "获取 bot 自身 QQ 号失败（%s），%d 秒内不再重试；"
                "如需跳过机器人自己的消息，请在 guard.ignored_user_ids 里手动填",
                exc.__class__.__name__, _BOT_ID_RETRY_BACKOFF // 60,
            )
            return
        data = resp if isinstance(resp, dict) else {}
        inner = data.get("data") if isinstance(data.get("data"), dict) else data
        uid = _first_text(inner.get("user_id"), inner.get("uin"), inner.get("uid"))
        if uid:
            self._bot_user_ids.add(uid)
            self.ctx.logger.info("已缓存 bot 自身 QQ 号=%s（其消息不参与身份建档）", uid)
        else:
            self._bot_id_retry_after = time.time() + _BOT_ID_RETRY_BACKOFF

    # ------------------------------------------------------------------
    # 内部：判定 / 鉴权
    # ------------------------------------------------------------------
    @staticmethod
    def _norm_ids(values: Any) -> set:
        """管理员/忽略名单兼容 ["123"] 与 ["qq:123"] 两种写法。"""
        out = set()
        for item in values or []:
            text = str(item).strip()
            if not text:
                continue
            out.add(text.split(":")[-1].strip().lower())
        return out

    def _is_admin(self, kwargs: dict) -> bool:
        """本地控制台天然放行；否则触发者须在 admin_ids 里。"""
        if bool(kwargs.get("is_local_operator")):
            return True
        admins = self._norm_ids(self.config.guard.admin_ids)
        if not admins:
            return False
        msg = _message_of(kwargs.get("message"), kwargs)
        uid = _first_text(kwargs.get("user_id"), extract_user_id(msg, kwargs)).lower()
        return bool(uid) and uid in admins

    def _is_ignored(self, user_id: str) -> bool:
        uid = str(user_id or "").strip().lower()
        if not uid:
            return True
        return uid in self._norm_ids(self.config.guard.ignored_user_ids) or uid in self._bot_user_ids

    def _alert_allowed(self, group_id: str, user_id: str, norm: str) -> bool:
        """冷却判定：同一人同一名字不重复刷，同一群两次提醒有最小间隔。"""
        now = time.time()
        key = (group_id, user_id, norm)
        if now < self._alert_until.get(key, 0.0):
            return False
        if now < self._group_until.get(group_id, 0.0):
            return False
        self._alert_until[key] = now + max(0, self.config.guard.alert_cooldown_seconds)
        self._group_until[group_id] = now + max(0, self.config.guard.group_alert_cooldown_seconds)
        self._prune_cooldown(now)
        return True

    def _prune_cooldown(self, now: float) -> None:
        for key in [k for k, v in self._alert_until.items() if v < now - 86400]:
            self._alert_until.pop(key, None)
        for gid in [g for g, v in self._group_until.items() if v < now - 86400]:
            self._group_until.pop(gid, None)

    # ------------------------------------------------------------------
    # 内部：文本构造
    # ------------------------------------------------------------------
    def _other_desc(self, conflict) -> str:
        """描述被撞的那一方：「张三（QQ 456，08-15 起使用）」。"""
        return f"「{conflict.other_name}」（QQ {conflict.other_user_id}，{format_time(conflict.since)} 起使用）"

    @staticmethod
    def _match_desc(conflict) -> str:
        if conflict.exact:
            return "完全一致"
        return f"相似度 {conflict.ratio * 100:.0f}%"

    def _build_alert(
        self, group_id: str, user_id: str, name: str, old_name: str,
        conflict, is_new: bool,
    ) -> str:
        """按场景套用提醒模板（模板坏了自动回退默认文案）。"""
        fields = {
            "name": name,
            "user_id": user_id,
            "old_name": old_name or "（无）",
            "new_name": name,
            "other_desc": self._other_desc(conflict),
            "match_desc": self._match_desc(conflict),
        }
        template = (
            self.config.guard.alert_template_newcomer if is_new
            else self.config.guard.alert_template_rename
        )
        try:
            return str(template).format(**fields)
        except Exception as exc:
            self.ctx.logger.warning("提醒模板渲染失败（%s），回退默认文案", exc)
            return (
                f"⚠️【身份提醒】{name}（QQ {user_id}）与{self._other_desc(conflict)}"
                f"{self._match_desc(conflict)}，请注意辨别身份。"
            )

    def _format_profile(self, profile: dict) -> str:
        """把档案渲染成可读文本。"""
        lines = [f"【身份档案】QQ {profile['user_id']}"]
        lines.append(f"· 当前名片：{profile['current'] or '（未记录）'}")
        lines.append(f"· 首次发言：{format_time(profile['first_seen'])}（{format_span(profile['first_seen'])}）")
        lines.append(f"· 最近发言：{format_span(profile['last_seen'])}")
        names = profile.get("names") or []
        if names:
            lines.append(f"· 名片历史（{len(names)} 个）：")
            for idx, rec in enumerate(names[:10], 1):
                span = format_time(rec.get("first_seen") or 0)
                times = int(rec.get("times") or 0)
                active = str(rec.get("norm")) == normalize_name(profile['current'])
                tail = " · 使用中" if active else ""
                lines.append(f"  {idx}. {rec.get('name')} · {span} 起 · {times} 次{tail}")
            if len(names) > 10:
                lines.append(f"  ……另有 {len(names) - 10} 个历史名片")
        lines.append(f"· 已信任：{'是' if profile.get('trusted') else '否'}")
        if profile.get("note"):
            lines.append(f"· 备注：{profile['note']}")
        return "\n".join(lines)

    def _format_events(self, group_id: str, limit: int) -> str:
        events = self._store.recent_events(group_id, limit) if self._store else []
        if not events:
            return "本群还没有记录到改名事件。"
        lines = [f"【最近改名记录】（{len(events)} 条）"]
        for evt in events:
            lines.append(
                f"· {format_time(evt.get('ts') or 0)}  QQ {evt.get('user_id')}  "
                f"「{evt.get('old') or '（无）'}」→「{evt.get('new') or '（无）'}」"
            )
        return "\n".join(lines)

    def _format_status(self) -> str:
        stats = self._store.stats() if self._store else {}
        cfg = self.config.guard
        lines = [
            "【身份识别状态】",
            f"· 群 {stats.get('groups', 0)} 个 · 成员档案 {stats.get('members', 0)} 份 · "
            f"改名事件 {stats.get('events', 0)} 条",
            f"· 已信任 {stats.get('trusted', 0)} 人 · 带备注 {stats.get('noted', 0)} 人",
            f"· 相似度阈值 {cfg.similarity_threshold:.2f} · 最短比对长度 {cfg.min_name_length}",
            f"· 群内提醒：{'开启' if cfg.alert_enabled else '关闭'}"
            f"（改名 {'开' if cfg.alert_on_rename else '关'} / 新人 {'开' if cfg.alert_on_new_member else '关'}）",
            f"· 提醒冷却：单人 {cfg.alert_cooldown_seconds}s · 群级 {cfg.group_alert_cooldown_seconds}s",
        ]
        if self._store:
            lines.append(f"· 档案文件：{self._store.path}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 内部：目标解析
    # ------------------------------------------------------------------
    @staticmethod
    def _cmd_arg(matched_groups: Any, kwargs: dict) -> str:
        """取命令参数：优先正则捕获组，其次从原始消息里剥掉命令本身。"""
        mg = matched_groups if isinstance(matched_groups, dict) else {}
        arg = str(mg.get("arg") or "").strip()
        if arg:
            return arg
        raw = str(kwargs.get("raw_message") or kwargs.get("processed_plain_text") or "").strip()
        match = re.match(r"^\s*[/／]?\s*\S+\s*(.*)$", raw, re.S)
        return (match.group(1) if match else "").strip()

    def _resolve_target(self, group_id: str, arg: str, kwargs: dict) -> tuple[str, str]:
        """把命令参数解析成 QQ 号。返回 (user_id, 提示)。

        支持：@某人（CQ 码里的 qq=）、纯数字 QQ、按名字模糊查找、留空查自己。
        """
        msg = _message_of(kwargs.get("message"), kwargs)
        raw = str(kwargs.get("raw_message") or "")
        at_match = _RE_AT_QQ.search(raw) or _RE_AT_QQ.search(arg)
        if at_match:
            return at_match.group(1), ""

        arg = (arg or "").strip()
        if not arg:
            uid = _first_text(kwargs.get("user_id"), extract_user_id(msg, kwargs))
            return uid, "" if uid else "没拿到你的 QQ 号，请直接 @ 对方或给出 QQ 号"

        if arg.isdigit() and 5 <= len(arg) <= 12:
            return arg, ""

        hits = self._store.find_by_name(group_id, arg, limit=3) if self._store else []
        if not hits:
            return "", f"在本群找不到名字像「{arg}」的人，试试直接给 QQ 号"
        if len(hits) > 1 and hits[0]["ratio"] - hits[1]["ratio"] < 0.15:
            options = "、".join(f"{h['name']}(QQ {h['user_id']})" for h in hits[:3])
            return "", f"「{arg}」匹配到多个人：{options}，请用 QQ 号精确查询"
        return hits[0]["user_id"], ""

    # ------------------------------------------------------------------
    # 内部：发送
    # ------------------------------------------------------------------
    async def _send(self, text: str, stream_id: str, group_id: str = "") -> bool:
        """发文本；没有 stream_id 时尝试按群号反查会话。"""
        if not stream_id and group_id:
            stream_id = await self._stream_by_group(group_id)
        if not stream_id:
            self.ctx.logger.warning("拿不到 stream_id，消息未发出：%s", text.replace("\n", " ")[:80])
            return False
        try:
            result = await asyncio.wait_for(
                self.ctx.send.text(text, stream_id), timeout=_SEND_TIMEOUT
            )
            return bool(result) if not isinstance(result, dict) else bool(result.get("success", True))
        except Exception as exc:
            self.ctx.logger.error("发送消息失败: %s", exc)
            return False

    async def _stream_by_group(self, group_id: str) -> str:
        """按群号反查 stream_id（兜底路径，失败返回空串）。"""
        try:
            resp = await self.ctx.chat.get_stream_by_group_id(group_id)
        except Exception as exc:
            self.ctx.logger.debug("按群号反查会话失败: %s", exc)
            return ""
        data = resp if isinstance(resp, dict) else {}
        inner = data.get("data") if isinstance(data.get("data"), dict) else data
        return _first_text(
            inner.get("stream_id"), inner.get("session_id"), inner.get("chat_id"), inner.get("id")
        )

    async def _reply(self, text: str, kwargs: dict) -> bool:
        """命令回复：从载荷里找 stream_id 并发送。"""
        msg = _message_of(kwargs.get("message"), kwargs)
        stream_id = extract_stream_id(msg, kwargs)
        group_id = extract_group_id(msg, kwargs)
        return await self._send(text, stream_id, group_id)

    # ------------------------------------------------------------------
    # Hook：旁路记录 + 撞名检测
    # ------------------------------------------------------------------
    @HookHandler(
        "chat.receive.after_process",
        name="identity_guard_observer",
        description="记录群成员名片历史，检测改名/新人冒充",
        mode=_HOOK_MODE_OBSERVE,
        order=_HOOK_ORDER_LATE,
        error_policy=_ERROR_POLICY_SKIP,
    )
    async def observe_message(self, message: Any = None, **kwargs: Any) -> dict:
        """观察每条入站群消息：建档 → 记录改名 → 撞名则提醒。"""
        if not self.config.plugin.enabled or not self._store:
            return {"action": "continue"}

        msg = _message_of(message, kwargs)
        if not msg:
            return {"action": "continue"}

        group_id = extract_group_id(msg, kwargs)
        user_id = extract_user_id(msg, kwargs)
        if not group_id or not user_id or self._is_ignored(user_id):
            return {"action": "continue"}

        name = extract_display_name(msg)
        if not name:
            return {"action": "continue"}

        obs = self._store.record_observation(
            group_id, user_id, name, trusted_ids=self.config.guard.trusted_user_ids
        )
        self._mark_dirty(persist_now=obs.name_changed)
        if not obs.has_conflict:
            return {"action": "continue"}

        if not self._should_alert(obs, group_id, user_id, name):
            return {"action": "continue"}

        conflict = obs.conflicts[0]
        text = self._build_alert(group_id, user_id, name, obs.old_name, conflict, obs.is_new_member)
        stream_id = extract_stream_id(msg, kwargs)
        self.ctx.logger.info(
            "疑似冒充：群 %s 用户 %s 改名 %r → %r，撞车 %s（相似度 %.2f）",
            group_id, user_id, obs.old_name, name, conflict.other_user_id, conflict.ratio,
        )
        self._spawn(self._send(text, stream_id, group_id))
        return {"action": "continue"}

    def _should_alert(self, obs, group_id: str, user_id: str, name: str) -> bool:
        """是否值得发提醒：可信名单 + 开关 + 场景 + 冷却。

        「首次有效记账」（成员存在但尚无有效名片，如之前只发过纯符号名）
        与新成员同权：都是「这个名字第一次出现在群里」，撞名就该提醒。
        """
        cfg = self.config.guard
        if self._store and self._store.is_trusted(group_id, user_id):
            return False  # 可信成员本人：撞名只是「换回别人的名字」，不提醒
        if not cfg.alert_enabled:
            return False
        if obs.is_new_member and not cfg.alert_on_new_member:
            return False
        if obs.name_changed and not cfg.alert_on_rename:
            return False
        return self._alert_allowed(group_id, user_id, normalize_name(name))

    # ------------------------------------------------------------------
    # Tool：给 LLM 用
    # ------------------------------------------------------------------
    @Tool(
        "check_identity",
        brief_description="核验群聊里某个人是不是他自称的那个人（查 QQ 号对应的群名片历史）",
        detailed_description=(
            "用途：群名片可以随便改，QQ 号改不了。当有人在群里自称是某个人、"
            "或者你怀疑某个人换了名片冒充别人时，用本工具核验身份。\n\n"
            "返回内容：该 QQ 号当前名片、首次发言时间、历史名片列表（含起用时间与发言次数）、"
            "是否已被标记为可信、管理备注。\n\n"
            "使用场景：\n"
            "- 有人说「我是群主 / 我是 XXX」需要确认\n"
            "- 有两个人名字一样或很像，分不清谁是谁\n"
            "- 涉及转账、私聊借钱、要密码等敏感请求，先核验对方身份\n\n"
            "注意：只记录本插件上线之后的数据，之前的历史无从查证；"
            "查不到档案不代表对方有问题，只是还没在本群发过言。\n\n"
            "参数说明：\n"
            "- target_user_id：string，可选。要查的 QQ 号；不填则查当前说话的人\n"
            "- display_name：string，可选。按名字查（拿不到 QQ 号时用），会模糊匹配"
        ),
        parameters=[
            ToolParameterInfo(
                name="target_user_id",
                param_type=ToolParamType.STRING,
                description="要核验的 QQ 号（不填则查当前说话的人）",
                required=False,
            ),
            ToolParameterInfo(
                name="display_name",
                param_type=ToolParamType.STRING,
                description="按群名片/昵称查询（拿不到 QQ 号时使用，模糊匹配）",
                required=False,
            ),
        ],
    )
    async def check_identity(self, target_user_id: str = "", display_name: str = "", **kwargs: Any) -> dict:
        """LLM 调用的核验入口。参数名用 target_user_id：避免与 Host 注入的
        调用者上下文 user_id 同名冲突（调用者 user_id 优先取 kwargs 顶层的 user_id）。"""
        if not self.config.plugin.enabled:
            return {"success": False, "content": "身份识别插件未启用"}
        if not self._store:
            return {"success": False, "content": "身份档案库尚未就绪"}

        msg = _message_of(kwargs.get("message"), kwargs)
        group_id = extract_group_id(msg, kwargs)
        if not group_id:
            return {"success": False, "content": "身份核验仅支持群聊"}

        at_match = _RE_AT_QQ.search(str(display_name or ""))
        # 目标 QQ 只认 LLM 显式传的 target_user_id / @提及；调用者身份从
        # Host 注入的顶层 user_id 取（传入 params 时 Host 会放进 kwargs）
        injected_uid = _first_text(kwargs.get("caller_user_id"))
        target = _first_text(target_user_id, at_match.group(1) if at_match else "")
        note = ""
        if not target:
            target, note = self._resolve_target(group_id, str(display_name or ""), kwargs)
        if not target:
            return {"success": False, "content": note or "没拿到要核验的 QQ 号"}

        profile = self._store.get_profile(group_id, target)
        if not profile:
            return {
                "success": True,
                "content": (
                    f"本群还没有 QQ {target} 的发言记录，无法核验身份。"
                    "（只有在本插件启用后发过言的人才会建档，查不到不等于有问题）"
                ),
            }
        return {"success": True, "content": self._format_profile(profile)}

    @Tool(
        "recent_name_changes",
        brief_description="查看本群最近有哪些人改过群名片（排查冒充用）",
        detailed_description=(
            "用途：列出本群最近发生的群名片变更（谁、什么时候、从什么改成什么）。\n\n"
            "使用场景：\n"
            "- 群里出现两个相似的名字，想确认是谁刚改的\n"
            "- 有人质疑「这不是我」，需要看改名时间线\n\n"
            "注意：只记录本插件上线之后的变更。\n\n"
            "参数说明：\n"
            "- limit：integer，可选。返回条数，默认 5，最大 20"
        ),
        parameters=[
            ToolParameterInfo(
                name="limit",
                param_type=ToolParamType.INTEGER,
                description="返回条数（1~20，默认 5）",
                required=False,
            ),
        ],
    )
    async def recent_name_changes(self, limit: int = 5, **kwargs: Any) -> dict:
        """LLM 调用的改名记录入口。"""
        if not self._store:
            return {"success": False, "content": "身份档案库尚未就绪"}
        msg = _message_of(kwargs.get("message"), kwargs)
        group_id = extract_group_id(msg, kwargs)
        if not group_id:
            return {"success": False, "content": "改名记录仅支持群聊"}
        try:
            count = max(1, min(20, int(limit or 5)))
        except (TypeError, ValueError):
            count = 5
        return {"success": True, "content": self._format_events(group_id, count)}

    # ------------------------------------------------------------------
    # Command：查档案
    # ------------------------------------------------------------------
    @Command(
        "identity",
        description="查询群成员身份档案（QQ 号 + 名片历史），识别改名片冒充",
        pattern=r"^\s*[/／]\s*(?:身份|身份查询|identity)(?:\s+(?P<arg>\S+))?\s*$",
        aliases=["身份查询"],
    )
    async def cmd_identity(self, matched_groups: Any = None, **kwargs: Any) -> tuple:
        """`/身份 <QQ号|名字|@某人>`，留空查自己。"""
        if not self._store:
            return False, "档案库未就绪", 0
        msg = _message_of(kwargs.get("message"), kwargs)
        group_id = extract_group_id(msg, kwargs)
        if not group_id:
            text = "身份查询仅支持群聊。"
            return True, text, 2 if await self._reply(text, kwargs) else 0

        arg = self._cmd_arg(matched_groups, kwargs)
        target, note = self._resolve_target(group_id, arg, kwargs)
        if not target:
            return True, note, 2 if await self._reply(note, kwargs) else 0

        profile = self._store.get_profile(group_id, target)
        text = self._format_profile(profile) if profile else (
            f"本群还没有 QQ {target} 的记录。对方在本群发言后会自动建档。"
        )
        return True, text, 2 if await self._reply(text, kwargs) else 0

    @Command(
        "whoami",
        description="查看自己的身份档案（首次发言时间、历史群名片）",
        pattern=r"^\s*[/／]\s*(?:我是谁|我的身份)\s*$",
        aliases=["我的身份"],
    )
    async def cmd_whoami(self, **kwargs: Any) -> tuple:
        """`/我是谁`。"""
        if not self._store:
            return False, "档案库未就绪", 0
        msg = _message_of(kwargs.get("message"), kwargs)
        group_id = extract_group_id(msg, kwargs)
        if not group_id:
            text = "身份查询仅支持群聊。"
            return True, text, 2 if await self._reply(text, kwargs) else 0

        uid = _first_text(kwargs.get("user_id"), extract_user_id(msg, kwargs))
        if not uid:
            text = "没拿到你的 QQ 号，请改用 /身份 <QQ号>。"
            return True, text, 2 if await self._reply(text, kwargs) else 0

        profile = self._store.get_profile(group_id, uid)
        text = self._format_profile(profile) if profile else (
            f"本群还没有 QQ {uid} 的记录，说句话就能建档了。"
        )
        return True, text, 2 if await self._reply(text, kwargs) else 0

    @Command(
        "renamelog",
        description="查看本群最近的群名片变更记录",
        pattern=r"^\s*[/／]\s*(?:改名记录|身份记录)(?:\s+(?P<arg>\d+))?\s*$",
        aliases=["身份记录"],
    )
    async def cmd_renamelog(self, matched_groups: Any = None, **kwargs: Any) -> tuple:
        """`/改名记录 [条数]`，默认 10 条。"""
        if not self._store:
            return False, "档案库未就绪", 0
        msg = _message_of(kwargs.get("message"), kwargs)
        group_id = extract_group_id(msg, kwargs)
        if not group_id:
            text = "改名记录仅支持群聊。"
            return True, text, 2 if await self._reply(text, kwargs) else 0

        arg = self._cmd_arg(matched_groups, kwargs)
        try:
            limit = max(1, min(30, int(arg))) if arg else 10
        except ValueError:
            limit = 10
        text = self._format_events(group_id, limit)
        return True, text, 2 if await self._reply(text, kwargs) else 0

    # ------------------------------------------------------------------
    # Command：管理（admin_ids 鉴权）
    # ------------------------------------------------------------------
    @Command(
        "identity_trust",
        description="把某人标记为可信（其本人改名不再提醒）",
        pattern=r"^\s*[/／]\s*身份(?:信任|取消信任)\s+(?P<arg>\S+)(?:\s+(?P<group>\d+))?\s*$",
        aliases=["身份取消信任"],
    )
    async def cmd_trust(self, matched_groups: Any = None, **kwargs: Any) -> tuple:
        """`/身份信任 <QQ号> [群号]` / `/身份取消信任 <QQ号> [群号]`。

        群号可省略（群内执行时自动取当前群）；本地控制台没有群上下文，必须显式给群号。
        """
        if not self._is_admin(kwargs):
            text = "没有权限：只有管理员能修改可信名单。"
            return True, text, 2 if await self._reply(text, kwargs) else 0
        if not self._store:
            return False, "档案库未就绪", 0

        msg = _message_of(kwargs.get("message"), kwargs)
        mg = matched_groups if isinstance(matched_groups, dict) else {}
        group_id = extract_group_id(msg, kwargs) or str(mg.get("group") or "").strip()
        if not group_id:
            text = "缺少群上下文：请在群内执行，或用 /身份信任 <QQ号> <群号>。"
            return True, text, 2 if await self._reply(text, kwargs) else 0

        arg = self._cmd_arg(matched_groups, kwargs)
        raw = str(kwargs.get("raw_message") or "")
        untrust = "取消信任" in raw
        target, note = self._resolve_target(group_id, arg, kwargs)
        if not target:
            return True, note, 2 if await self._reply(note, kwargs) else 0

        self._store.set_trusted(group_id, target, not untrust)
        self._mark_dirty(persist_now=True)
        text = f"已将 QQ {target} {'加入' if not untrust else '移出'}可信名单。"
        return True, text, 2 if await self._reply(text, kwargs) else 0

    @Command(
        "identity_note",
        description="给某人写身份备注（如：群主，已线下核实）",
        pattern=r"^\s*[/／]\s*身份备注\s+(?P<arg>\S+)\s*(?P<rest>.*)$",
    )
    async def cmd_note(self, matched_groups: Any = None, **kwargs: Any) -> tuple:
        """`/身份备注 <QQ号> <备注内容>`；备注留空表示清除。"""
        if not self._is_admin(kwargs):
            text = "没有权限：只有管理员能写身份备注。"
            return True, text, 2 if await self._reply(text, kwargs) else 0
        if not self._store:
            return False, "档案库未就绪", 0

        msg = _message_of(kwargs.get("message"), kwargs)
        group_id = extract_group_id(msg, kwargs)
        if not group_id:
            text = "缺少群上下文：身份备注需要在群内执行。"
            return True, text, 2 if await self._reply(text, kwargs) else 0

        mg = matched_groups if isinstance(matched_groups, dict) else {}
        target_id = str(mg.get("arg") or "").strip()
        note_text = str(mg.get("rest") or "").strip()
        if not target_id:
            text = "用法：/身份备注 <QQ号> <备注内容>"
            return True, text, 2 if await self._reply(text, kwargs) else 0
        if not (target_id.isdigit() and 5 <= len(target_id) <= 12):
            text = "请给出正确的 QQ 号：/身份备注 <QQ号> <备注内容>"
            return True, text, 2 if await self._reply(text, kwargs) else 0

        self._store.set_note(group_id, target_id, note_text)
        self._mark_dirty(persist_now=True)
        text = f"已{'写入' if note_text else '清除'} QQ {target_id} 的备注" + (
            f"：{note_text}" if note_text else "。"
        )
        return True, text, 2 if await self._reply(text, kwargs) else 0

    @Command(
        "identity_status",
        description="查看身份识别插件的运行状态与档案统计",
        pattern=r"^\s*[/／]\s*(?:身份状态|身份统计)\s*$",
        aliases=["身份统计"],
    )
    async def cmd_status(self, **kwargs: Any) -> tuple:
        """`/身份状态`。"""
        text = self._format_status()
        return True, text, 2 if await self._reply(text, kwargs) else 0


# ================================================================ WebUI 显示层补丁
#
# 可视化模式的 FieldRenderer（dashboard/src/routes/plugin-config.tsx:163-361，
# 1.3.1 布局）按 ui_type 渲染控件时只输出 label / hint / placeholder，
# **从不渲染 description**；本插件字段的填法说明都写在 description 里，
# 不搬进 hint，用户在配置页上一个字都看不到（源代码模式才见得到）。
# 这里只改展示元数据，不碰任何配置键与校验语义。

#: 默认收起的 section：标题自带「可选 / 默认关闭」的功能节。收起后标题
#: 与说明仍可见，点开即可配置，避免整页卡片全开淹没常用配置。
_WEBUI_COLLAPSED_SECTIONS: frozenset = frozenset({})

#: 手工指定的 section 标题（键 = section 名）；仅在 SDK 输出标题等于
#: 节名（未配置 __ui_label__）时采用。
_WEBUI_SECTION_TITLES: dict = {}

#: 手工指定的字段 label（键 = 字段名）；仅在自动推导不可用时采用。
_WEBUI_LABEL_OVERRIDES: dict = {}


#: 推导 label 用的中文分隔符（取最靠左的一个）
_WEBUI_CJK_SEPS = "。！？；，：（(、"


def _webui_label_from_description(description: str) -> str:
    """从中文 description 里取第一小句当显示标题（取不到返回空串）。

    在中英文标点里找**最靠左**的分隔符，取它前面的短语——通常是字段
    本身的名字；超长时截到 12 字符并避免把英文单词截一半。
    """
    text = (description or "").strip()
    if not text:
        return ""
    cut = len(text)
    for sep in _WEBUI_CJK_SEPS:
        idx = text.find(sep)
        if 0 < idx < cut:
            cut = idx
    text = text[:cut].strip()
    if len(text) > 12:
        text = text[:12]
        if " " in text[4:]:
            text = text[: text.rfind(" ")].rstrip() or text
    while text and text[-1] in "（(\"“：:，,；;、":
        text = text[:-1].rstrip()
    return text if len(text) >= 2 else ""


def _apply_webui_display_polish(schema: dict) -> dict:
    """把 description 抄进 hint、补中文 label / 节标题、收起可选功能节。"""
    if not isinstance(schema, dict):
        return schema
    sections = schema.get("sections")
    if not isinstance(sections, dict):
        return schema
    for name, section in sections.items():
        if not isinstance(section, dict):
            continue
        if name in _WEBUI_COLLAPSED_SECTIONS:
            section["collapsed"] = True
        title = section.get("title")
        if not title or title == name:
            new_title = _WEBUI_SECTION_TITLES.get(name) or _webui_label_from_description(
                section.get("description") or ""
            )
            if new_title and new_title != name:
                section["title"] = new_title
        for fname, field in (section.get("fields") or {}).items():
            if not isinstance(field, dict):
                continue
            if fname == "config_version":
                field["hidden"] = True  # 插件自维护字段：可视化模式不渲染，源代码模式仍可见
            if not field.get("hint") and field.get("description"):
                field["hint"] = field["description"]
            label = field.get("label")
            if (not label or label == fname) and field.get("description"):
                new_label = _WEBUI_LABEL_OVERRIDES.get(fname) or _webui_label_from_description(
                    field["description"]
                )
                if new_label and new_label != fname:
                    field["label"] = new_label
    return schema


def create_plugin() -> IdentityGuardPlugin:
    """创建插件实例。"""
    return IdentityGuardPlugin()
