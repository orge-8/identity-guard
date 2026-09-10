"""群聊身份档案库（纯逻辑，不依赖 maibot_sdk，可离线单测）。

核心思路
--------
QQ 号（user_id）是**不可变的身份锚点**，群名片只是可变的展示属性。
插件长期记录「谁在什么时候用过什么名片」，当某个账号的名片与**其他账号**
正在使用或曾经使用过的名片相同或高度相似时，产出 Conflict 供上层告警。

存储结构（JSON，落盘到 ctx.paths.data_dir/identity_store.json）::

    {
      "version": 1,
      "groups": {
        "<group_id>": {
          "members": {
            "<user_id>": {
              "current": "当前名片",
              "current_norm": "归一化后的当前名片",
              "first_seen": 1757400000.0,
              "last_seen": 1757400000.0,
              "names": [{"name": "...", "norm": "...",
                         "first_seen": 0.0, "last_seen": 0.0, "times": 1}],
              "trusted": false,
              "note": ""
            }
          },
          "events": [{"ts": 0.0, "user_id": "...", "old": "...", "new": "..."}]
        }
      }
    }
"""

import json
import os
import re
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

STORE_VERSION = 1

# 归一化：只保留中文 / 英文字母 / 数字，去掉空白、标点、emoji、装饰符
_RE_SPACE = re.compile(r"[\s\u3000]+")
_RE_KEEP = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")


def normalize_name(name: str) -> str:
    """归一化展示名：小写化 + 去空白 + 去标点/emoji，只保留中英文数字。

    目的：绕过「张三 」「张三✨」「Zhang-San」这类加空格/加符号的伪装。
    """
    if not name:
        return ""
    text = str(name).strip().lower()
    text = _RE_SPACE.sub("", text)
    return _RE_KEEP.sub("", text)


def similarity(a: str, b: str) -> float:
    """两个**已归一化**名字的相似度（0~1）。

    长度比低于 0.5 时直接判 0：既省掉 SequenceMatcher 的开销，
    也避免「小明」与「小明明明的超级长名字」被算成中等相似。
    """
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if min(len(a), len(b)) / max(len(a), len(b)) < 0.5:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def name_effective_check(norm: str) -> bool:
    """归一化后非空才算有效名字。"""
    return bool(norm)


class Conflict:
    """一次疑似冒充：某账号的名片与另一账号的名片撞了。"""

    __slots__ = ("other_user_id", "other_name", "ratio", "since", "active")

    def __init__(self, other_user_id: str, other_name: str, ratio: float,
                 since: float, active: bool) -> None:
        self.other_user_id = str(other_user_id)
        self.other_name = other_name
        self.ratio = float(ratio)
        self.since = float(since)
        self.active = bool(active)  # 对方当前是否仍在用这个名字

    @property
    def exact(self) -> bool:
        """完全一致（归一化后相等）。"""
        return self.ratio >= 0.999

    def __repr__(self) -> str:
        return (f"Conflict(other={self.other_user_id}, name={self.other_name!r}, "
                f"ratio={self.ratio:.3f}, active={self.active})")


class Observation:
    """一次入站观察的结果。"""

    __slots__ = ("is_new_member", "name_changed", "old_name", "new_name", "conflicts")

    def __init__(self, is_new_member: bool, name_changed: bool, old_name: str,
                 new_name: str, conflicts: list) -> None:
        self.is_new_member = is_new_member
        self.name_changed = name_changed
        self.old_name = old_name
        self.new_name = new_name
        self.conflicts = conflicts or []

    @property
    def has_conflict(self) -> bool:
        return bool(self.conflicts)


class IdentityStore:
    """身份档案库。所有方法都是同步的（数据只在内存中，落盘由调用方调度）。"""

    def __init__(
        self,
        path: "str | os.PathLike",  # 引号形式：避免 3.9 求值 `X | Y` 报错
        min_name_length: int = 2,
        similarity_threshold: float = 0.86,
        max_history_names: int = 20,
        max_events: int = 300,
        max_members: int = 2000,
    ) -> None:
        self._path = Path(path)
        self.min_name_length = max(1, int(min_name_length))
        self.similarity_threshold = float(similarity_threshold)
        self.max_history_names = max(1, int(max_history_names))
        self.max_events = max(1, int(max_events))
        self.max_members = max(1, int(max_members))
        self._data: dict = {"version": STORE_VERSION, "groups": {}}

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------
    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> None:
        """从磁盘加载；文件不存在/损坏时回退到空库（不让插件挂掉）。"""
        try:
            if not self._path.exists():
                self._data = {"version": STORE_VERSION, "groups": {}}
                return
            raw = self._path.read_text(encoding="utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict) or not isinstance(data.get("groups"), dict):
                raise ValueError("档案结构不合法")
            data.setdefault("version", STORE_VERSION)
            self._data = data
        except Exception:
            # 损坏时另存一份备份，便于人工排查，但不阻断插件启动
            try:
                if self._path.exists():
                    backup = self._path.with_suffix(self._path.suffix + ".corrupt")
                    self._path.replace(backup)
            except Exception:
                pass
            self._data = {"version": STORE_VERSION, "groups": {}}

    def save(self) -> bool:
        """原子写盘（临时文件 + os.replace），失败返回 False。"""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.write_text(
                json.dumps(self._data, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            os.replace(tmp, self._path)
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    @staticmethod
    def _group(data: dict, group_id: str) -> dict:
        groups = data.setdefault("groups", {})
        group = groups.get(group_id)
        if not isinstance(group, dict):
            group = {"members": {}, "events": []}
            groups[group_id] = group
        group.setdefault("members", {})
        group.setdefault("events", [])
        return group

    def _touch_name(self, member: dict, name: str, norm: str, now: float) -> None:
        """把当前名字写进该成员的名片历史。"""
        names = member.setdefault("names", [])
        for rec in names:
            if rec.get("norm") == norm:
                rec["last_seen"] = now
                rec["times"] = int(rec.get("times") or 0) + 1
                rec["name"] = name  # 保留最新写法（可能只是加了空格/符号）
                return
        names.append({
            "name": name,
            "norm": norm,
            "first_seen": now,
            "last_seen": now,
            "times": 1,
        })
        if len(names) > self.max_history_names:
            # 淘汰最久没用的历史名片（当前名片不受影响，它一定在 last_seen 最大之列）
            names.sort(key=lambda r: float(r.get("last_seen") or 0.0))
            del names[: len(names) - self.max_history_names]
            names.sort(key=lambda r: float(r.get("first_seen") or 0.0))

    # ------------------------------------------------------------------
    # 核心：记录一次观察
    # ------------------------------------------------------------------
    def record_observation(
        self,
        group_id: str,
        user_id: str,
        display_name: str,
        now: float | None = None,
        trusted_ids: Any = None,
    ) -> Observation:
        """记录「某群里某个 QQ 号此刻叫这个名字」，返回观察结果。

        trusted_ids：可信名单（管理端人工核实过的人）。
        可信成员**自己改名不告警**，但别人冒用可信成员的名字照样告警。
        """
        now = now if now is not None else time.time()
        gid, uid = str(group_id), str(user_id)
        name = str(display_name or "").strip()
        norm = normalize_name(name)

        group = self._group(self._data, gid)
        members = group["members"]
        member = members.get(uid)
        is_new = not isinstance(member, dict)
        if is_new:
            member = {
                "current": name if name_effective_check(norm) else "",
                "current_norm": norm if name_effective_check(norm) else "",
                "first_seen": now,
                "last_seen": now,
                "names": [],
                "trusted": False,
                "note": "",
            }
            members[uid] = member

        old_name = str(member.get("current") or "")
        old_norm = str(member.get("current_norm") or "")
        # 「有效名字」= 归一化后非空（纯 emoji/纯符号的名字不参与记账，
        # 否则出现检测盲区：用纯 emoji 中间态改名即可绕过撞名检测）
        name_effective = bool(norm)
        # 只有从「有效旧名」换成「有效新名」才算改名事件；
        # 旧状态为空（首次有效记账/纯符号名）不算改名
        name_changed = bool(old_norm) and name_effective and old_norm != norm

        member["last_seen"] = now
        if name_effective and (name_changed or not member.get("names")):
            member["current"] = name
            member["current_norm"] = norm
            self._touch_name(member, name, norm, now)

        if name_changed:
            events = group["events"]
            events.append({"ts": now, "user_id": uid, "old": old_name, "new": name})
            if len(events) > self.max_events:
                del events[: len(events) - self.max_events]

        conflicts: list[Conflict] = []
        # 首次有效记账（is_new 或 旧 norm 为空但这次有效）也要参与撞名检测——
        # 否则「纯 emoji 中间名 → 受害者名字」这条路径会绕过检测
        first_effective = name_effective and not old_norm
        if norm and len(norm) >= self.min_name_length and (
            (is_new and name_effective) or name_changed or first_effective
        ):
            trusted = {str(x) for x in (trusted_ids or [])}
            if uid not in trusted:
                conflicts = self._find_conflicts(members, uid, norm, self.similarity_threshold)

        self._prune(group)
        return Observation(is_new, name_changed, old_name, name, conflicts)

    def _find_conflicts(
        self, members: dict, uid: str, norm: str, threshold: float
    ) -> list[Conflict]:
        """在当前群里找与该名字撞车的**其他**成员。"""
        found: list[Conflict] = []
        for other_uid, other in members.items():
            if other_uid == uid or not isinstance(other, dict):
                continue
            best_ratio = 0.0
            best_name = ""
            best_since = 0.0
            best_active = False
            # 当前名片 + 历史名片都参与比较（冒充者可能挑对方三个月前用过的名字）
            for rec in list(other.get("names") or []):
                rec_norm = str(rec.get("norm") or "")
                if not rec_norm:
                    continue
                ratio = similarity(norm, rec_norm)
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_name = str(rec.get("name") or "")
                    best_since = float(rec.get("first_seen") or 0.0)
                    best_active = str(other.get("current_norm") or "") == rec_norm
            if best_ratio >= threshold:
                found.append(Conflict(other_uid, best_name, best_ratio, best_since, best_active))
        found.sort(key=lambda c: (c.ratio, c.since), reverse=True)
        return found[:3]

    def _prune(self, group: dict) -> None:
        """成员数超限时淘汰最久未发言且未标记的档案。"""
        members = group.get("members") or {}
        if len(members) <= self.max_members:
            return
        removable = [
            (uid, float(m.get("last_seen") or 0.0))
            for uid, m in members.items()
            if isinstance(m, dict) and not m.get("trusted") and not m.get("note")
        ]
        removable.sort(key=lambda item: item[1])
        for uid, _ in removable[: max(0, len(members) - self.max_members)]:
            members.pop(uid, None)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def get_profile(self, group_id: str, user_id: str) -> dict | None:
        """取某个成员在某群的档案（含名片历史），没有则 None。"""
        group = (self._data.get("groups") or {}).get(str(group_id))
        if not isinstance(group, dict):
            return None
        member = (group.get("members") or {}).get(str(user_id))
        if not isinstance(member, dict):
            return None
        names = sorted(
            [n for n in (member.get("names") or []) if isinstance(n, dict)],
            key=lambda r: float(r.get("first_seen") or 0.0),
        )
        return {
            "user_id": str(user_id),
            "current": str(member.get("current") or ""),
            "first_seen": float(member.get("first_seen") or 0.0),
            "last_seen": float(member.get("last_seen") or 0.0),
            "names": names,
            "trusted": bool(member.get("trusted")),
            "note": str(member.get("note") or ""),
        }

    def find_by_name(self, group_id: str, query: str, limit: int = 5) -> list[dict]:
        """按名字（当前或历史）模糊查找成员，返回 [{user_id, name, ratio, active}]。"""
        group = (self._data.get("groups") or {}).get(str(group_id))
        if not isinstance(group, dict):
            return []
        q = normalize_name(query)
        if not q:
            return []
        out: list[dict] = []
        for uid, member in (group.get("members") or {}).items():
            if not isinstance(member, dict):
                continue
            best_ratio, best_name, best_active = 0.0, "", False
            for rec in list(member.get("names") or []):
                rec_norm = str(rec.get("norm") or "")
                if not rec_norm:
                    continue
                ratio = similarity(q, rec_norm)
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_name = str(rec.get("name") or "")
                    best_active = str(member.get("current_norm") or "") == rec_norm
            if best_ratio >= 0.6:
                out.append({
                    "user_id": uid,
                    "name": best_name,
                    "ratio": best_ratio,
                    "active": best_active,
                })
        out.sort(key=lambda item: (item["ratio"], item["active"]), reverse=True)
        return out[: max(1, int(limit))]

    def recent_events(self, group_id: str, limit: int = 10) -> list[dict]:
        """最近的改名事件（新的在前）。"""
        group = (self._data.get("groups") or {}).get(str(group_id))
        if not isinstance(group, dict):
            return []
        events = [e for e in (group.get("events") or []) if isinstance(e, dict)]
        events = sorted(events, key=lambda e: float(e.get("ts") or 0.0), reverse=True)
        return events[: max(1, int(limit))]

    # ------------------------------------------------------------------
    # 管理
    # ------------------------------------------------------------------
    def set_trusted(self, group_id: str, user_id: str, trusted: bool = True) -> bool:
        """标记/取消信任（成员不存在时也会建空档案，方便提前登记）。"""
        group = self._group(self._data, str(group_id))
        members = group["members"]
        uid = str(user_id)
        member = members.get(uid)
        if not isinstance(member, dict):
            member = {
                "current": "", "current_norm": "", "first_seen": time.time(),
                "last_seen": time.time(), "names": [], "trusted": trusted, "note": "",
            }
            members[uid] = member
        member["trusted"] = bool(trusted)
        return True

    def set_note(self, group_id: str, user_id: str, note: str) -> bool:
        group = self._group(self._data, str(group_id))
        members = group["members"]
        uid = str(user_id)
        member = members.get(uid)
        if not isinstance(member, dict):
            member = {
                "current": "", "current_norm": "", "first_seen": time.time(),
                "last_seen": time.time(), "names": [], "trusted": False, "note": note,
            }
            members[uid] = member
        member["note"] = str(note or "")
        return True

    def is_trusted(self, group_id: str, user_id: str) -> bool:
        member = self.get_profile(group_id, user_id)
        return bool(member and member.get("trusted"))

    def stats(self) -> dict:
        """全局统计（跨群）。"""
        groups = self._data.get("groups") or {}
        members_total = 0
        events_total = 0
        trusted_total = 0
        noted_total = 0
        for group in groups.values():
            if not isinstance(group, dict):
                continue
            members = group.get("members") or {}
            members_total += len(members)
            events_total += len(group.get("events") or [])
            for m in members.values():
                if isinstance(m, dict):
                    trusted_total += 1 if m.get("trusted") else 0
                    noted_total += 1 if m.get("note") else 0
        return {
            "groups": len(groups),
            "members": members_total,
            "events": events_total,
            "trusted": trusted_total,
            "noted": noted_total,
        }
