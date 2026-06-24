"""
Quality Guardian (v2) — staged, profile-first, human-in-the-loop DQ agent.

A second implementation of the Quality Guardian, built alongside the original
Evaluator-pattern agent (agents/quality_guardian/agent.py is left untouched). It
handles ANY table without binding rules to column names, by inverting the order:
profile the data first, let the model classify from evidence, ask the user what to
focus on, then run bounded checks the user (or model) requested.

Flow:
  A. BASIC   (exact, full table)  — volume + completeness + uniqueness scorecard
  B. PROFILE (sample, min(1%,100k)) — per-column stats + regex fingerprints
  C. RENDER schema + profile, ASK  — "any specific columns / rules to scan?"
  D. ACT on the user's instruction — LLM → bounded check IR → validate → run → score

Boundaries live in tools/quality/check_ir.py: schema-grounded, allowlisted check
types, read-only, caps, and a confirm gate before a full-table scan. The LLM only
authors intent; nothing runs until validate_request() passes.

Entry points (both via run(), dispatched on kwargs):
  run(goal, asset="db.tbl")                       → stages A+B+C (profile + ask)
  run(goal, asset="db.tbl", user_action="...")    → stage D (bounded act)
"""

import asyncio
import json
import logging
import time
from typing import AsyncGenerator, Optional

from agents.base_agent import BaseAgent
from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
from tools.iceberg.iceberg_tools import describe_iceberg_table
from tools.quality import profiler
from tools.quality import check_ir
from tools.quality import scan_state

logger = logging.getLogger(__name__)

_META_CACHE: dict[str, tuple[float, dict]] = {}
_META_TTL = 600.0


class QualityGuardianV2Agent(BaseAgent):
    """Profile-first, bounded, human-in-the-loop quality agent."""

    def __init__(self):
        super().__init__(
            agent_id="quality_guardian",   # shares the audit trail with the original
            description="Profile-first DQ: basic checks → sample profile → ask → bounded act",
        )

    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        asset = kwargs.get("asset") or kwargs.get("table_name")
        if not asset:
            yield self.emit("error", message="No asset/table_name provided")
            return

        user_action = kwargs.get("user_action")
        if user_action:
            async for ev in self._act(asset, user_action, kwargs.get("profile"), kwargs.get("confirm", False)):
                yield ev
        else:
            async for ev in self._profile(asset, goal, force=kwargs.get("force", False)):
                yield ev

    # ── Stages A + B + C ──────────────────────────────────────────────────────

    async def _profile(self, asset: str, goal: str, force: bool = False) -> AsyncGenerator[dict, None]:
        yield self.emit("started", asset=asset, goal=goal, mode="profile")

        # Resolve metadata (re-fetched server-side; the boundary never trusts the client).
        # describe already returns the snapshot history, so the data version is free.
        yield self.emit("step", name="resolve_schema", status="running")
        meta = await self._describe_meta(asset)
        fields = (meta or {}).get("fields", [])
        if not fields:
            yield self.emit("error", message=f"Could not resolve schema for {asset}")
            return
        version = self._data_version(meta)
        schema = {f["name"]: f.get("type", "string") for f in fields}
        yield self.emit("schema", asset=asset, columns=[{"name": n, "type": t} for n, t in schema.items()])
        yield self.emit("step", name="resolve_schema", status="complete")

        # ── Freshness gate — skip the scan if the Iceberg snapshot is unchanged ──────
        if not force and scan_state.is_unchanged(asset, version):
            last = scan_state.get_last(asset) or {}
            b, p = last.get("basic") or {}, last.get("profile") or {}
            yield self.emit("skipped_unchanged", asset=asset, version=version,
                            last_scanned=last.get("scanned_at"),
                            message="No new Iceberg snapshot since the last scan — serving the "
                                    "cached result instead of re-running. Use 'Re-scan anyway' to force.")
            yield self.emit("basic_scorecard", asset=asset, exact=True, scope="full", cached=True,
                            overall_score=b.get("overall_score"), counts=b.get("counts"),
                            total_rows=b.get("total_rows"), driver=b.get("driver"), checks=b.get("checks", []))
            yield self.emit("sample_profile", asset=asset, cached=True, estimated=True,
                            sampled_rows=p.get("sampled_rows", 0), sample_rule=p.get("sample_rule", ""),
                            columns=p.get("columns", []))
            hints = self._semantic_hints(p.get("columns", []))
            if hints:
                yield self.emit("semantic_hints", asset=asset, hints=hints,
                                note="From the last scan — confirm before binding")
            yield self.emit("question", asset=asset,
                            message="Showing the last result (data unchanged). Name columns/rules to scan, "
                                    "or send your answer to /api/quality-guardian/act.",
                            suggested=hints, profile=p)
            yield self.emit("complete", summary=f"{asset} unchanged since {last.get('scanned_at', 'last scan')} "
                                                f"— served cached score {b.get('overall_score')}.")
            return

        # Stage A — basic exact checks (full table)
        yield self.emit("step", name="basic_checks", status="running")
        try:
            basic = await asyncio.wait_for(
                asyncio.to_thread(profiler.basic_checks, asset, fields),
                timeout=profiler.QUERY_TIMEOUT_SECS,
            )
        except Exception as e:
            logger.warning(f"[qg2] basic checks failed for {asset}: {e}")
            yield self.emit("error", message=f"Basic checks failed: {e}")
            return
        yield self.emit("basic_scorecard", asset=asset,
                        overall_score=basic["overall_score"], counts=basic["counts"],
                        total_rows=basic["total_rows"], driver=basic["driver"],
                        checks=basic["checks"], scope="full", exact=True)
        yield self.emit("step", name="basic_checks", status="complete")

        # Stage B — sample profile
        yield self.emit("step", name="sample_profile", status="running")
        try:
            prof = await asyncio.wait_for(
                asyncio.to_thread(profiler.sample_profile, asset, fields, basic["total_rows"]),
                timeout=profiler.QUERY_TIMEOUT_SECS,
            )
        except Exception as e:
            logger.warning(f"[qg2] sample profile failed for {asset}: {e}")
            prof = {"asset": asset, "sampled_rows": 0, "estimated": True, "columns": [], "error": str(e)}
        yield self.emit("sample_profile", asset=asset, sampled_rows=prof.get("sampled_rows", 0),
                        sample_rule=prof.get("sample_rule", ""), estimated=True,
                        columns=prof.get("columns", []))
        yield self.emit("step", name="sample_profile", status="complete")

        # Persist the version + results so the next profile can skip if data is unchanged.
        scan_state.save(asset, version, basic, prof)

        # Stage C — render + ask (with cheap heuristic hints; user/model decides what to deepen)
        hints = self._semantic_hints(prof.get("columns", []))
        if hints:
            yield self.emit("semantic_hints", asset=asset, hints=hints,
                            note="Inferred from sample evidence — confirm before binding")
        self.log_decision(
            decision_type="profile",
            inputs={"asset": asset, "goal": goal, "version": version},
            output={"overall_score": basic["overall_score"], "total_rows": basic["total_rows"],
                    "sampled_rows": prof.get("sampled_rows", 0)},
            metadata={"hints": hints},
        )
        yield self.emit("question", asset=asset,
                        message="Any specific columns or rules you want me to scan? "
                                "(Name columns, or say 'go' to deep-scan the suggested ones.) "
                                "Send your answer to /api/quality-guardian/act.",
                        suggested=hints, profile=prof)
        yield self.emit("complete", summary=f"Profiled {asset}: score {basic['overall_score']} "
                                            f"on {basic['total_rows']:,} rows. Awaiting focus.")

    # ── Stage D — bounded act on the user's instruction ───────────────────────

    async def _act(self, asset: str, user_action: str, client_profile: Optional[dict],
                   confirm: bool) -> AsyncGenerator[dict, None]:
        yield self.emit("started", asset=asset, mode="act", user_action=user_action)

        # Re-resolve schema server-side — grounding must not depend on client input
        fields = await self._schema_fields(asset)
        if not fields:
            yield self.emit("error", message=f"Could not resolve schema for {asset}")
            return
        schema = {f["name"]: f.get("type", "string") for f in fields}

        # LLM translates NL instruction → candidate IR (grounded by schema + profile)
        yield self.emit("thought", message="Translating your request into bounded checks…")
        req = await self._llm_to_ir(asset, user_action, schema, client_profile)
        yield self.emit("proposed_checks", asset=asset, raw=req.get("checks", []), scope=req.get("scope", "sample"))

        # Validate against the boundary — fail closed
        result = check_ir.validate_request(req, schema)
        yield self.emit("validation", ok=result["ok"], accepted=len(result["checks"]),
                        errors=result["errors"], scope=result["scope"])
        if not result["ok"]:
            yield self.emit("complete", summary="No valid checks to run. "
                            f"Rejected: {'; '.join(result['errors']) or 'nothing proposed'}")
            return

        # Confirm gate — a full-table scan must be explicitly approved
        if result["needs_confirm"] and not confirm:
            yield self.emit("confirm_required", asset=asset, scope="full",
                            checks=result["checks"],
                            message="This requests a FULL-TABLE scan (beyond the sample). "
                                    "Re-send with confirm=true to proceed; otherwise it runs on the sample.")
            return

        scope = result["scope"]
        # sample scope needs the row count to size the TABLESAMPLE; full scope = no clause
        sample_clause = None
        if scope == "sample":
            total = await self._row_estimate(asset, fields)
            sample_clause = profiler._sample_clause(total)

        sql, specs = check_ir.compile_sql(asset, result["checks"], sample_clause)
        logger.info(f"[qg2] compiled DQ SQL for {asset} ({scope}): {sql[:300]}")
        yield self.emit("executing", asset=asset, scope=scope, check_count=len(result["checks"]))

        try:
            row = await asyncio.wait_for(
                asyncio.to_thread(self._run_sql, sql), timeout=profiler.QUERY_TIMEOUT_SECS)
        except Exception as e:
            logger.warning(f"[qg2] check execution failed for {asset}: {e}")
            self.log_decision("targeted_check_failed", {"asset": asset, "action": user_action},
                              {"error": str(e)}, status="fail")
            yield self.emit("error", message=f"Check execution failed: {e}")
            return

        checks = check_ir.grade(row, specs, scope)
        score = profiler._score(checks)
        yield self.emit("results", asset=asset, scope=scope, exact=(scope == "full"),
                        overall_score=score["overall_score"], counts=score["counts"],
                        driver=score["driver"], checks=checks)

        status = "fail" if score["counts"]["fail"] else ("warn" if score["counts"]["warn"] else "success")
        self.log_decision(
            decision_type="targeted_check",
            inputs={"asset": asset, "action": user_action, "scope": scope},
            output={"overall_score": score["overall_score"], "counts": score["counts"]},
            metadata={"checks": checks},
            status=status,
        )
        yield self.emit("complete", summary=f"Ran {len(checks)} checks on {asset} ({scope}). "
                                            f"Score {score['overall_score']}. "
                                            "Send another instruction to continue.")

    # ── helpers ───────────────────────────────────────────────────────────────

    async def _describe_meta(self, asset: str) -> Optional[dict]:
        """Full table metadata (schema + snapshot history), cached. One catalog read."""
        cached = _META_CACHE.get(asset)
        if cached and time.monotonic() - cached[0] < _META_TTL:
            return cached[1]
        try:
            meta = await asyncio.to_thread(describe_iceberg_table, asset)
            if meta and meta.get("fields"):
                _META_CACHE[asset] = (time.monotonic(), meta)
            return meta
        except Exception as e:
            logger.warning(f"[qg2] describe failed for {asset}: {e}")
            return None

    async def _schema_fields(self, asset: str) -> list[dict]:
        meta = await self._describe_meta(asset)
        return (meta or {}).get("fields", [])

    @staticmethod
    def _data_version(meta: Optional[dict]) -> Optional[str]:
        """The current data version = id of the most recent Iceberg snapshot.
        None when unknown (non-Iceberg / mock) → the freshness gate fails open."""
        snaps = (meta or {}).get("snapshots") or []
        if not snaps:
            return None
        latest = max(snaps, key=lambda s: s.get("timestamp_ms") or 0)
        sid = latest.get("snapshot_id")
        return str(sid) if sid is not None else None

    async def _row_estimate(self, asset: str, fields: list[dict]) -> int:
        """Cheap-ish row count to size the sample. Falls back to 0 (→ cap-limited sample)."""
        try:
            def _count():
                conn = profiler.get_impala_conn()
                try:
                    cur = conn.cursor()
                    cur.execute(f"SELECT COUNT(*) FROM {asset}")
                    return int((cur.fetchone() or [0])[0] or 0)
                finally:
                    conn.close()
            return await asyncio.wait_for(asyncio.to_thread(_count), timeout=profiler.QUERY_TIMEOUT_SECS)
        except Exception:
            return 0

    def _run_sql(self, sql: str) -> dict:
        conn = profiler.get_impala_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            row = cur.fetchone()
            return dict(zip([d[0] for d in cur.description], row))
        finally:
            conn.close()

    def _semantic_hints(self, columns: list[dict]) -> list[dict]:
        """Cheap, evidence-based suggestions from the sample profile — NOT bound rules.
        These seed the 'go' default and the user's review; the model refines on request."""
        hints = []
        for c in columns:
            col, looks = c.get("column"), c.get("looks_like") or {}
            proposed = []
            # strong fingerprint match → propose the matching format check
            for fp, frac in looks.items():
                if frac >= 0.8 and fp in ("email", "ipv4", "uuid", "url", "phone"):
                    proposed.append({"col": col, "type": "regex_match", "pattern": fp})
            # all-positive numeric that dips negative is suspicious → non_negative
            if c.get("negatives") == 0 and c.get("min") is not None and c["min"] >= 0:
                proposed.append({"col": col, "type": "non_negative"})
            if c.get("future_dates", 0) == 0 and c.get("type", "").lower().startswith(("timestamp", "date")):
                proposed.append({"col": col, "type": "not_future"})
            if proposed:
                hints.append({"column": col, "evidence": looks or {"min": c.get("min"), "max": c.get("max")},
                              "proposed_checks": proposed})
        return hints

    async def _llm_to_ir(self, asset: str, user_action: str, schema: dict,
                         profile: Optional[dict]) -> dict:
        """Ask the LLM to turn an NL instruction into a check-IR request. The output is
        treated as untrusted — validate_request() is the real gate."""
        cols = [{"name": n, "type": t} for n, t in schema.items()]
        prof_cols = (profile or {}).get("columns", []) if isinstance(profile, dict) else []
        system = (
            "You convert a data-quality instruction into a STRICT JSON request. "
            "You may ONLY use columns from the provided schema and check types from the catalog. "
            "Never invent columns, raw SQL, or regex. Respond with JSON only."
        )
        user = f"""Asset: {asset}

Schema (only these columns are valid):
{json.dumps(cols, indent=2)}

Sample profile (evidence — null rates, ranges, fingerprints):
{json.dumps(prof_cols, indent=2)[:4000]}

Allowed check types and their params:
{json.dumps(check_ir.CHECK_CATALOG, indent=2)}
regex_match.pattern must be one of: {list(check_ir.PATTERNS.keys())}

User instruction: "{user_action}"

Return JSON exactly like:
{{"scope": "sample" | "full",
  "checks": [{{"col": "<schema column>", "type": "<allowed type>", ...params}}]}}
Use scope "full" only if the user explicitly asks to scan the whole/entire table."""
        raw = await self._call_llm(user, system)
        return self._parse_json(raw)

    async def _call_llm(self, prompt: str, system: str) -> str:
        import httpx
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{LLM_BASE_URL}/chat/completions",
                    json={"model": LLM_MODEL, "temperature": 0.1, "max_tokens": 800,
                          "messages": [{"role": "system", "content": system},
                                       {"role": "user", "content": prompt}]},
                    headers={"Authorization": f"Bearer {LLM_API_KEY}"},
                )
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
                logger.info(f"[qg2][LLM] system={system[:60]}… → {content[:120]}…")
                return content
        except Exception as e:
            logger.warning(f"[qg2] LLM call failed: {e}")
            return "{}"

    @staticmethod
    def _parse_json(text: str) -> dict:
        """Extract the first JSON object from the model output; tolerant of code fences."""
        import re as _re
        if not text:
            return {}
        m = _re.search(r"\{.*\}", text, _re.DOTALL)
        if not m:
            return {}
        try:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else {}
        except json.JSONDecodeError:
            return {}
