import os
import json
import re
import difflib
from typing import Dict, Any, List
from dotenv import load_dotenv
from google import genai
from pydantic import BaseModel, Field

load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")


def _similar(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


def _normalize_columns(raw_columns_list) -> List[str]:
    cols = []
    if isinstance(raw_columns_list, list):
        for item in raw_columns_list:
            if isinstance(item, str):
                cols.append(item)
            elif isinstance(item, dict) and "name" in item:
                cols.append(item["name"])
    return cols


def _extract_clause(sql: str, start_kw: str, stop_kws: List[str]) -> str:
    """Return substring between start_kw and the earliest of stop_kws (or end). Case-insensitive."""
    pattern = re.compile(rf"(?i){re.escape(start_kw)}\s+(.*)", re.S)
    m = pattern.search(sql)
    if not m:
        return ""
    rest = m.group(1)
    # find earliest stop keyword
    idx = None
    for kw in stop_kws:
        r = re.search(rf"(?i)\b{re.escape(kw)}\b", rest)
        if r:
            if idx is None or r.start() < idx:
                idx = r.start()
    return rest if idx is None else rest[:idx]


def ai_sql(body: Dict[str, Any]) -> Dict[str, Any]:
    client = genai.Client(api_key=API_KEY)

    table_name = body.get("table_name", "")
    raw_columns_list = body.get("columns_list", [])
    natural_query = body.get("query", "")

    columns_list = _normalize_columns(raw_columns_list)

    # Minimal schema
    class SQLResult(BaseModel):
        sql: str = Field(description="SQL query with literal values embedded.")

    columns_str = ", ".join(columns_list)

    prompt = f"""
You convert English into SQL.

IMPORTANT:
- Embed literal values directly in SQL.
- Only use columns from this list: {columns_str}
- If user asks for a column NOT in the list, DO NOT invent a column.
- If you cannot map the user's request to the table/columns, return a SQL that indicates an error or just produce an empty result.
User query: {natural_query}
Table: {table_name}

Return JSON matching this schema:
{SQLResult.model_json_schema()}
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_json_schema": SQLResult.model_json_schema(),
            },
        )

        json_text = (response.text or "").strip()
        try:
            json_obj = json.loads(json_text)
        except json.JSONDecodeError:
            m = re.search(r"(\{.*\})", json_text, re.S)
            if not m:
                return {"status": "error", "message": "Invalid JSON from model"}
            json_obj = json.loads(m.group(1))

        try:
            SQLResult.model_validate(json_obj)
        except Exception:
            return {"status": "error", "message": "Invalid SQL structure from model"}

        sql = (json_obj.get("sql") or "").strip()
        if not sql:
            return {"status": "error", "message": "Model returned empty SQL"}

        sql_lower = sql.lower()

        # Reject obvious always-false fallbacks
        always_false_patterns = [r"1\s*=\s*0", r"0\s*=\s*1", r"where\s+false", r"1\s*!=\s*1", r"1\s*<>\s*1"]
        for pat in always_false_patterns:
            if re.search(pat, sql_lower):
                return {"status": "error", "message": "Query cannot be created from the given input"}

        # If model returned a direct error string SQL like:
        # SELECT 'Error: Column "salary" does not exist...' AS error_message;
        # detect and return clean error
        if re.search(r"select\s+'[^']*error[^']*'", sql_lower) or re.search(r"does not exist", sql_lower) or re.search(r"column .* does not exist", sql_lower):
            return {"status": "error", "message": "Invalid column used in query"}

        # --- CLEAN STRINGS before tokenizing ---
        # remove single-quoted and double-quoted string literals so their inner words don't pollute tokens
        sql_no_strings = re.sub(r"'(?:[^']|'')*'", "''", sql)  # replace with empty literal
        sql_no_strings = re.sub(r'"(?:[^"]|"")*"', '""', sql_no_strings)

        # extract backticked identifiers explicitly (these are clear column references)
        backticked = re.findall(r"`([^`]+)`", sql_no_strings)

        # extract SELECT clause identifiers (between SELECT and FROM)
        select_part = _extract_clause(sql_no_strings, "select", ["from"])
        # remove aliases after AS to avoid validating aliases
        select_part_clean = re.sub(r"(?i)\bas\s+[a-zA-Z_][a-zA-Z0-9_]*", "", select_part)
        # also remove comma-separated aliases after space (e.g., "col1 col_alias")
        select_part_clean = re.sub(r"\b[a-zA-Z_][a-zA-Z0-9_]*\s+[a-zA-Z_][a-zA-Z0-9_]*\b", lambda m: m.group(0).split()[0], select_part_clean)

        # extract WHERE clause (up to ORDER/GROUP/LIMIT)
        where_part = _extract_clause(sql_no_strings, "where", ["group", "order", "limit", ";"])

        # find candidate tokens in select and where parts
        tokens_select = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", select_part_clean)
        tokens_where = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", where_part)

        # Build allowed words from provided column names
        allowed_words = set()
        allowed_full = set()
        for c in columns_list:
            lc = c.lower()
            normalized_full = re.sub(r"\s+", "_", lc)
            allowed_full.add(normalized_full)
            for w in re.findall(r"[a-zA-Z0-9_]+", lc):
                allowed_words.add(w)

        # also include table name parts
        if table_name:
            for w in re.findall(r"[a-zA-Z0-9_]+", table_name.lower()):
                allowed_words.add(w)
                allowed_full.add(w)

        # helper to validate one identifier fuzzily
        def matches_allowed(ident: str) -> bool:
            n = ident.lower().strip()
            if re.fullmatch(r"\d+", n):
                return True
            if n in allowed_full:
                return True
            if n in allowed_words:
                return True
            # prefix checks
            for aw in allowed_words:
                if n.startswith(aw) or aw.startswith(n):
                    return True
            # fuzzy similarity
            for aw in allowed_words:
                if _similar(n, aw) >= 0.75:
                    return True
            for af in allowed_full:
                if _similar(n, af) >= 0.75:
                    return True
            return False

        # Collect candidate identifiers to check:
        candidates = set()
        # check backticked names split into words and normalized full
        for bt in backticked:
            # add full normalized and component words
            candidates.add(re.sub(r"\s+", "_", bt.lower()))
            for w in re.findall(r"[a-zA-Z0-9_]+", bt.lower()):
                candidates.add(w)
        # tokens from select and where
        for t in tokens_select + tokens_where:
            candidates.add(t.lower())

        # Filter out SQL keywords and small tokens
        sql_keywords = {
            "select", "from", "where", "and", "or", "order", "by", "group", "having",
            "limit", "offset", "asc", "desc", "join", "left", "right", "inner", "on",
            "as", "in", "is", "null", "not", "between", "like", "distinct", "count",
            "sum", "avg", "min", "max", "case", "when", "then", "else"
        }

        unknowns = []
        for ident in candidates:
            if not ident or ident in sql_keywords:
                continue
            if len(ident) <= 1:
                continue
            if not matches_allowed(ident):
                unknowns.append(ident)

        if unknowns:
            # keep message simple per your requirement
            return {"status": "error", "message": "Invalid column used in query"}

        # If all checks pass → success
        return {"status": "success", "sql": sql}

    except Exception:
        return {"status": "error", "message": "Internal processing error"}

if __name__ == "__main__":
    test_body = { 'columns_list': [ {'name': 'Roll_No', 'type': 'number'}, {'name': 'Candidate Last Name', 'type': 'text'}, {'name': 'Degree/Dept', 'type': 'text'}, {'name': 'Select Year of Passing', 'type': 'number'}, {'name': 'Nationality', 'type': 'text'}, {'name': 'num', 'type': 'number'} ],
                  'table_name': 'A4Zi', 'query': "Show everyone whose last name ends with y" }
    result = ai_sql(test_body)
    print(json.dumps(result, indent=2))
