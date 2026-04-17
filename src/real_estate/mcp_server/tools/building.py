"""MCP tools for building registry (건축물대장) data — 건축HUB."""

from __future__ import annotations

import os
import urllib.parse
from datetime import datetime
from typing import Any

from real_estate.mcp_server import mcp
from real_estate.mcp_server._helpers import _check_api_key, _fetch_json

_BUILDING_REGISTRY_URL = (
    "https://apis.data.go.kr/1613000/BldRgstHubService/getBrBasisOulnInfo"
)

# 주용도 → property_type 허용 목록
_PURPOSE_MAP: dict[str, list[str]] = {
    "apartment":  ["아파트"],
    "villa":      ["다세대주택", "연립주택"],
    "house":      ["단독주택", "다가구주택"],
    "officetel":  ["오피스텔"],
    "commercial": ["제1종근린생활시설", "제2종근린생활시설", "판매시설", "업무시설"],
}


def _registry_url(region_code: str, page_no: int, num_of_rows: int) -> str:
    key = os.getenv("DATA_GO_KR_API_KEY", "")
    params = urllib.parse.urlencode({
        "sigunguCd":  region_code,
        "numOfRows":  num_of_rows,
        "pageNo":     page_no,
        "_type":      "json",
    })
    return f"{_BUILDING_REGISTRY_URL}?serviceKey={urllib.parse.quote(key, safe='')}&{params}"


def _extract_items(raw: Any) -> list[dict]:
    """JSON 응답에서 item 목록 추출 (단건일 때 dict → list 변환)."""
    try:
        items = raw["response"]["body"]["items"]
        if not items:
            return []
        data = items.get("item", [])
        return [data] if isinstance(data, dict) else data
    except (KeyError, TypeError):
        return []


@mcp.tool()
async def get_building_registry(
    region_code: str,
    property_type: str = "villa",
    num_of_rows: int = 1000,
) -> dict[str, Any]:
    """Return building registry (건축물대장) records for a region.

    Unlike trade data, this covers ALL buildings — including ones with no recent
    transactions — making it essential for identifying redevelopment candidates
    that are simply not being traded (매물 잠김).

    Korean keywords: 건축물대장, 재건축, 재개발, 노후건물, 준공연도, 용적률, 건폐율

    Args:
        region_code:   5-digit legal district code (from get_region_code).
        property_type: apartment / villa / house / officetel / commercial.
        num_of_rows:   Max records (default 1000).

    Returns:
        total_count: Total buildings returned from API.
        items: All buildings — bld_nm, dong, build_year, age_years,
               vl_rat(용적률), bc_rat(건폐율), tot_area, hhld_cnt,
               floor_cnt, purpose, plat_plc.
        old_items: Subset where age_years >= 20 (재건축 연한 기준).
        old_count: len(old_items).
    """
    err = _check_api_key()
    if err:
        return err

    url = _registry_url(region_code, page_no=1, num_of_rows=num_of_rows)
    raw, fetch_err = await _fetch_json(url)
    if fetch_err:
        return fetch_err

    raw_items = _extract_items(raw)
    if not raw_items:
        return {
            "error": "no_data",
            "message": "건축물대장 데이터를 찾을 수 없습니다.",
            "items": [], "old_items": [], "total_count": 0, "old_count": 0,
        }

    allowed = _PURPOSE_MAP.get(property_type, [])
    current_year = datetime.now().year
    items: list[dict] = []

    for r in raw_items:
        purpose = (r.get("mainPurpsCdNm") or "").strip()
        if allowed and not any(p in purpose for p in allowed):
            continue

        # 사용승인일 YYYYMMDD → 연도
        use_apr = str(r.get("useAprDay") or "").strip()
        build_year = int(use_apr[:4]) if len(use_apr) >= 4 and use_apr[:4].isdigit() else None
        age = (current_year - build_year) if build_year else None

        def _f(v: Any) -> float | None:
            try:
                return float(v) if v not in (None, "", " ") else None
            except (TypeError, ValueError):
                return None

        items.append({
            "bld_nm":    (r.get("bldNm") or "").strip(),
            "dong":      (r.get("dongNm") or "").strip(),
            "plat_plc":  (r.get("platPlc") or "").strip(),
            "build_year": build_year,
            "age_years":  age,
            "vl_rat":    _f(r.get("vlRat")),      # 용적률 (%)
            "bc_rat":    _f(r.get("bcRat")),       # 건폐율 (%)
            "tot_area":  _f(r.get("totArea")),     # 연면적 (㎡)
            "hhld_cnt":  r.get("hhldCnt"),         # 세대수
            "floor_cnt": r.get("grndFlrCnt"),      # 지상층수
            "purpose":   purpose,
        })

    old_items = [i for i in items if i.get("age_years") and i["age_years"] >= 20]
    # 오래된 순 정렬
    old_items.sort(key=lambda x: x.get("build_year") or 9999)

    return {
        "total_count": len(items),
        "old_count":   len(old_items),
        "items":       items,
        "old_items":   old_items,
    }
