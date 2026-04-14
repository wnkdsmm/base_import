from __future__ import annotations

from fastapi import APIRouter, Query

from .api_common import run_analytics_request


router = APIRouter()


def get_access_points_data(**kwargs):
    from app.services.access_points.core import get_access_points_data as _get_access_points_data

    return _get_access_points_data(**kwargs)


@router.get("/api/access-points-data")
def access_points_data_endpoint(
    table_name: str = "all",
    district: str = "all",
    year: str = "all",
    limit: str = "25",
    feature_columns: list[str] | None = Query(None),
):
    return run_analytics_request(
        lambda: get_access_points_data(
            table_name=table_name,
            district=district,
            year=year,
            limit=limit,
            feature_columns=feature_columns or [],
        ),
        invalid_code="access_points_invalid_request",
        invalid_message="РќРµРєРѕСЂСЂРµРєС‚РЅС‹Рµ РїР°СЂР°РјРµС‚СЂС‹ СЂРµР№С‚РёРЅРіР° РїСЂРѕР±Р»РµРјРЅС‹С… С‚РѕС‡РµРє.",
        failed_code="access_points_failed",
        failed_message="РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕСЃС‚СЂРѕРёС‚СЊ СЂРµР№С‚РёРЅРі РїСЂРѕР±Р»РµРјРЅС‹С… С‚РѕС‡РµРє. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕРІС‚РѕСЂРёС‚СЊ Р·Р°РїСЂРѕСЃ.",
    )
