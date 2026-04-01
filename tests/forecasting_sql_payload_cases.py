from datetime import date

from tests.forecasting_sql_support import ForecastingSqlSupport, forecasting_core, patch, resolve_option


class ForecastingSqlPayloadTests(ForecastingSqlSupport):
    def test_partial_forecast_uses_sql_count_without_loading_decision_support(self) -> None:
        table_options = [{"value": "fires", "label": "РџРѕР¶Р°СЂС‹"}]
        option_catalog = {
            "districts": [{"value": "all", "label": "Р’СЃРµ СЂР°Р№РѕРЅС‹"}],
            "causes": [{"value": "all", "label": "Р’СЃРµ РїСЂРёС‡РёРЅС‹"}],
            "object_categories": [{"value": "all", "label": "Р’СЃРµ РєР°С‚РµРіРѕСЂРёРё"}],
        }
        metadata_items = [{"table_name": "fires", "resolved_columns": {"date": "fire_date"}}]
        daily_history = [
            {"date": date(2024, 1, 1), "count": 3, "avg_temperature": 1.5},
            {"date": date(2024, 1, 2), "count": 0, "avg_temperature": None},
        ]
        forecast_rows = [
            {
                "date": date(2024, 1, 3),
                "date_display": "03.01.2024",
                "weekday_label": "СЃСЂ",
                "forecast_value": 2.0,
                "forecast_value_display": "2",
                "fire_probability": 0.5,
                "fire_probability_display": "50%",
                "scenario_label": "Р’С‹С€Рµ РѕР±С‹С‡РЅРѕРіРѕ",
                "scenario_hint": "РўРµСЃС‚РѕРІС‹Р№ СЃС†РµРЅР°СЂРёР№",
                "scenario_tone": "fire",
            }
        ]
        backtest = {
            "is_ready": False,
            "message": "",
            "rows": [],
            "model_metrics": {},
            "baseline_metrics": {},
            "overview": {"folds": 0, "min_train_days": 0, "validation_horizon_days": 1},
        }

        with (
            patch.object(forecasting_core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(forecasting_core, "_resolve_forecasting_selection", return_value="fires"),
            patch.object(forecasting_core, "_selected_source_tables", return_value=["fires"]),
            patch.object(forecasting_core, "_parse_forecast_days", return_value=14),
            patch.object(forecasting_core, "_parse_history_window", return_value="all"),
            patch.object(forecasting_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])),
            patch.object(forecasting_core, "_build_option_catalog_sql", return_value=option_catalog),
            patch.object(forecasting_core, "_resolve_option_value", side_effect=resolve_option),
            patch.object(forecasting_core, "_count_forecasting_records_sql", return_value=17) as count_mock,
            patch.object(forecasting_core, "_build_daily_history_sql", return_value=daily_history),
            patch.object(forecasting_core, "_run_scenario_backtesting", return_value=backtest),
            patch.object(forecasting_core, "_build_forecast_rows", return_value=forecast_rows),
            patch.object(forecasting_core, "_build_weekday_profile", return_value=[]),
            patch.object(
                forecasting_core,
                "_build_forecast_chart",
                return_value={"title": "daily", "plotly": {}, "empty_message": ""},
            ),
            patch.object(
                forecasting_core,
                "_build_forecast_breakdown_chart",
                return_value={"title": "breakdown", "plotly": {}, "empty_message": ""},
            ),
            patch.object(
                forecasting_core,
                "_build_weekday_chart",
                return_value={"title": "weekday", "plotly": {}, "empty_message": ""},
            ),
            patch.object(
                forecasting_core,
                "_build_geo_chart",
                return_value={"title": "geo", "plotly": {}, "empty_message": "pending"},
            ),
            patch.object(forecasting_core, "_build_insights", return_value=[]),
            patch.object(
                forecasting_core,
                "build_decision_support_payload",
                side_effect=AssertionError("decision support must stay deferred"),
            ),
        ):
            payload = forecasting_core.get_forecasting_data(
                table_name="fires",
                district="all",
                cause="all",
                object_category="all",
                temperature="",
                forecast_days="14",
                history_window="all",
                include_decision_support=False,
            )

        count_mock.assert_called_once()
        self.assertTrue(payload["has_data"])
        self.assertTrue(payload["decision_support_pending"])
        self.assertFalse(payload["decision_support_ready"])
        self.assertEqual(payload["summary"]["fires_count_display"], "17")
        self.assertEqual(payload["charts"]["geo"]["empty_message"], "pending")

    def test_forecasting_service_payload_smoke_keeps_sections_consistent(self) -> None:
        payload = self._build_forecasting_service_payload_smoke()
        summary = payload["summary"]
        comparison_rows = payload["quality_assessment"]["comparison_rows"]
        temperature_card = next(item for item in payload["features"] if item["label"] == "РўРµРјРїРµСЂР°С‚СѓСЂР°")

        self.assertTrue(payload["has_data"])
        self.assertTrue(payload["decision_support_pending"])
        self.assertFalse(payload["decision_support_ready"])
        self.assertEqual(summary["selected_table_label"], "fires")
        self.assertEqual(summary["fires_count_display"], "17")
        self.assertEqual(summary["history_days_display"], "3")
        self.assertEqual(summary["forecast_days_display"], "2")
        self.assertEqual(summary["predicted_total_display"], "3")
        self.assertEqual(summary["peak_forecast_day_display"], "04.01.2024")
        self.assertEqual(
            payload["notes"],
            [
                "РСЃС‚РѕСЂРёСЏ РєРѕСЂРѕС‚РєР°СЏ, РїРѕСЌС‚РѕРјСѓ СЃС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· РјРѕР¶РµС‚ Р±С‹С‚СЊ РјРµРЅРµРµ СѓСЃС‚РѕР№С‡РёРІС‹Рј.",
                "РЎС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· Р»СѓС‡С€Рµ С‡РёС‚Р°С‚СЊ РєР°Рє РѕСЂРёРµРЅС‚РёСЂ РїРѕ СѓСЂРѕРІРЅСЋ РЅР°РіСЂСѓР·РєРё Рё РїСЂРёРѕСЂРёС‚РµС‚Р°Рј, Р° РЅРµ РєР°Рє С‚РѕС‡РЅРѕРµ РѕР±РµС‰Р°РЅРёРµ С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ РІ РєР°Р¶РґС‹Р№ РґРµРЅСЊ.",
            ],
        )
        self.assertEqual(temperature_card["status_label"], "РќРёР·РєРѕРµ РїРѕРєСЂС‹С‚РёРµ (2/3 РґРЅРµР№ (66,7%))")
        self.assertEqual(
            temperature_card["source"],
            "fires: avg_temperature | РїРѕРєСЂС‹С‚РёРµ РїРѕ РґРЅРµРІРЅРѕР№ РёСЃС‚РѕСЂРёРё: 2/3 РґРЅРµР№ (66,7%)",
        )
        self.assertEqual(
            temperature_card["description"],
            "РљРѕР»РѕРЅРєР° С‚РµРјРїРµСЂР°С‚СѓСЂС‹ РЅР°Р№РґРµРЅР°, РЅРѕ РїРѕРєСЂС‹С‚РёРµ РЅРёР·РєРѕРµ: С‚РµРјРїРµСЂР°С‚СѓСЂРЅС‹Р№ РїСЂРёР·РЅР°Рє РЅРµР»СЊР·СЏ СЃС‡РёС‚Р°С‚СЊ РЅР°РґС‘Р¶РЅС‹Рј РґР»СЏ ML Рё С‚РµРјРїРµСЂР°С‚СѓСЂРЅРѕР№ РїРѕРїСЂР°РІРєРё.",
        )
        self.assertEqual(temperature_card["coverage_display"], "2/3 РґРЅРµР№ (66,7%)")
        self.assertEqual(
            comparison_rows,
            [
                {
                    "method_label": "РЎРµР·РѕРЅРЅР°СЏ Р±Р°Р·РѕРІР°СЏ РјРѕРґРµР»СЊ",
                    "role_label": "Р‘Р°Р·РѕРІР°СЏ РјРѕРґРµР»СЊ",
                    "mae_display": "0,5",
                    "rmse_display": "0,7",
                    "smape_display": "20%",
                    "selection_label": "РћРїРѕСЂРЅР°СЏ Р»РёРЅРёСЏ",
                    "mae_delta_display": "0%",
                },
                {
                    "method_label": "РЎС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР·",
                    "role_label": "Р­РІСЂРёСЃС‚РёС‡РµСЃРєР°СЏ РјРѕРґРµР»СЊ",
                    "mae_display": "0,4",
                    "rmse_display": "0,6",
                    "smape_display": "18%",
                    "selection_label": "Р Р°Р±РѕС‡Р°СЏ РјРѕРґРµР»СЊ",
                    "mae_delta_display": "-20%",
                },
            ],
        )
        self.assertEqual(payload["risk_prediction"]["feature_cards"], payload["features"])
        self._assert_no_mojibake(
            {
                "summary": summary,
                "notes": payload["notes"],
                "features": payload["features"],
                "comparison_rows": comparison_rows,
            },
            context="forecasting service payload smoke",
        )
