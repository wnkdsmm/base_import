import re
import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "app" / "templates"

EXPECTED_SIDEBAR_ACTIVE = {
    "access_points.html": "access_points",
    "clustering.html": "clustering",
    "column_search.html": "column_search",
    "fire_map.html": "fire_map",
    "forecasting.html": "forecasting",
    "index.html": "dashboard",
    "ml_model.html": "ml_model",
    "select_table.html": "select_table",
    "tables.html": "tables",
    "table_view.html": "tables",
}

EXPECTED_NAV_HREFS = {
    "access_points": "/access-points",
    "clustering": "/clustering",
    "column_search": "/column-search",
    "dashboard": "/",
    "fire_map": "/fire-map",
    "forecasting": "/forecasting",
    "ml_model": "/ml-model",
    "select_table": "/select_table",
    "tables": "/tables",
}


def _read_template(name: str) -> str:
    return (TEMPLATE_DIR / name).read_text(encoding="utf-8")


class TemplateLayoutSmokeTest(unittest.TestCase):
    def test_base_template_includes_shared_sidebar_nav(self):
        base = _read_template("base.html")

        self.assertIn("{% include 'includes/sidebar_nav.html' %}", base)
        self.assertNotIn("sidebar_active_class", base)

    def test_sidebar_pages_define_expected_active_key(self):
        expected_templates = set(EXPECTED_SIDEBAR_ACTIVE)
        sidebar_templates = set()

        for path in TEMPLATE_DIR.glob("*.html"):
            text = path.read_text(encoding="utf-8")
            if '{% extends "base.html" %}' not in text:
                continue
            if re.search(r"{%\s*block\s+body\s*%}", text):
                continue
            if path.name == "base.html":
                continue
            sidebar_templates.add(path.name)

        self.assertEqual(sidebar_templates, expected_templates)

        for name, expected in EXPECTED_SIDEBAR_ACTIVE.items():
            with self.subTest(template=name):
                text = _read_template(name)
                self.assertNotIn("sidebar_active_class", text)
                match = re.search(
                    r'{%\s*set\s+sidebar_active\s*=\s*"([^"]+)"\s*%}',
                    text,
                )
                self.assertIsNotNone(match)
                self.assertEqual(match.group(1), expected)

    def test_sidebar_nav_uses_sidebar_active_as_single_source(self):
        env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=True)
        template = env.get_template("includes/sidebar_nav.html")
        source = _read_template("includes/sidebar_nav.html")

        self.assertNotIn("sidebar_active_class", source)

        for active, expected_href in EXPECTED_NAV_HREFS.items():
            with self.subTest(active=active):
                html = template.render(sidebar_active=active)
                anchors = re.findall(r"<a\b[^>]*>.*?</a>", html, flags=re.DOTALL)
                current_anchors = [
                    anchor for anchor in anchors if 'aria-current="page"' in anchor
                ]
                active_anchors = [anchor for anchor in anchors if "is-active" in anchor]

                self.assertEqual(len(current_anchors), 1)
                self.assertEqual(active_anchors, current_anchors)
                self.assertIn(f'href="{expected_href}"', current_anchors[0])

    def test_pages_load_shared_helpers_before_dependent_scripts(self):
        expectations = {
            "index.html": ("analytics_shared.js", "dashboard.js"),
            "tables.html": ("analytics_shared.js", "import.js", "tables.js"),
        }

        for template_name, scripts in expectations.items():
            with self.subTest(template=template_name):
                text = _read_template(template_name)
                positions = {script: text.index(script) for script in scripts}
                shared_position = positions["analytics_shared.js"]

                for script, position in positions.items():
                    if script == "analytics_shared.js":
                        continue
                    self.assertLess(shared_position, position)

    def test_forecasting_quality_panel_is_removed_from_template(self):
        include_text = _read_template("includes/forecasting/_quality_overview.html")

        self.assertNotIn("scenarioQualityPanel", include_text)
        self.assertNotIn("scenarioQualityTitle", include_text)
        self.assertNotIn("scenarioQualityMethodology", include_text)


if __name__ == "__main__":
    unittest.main()
