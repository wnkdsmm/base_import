.PHONY: jsmin

jsmin:
	@echo "Minifying JS into app/static/js_min using python -m jsmin"
	@python -c "from pathlib import Path; import subprocess; src=Path('app/static/js'); dst=Path('app/static/js_min'); dst.mkdir(parents=True, exist_ok=True); [ (dst / p.relative_to(src)).parent.mkdir(parents=True, exist_ok=True) or (dst / p.relative_to(src)).write_text(subprocess.check_output(['python','-m','jsmin'], input=p.read_text(encoding='utf-8'), text=True), encoding='utf-8') for p in src.rglob('*.js') ]"
