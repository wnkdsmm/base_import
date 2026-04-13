"""ML service package.

The package intentionally avoids eager star-imports so submodules like
``app.services.ml_model.ml_model_types`` (and compatibility wrappers like
``app.services.ml_model.constants``) can be imported without pulling in the
full forecasting/ML runtime and creating circular imports.
"""
