from web_config.app_settings import FILTER_PARAM_NAMES


def _normalize_filter_value(value):
    normalized = str(value or "").strip()
    return normalized or None


def build_filter_cache_key(filter_params):
    raw_filters = filter_params or {}
    return tuple(
        _normalize_filter_value(raw_filters.get(filter_name))
        for filter_name in FILTER_PARAM_NAMES
    )
