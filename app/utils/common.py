def split_csv_to_list(val: str, default: list[str] | None = None) -> list[str]:
    """Splits a comma-separated string into a list of strings.

    Strips whitespace from each item.
    If the string is empty or None, returns the default list (defaulting to ["*"]).
    """
    if default is None:
        default = ["*"]

    if not val:
        return default

    return [item.strip() for item in val.split(",")]
