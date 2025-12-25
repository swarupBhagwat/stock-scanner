from scan.schema import RULE_DEFINITIONS

def validate_rule(rule: dict):
    if not isinstance(rule, dict):
        raise ValueError("Rule must be an object")

    # AND / OR
    if "AND" in rule or "OR" in rule:
        key = "AND" if "AND" in rule else "OR"
        rules = rule[key]

        if not isinstance(rules, list) or not rules:
            raise ValueError(f"{key} must be a non-empty list")

        for r in rules:
            validate_rule(r)
        return

    # Leaf rule
    if len(rule) != 1:
        raise ValueError(f"Invalid rule format: {rule}")

    name, cfg = next(iter(rule.items()))

    if name not in RULE_DEFINITIONS:
        raise ValueError(f"Unknown rule: {name}")

    if not isinstance(cfg, dict):
        raise ValueError(f"Rule config must be object for {name}")

    schema = RULE_DEFINITIONS[name]

    for k in schema["required"]:
        if k not in cfg:
            raise ValueError(f"Missing required '{k}' in {name}")

    for k in cfg:
        if k not in schema["required"] + schema["optional"]:
            raise ValueError(f"Invalid key '{k}' in {name}")
