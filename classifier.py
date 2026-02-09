def classify(stats):
    """
    Decide storage backend for each field
    stats: output from Analyzer.get_stats()
    returns: dict {field_name: 'sql' or 'mongo'}
    """
    decisions = {}
    for field, s in stats.items():
        if s["nested"]:
            decisions[field] = "mongo"
        elif s["freq"] > 0.6 and len(s["types"]) == 1:
            decisions[field] = "sql"
        else:
            decisions[field] = "mongo"
    return decisions

