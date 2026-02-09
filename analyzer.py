from collections import defaultdict

class Analyzer:
    def __init__(self):
        self.total = 0
        self.stats = defaultdict(lambda: {
            "count": 0,
            "types": set(),
            "unique": set(),
            "nested": False
        })

    def update(self, record):
        self.total += 1

        for k, v in record.items():
            s = self.stats[k]
            s["count"] += 1
            s["types"].add(type(v).__name__)
            s["unique"].add(str(v))

            if isinstance(v, (dict, list)):
                s["nested"] = True

    def get_stats(self):
        result = {}

        for k, s in self.stats.items():
            result[k] = {
                "freq": s["count"] / self.total,
                "types": s["types"],
                "unique_count": len(s["unique"]),
                "nested": s["nested"]
            }

        return result
