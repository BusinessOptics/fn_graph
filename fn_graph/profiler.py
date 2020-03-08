import time


def now():
    return time.perf_counter()


class Profiler:
    def __init__(self):
        self.start = {}
        self.end = {}

    def __call__(self, event_type, details):
        name = details.get("name")
        instruction = {
            "start_calculation": ("start", "calculation", "preparation"),
            "prepared_calculation": ("end", "calculation", "preparation"),
            "start_step": ("start", "step", name),
            "end_step": ("end", "step", name),
            "start_function": ("start", "execution", name),
            "end_function": ("end", "execution", name),
            "start_cache_retrieval": ("start", "cache_retrieval", name),
            "end_cache_retrieval": ("end", "cache_retrieval", name),
            "start_cache_store": ("start", "cache_store", name),
            "end_cache_store": ("end", "cache_store", name),
        }.get(event_type)

        if instruction:
            event, category, name = instruction
            store = getattr(self, event)
            store[(category, name)] = now()

    def results(self):
        def period(key):
            return self.end.get(key, 0) - self.start.get(key, 0)

        def function_profile(name):
            total = period(("step", name))
            execution = period(("execution", name))
            retrieval = period(("cache_retrieval", name))
            store = period(("cache_store", name))

            overhead = total - execution - retrieval - store
            return dict(
                total=total,
                overhead=overhead,
                execution=execution,
                cache_retrieval=retrieval,
                cache_store=store,
            )

        names = [name for event, name in self.start.keys() if event == "step"]

        return dict(
            startup=dict(
                preparation=dict(
                    preparation=period(("calculation", "preparation")),
                    total=period(("calculation", "preparation")),
                )
            ),
            functions={name: function_profile(name) for name in names},
        )
