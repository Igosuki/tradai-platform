async def run_in_loop(fn, *args, **kwargs):
    return await fn(*args, **kwargs)
