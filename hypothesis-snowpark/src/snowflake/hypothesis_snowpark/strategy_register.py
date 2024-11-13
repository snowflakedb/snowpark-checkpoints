from hypothesis.strategies import composite

snowpark_strategies = {}


def register(*args):
    """
    Register a function into the snowpark_strategies dictionary and also composed it as a SearchStrategy
    :param args: It's the key we want to register in the dictionary.
    :return: the composited function.
    """
    def wrapper(func):
        if len(args) > 0:
            composed_func = composite(func)
            snowpark_strategies[args[0]] = composed_func()
        else:
            composed_func = func
        return composed_func

    return wrapper
