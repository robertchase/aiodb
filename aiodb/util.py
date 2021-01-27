"""utilities"""
import importlib


def import_by_path(target):
    """import code by dot-separated path"""
    if isinstance(target, str):
        modnam, funnam = target.rsplit('.', 1)
        mod = importlib.import_module(modnam)
        return getattr(mod, funnam)
    return target


def snake_to_camel(name):
    """convert a snake-cased string to camel-cased"""
    return name[0].lower() + "".join(
        "_" + ch.lower() if ch.isupper() else ch
        for ch in name[1:])
