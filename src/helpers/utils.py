

@staticmethod
def to_dict(obj):
    if isinstance(obj, dict):
        return obj
    else:
        try:
            return vars(obj)
        except TypeError:
            return {}