from os import getenv


_HIGH = {getenv('MY_ID')}
_MED = {getenv('JILL_ID')}

HIGH_CLEARANCE = 2
MEDIUM_CLEARANCE = 1


async def check_perms(uid: str, clearance: int) -> bool:
    """Return whether an interaction user ID is in a sequence of IDs."""
    allowed = set()
    if clearance <= HIGH_CLEARANCE:
        allowed.update(_HIGH)
    if clearance <= MEDIUM_CLEARANCE:
        allowed.update(_MED)

    return uid in allowed
