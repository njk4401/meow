from os import getenv

from nextcord import Interaction
from nextcord.ext import commands


_HIGH = {getenv('MY_ID')}
_MED = {getenv('JILL_ID')}

HIGH_CLEARANCE = 2
MEDIUM_CLEARANCE = 1


def check_perms(uid: str, clearance: int) -> bool:
    """Return whether an interaction user ID is in a sequence of IDs."""
    allowed = set()
    if clearance <= HIGH_CLEARANCE:
        allowed.update(_HIGH)
    if clearance <= MEDIUM_CLEARANCE:
        allowed.update(_MED)

    return uid in allowed


def has_clearance(lvl: int):
    """Decorator for application commands, checking whether a
    user has sufficient clearance before execution.
    """
    async def predicate(interaction: Interaction) -> bool:
        allowed = check_perms(str(interaction.user.id), lvl)
        if not allowed:
            await interaction.response.send_message(
                "You don't have permission to use this command",
                ephemeral=True
            )
        return allowed
    return commands.check(predicate)
