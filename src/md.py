from typing import Any, Iterable, Mapping


"""Discord markdown syntax."""


HEADER_BIG = '#'
HEADER_SMALL = '##'
HEADER_SMALLER = '###'


def escape(s: Any, /) -> str:
    """Return escaped text such that markdown will not be applied."""
    return f'\\{s}'

def italic(s: Any, /) -> str:
    """Return italicized text."""
    return f'*{s}*'

def bold(s: Any, /) -> str:
    """Return bolded text."""
    return f'**{s}**'

def underline(s: Any, /) -> str:
    """Return underlined text."""
    return f'__{s}__'

def strike(s: Any, /) -> str:
    """Return strikethrough text."""
    return f'~~{s}~~'

def header(s: Any, /, size: str) -> str:
    """Return header text.

    Parameters:
        size (str):
            How big the header should be. Use the included constants:
                HEADER_BIG
                HEADER_SMALL
                HEADER_SMALLER
    """
    valid = (HEADER_BIG, HEADER_SMALL, HEADER_SMALLER)
    if size not in valid:
        raise ValueError(f'Invalid size: "{s}"\nValid: {', '.join(valid)}')
    return f'{size} {s}'

def subtext(s: Any, /) -> str:
    """Return subtext text."""
    return f'-# {s}'

def hyperlink(s: Any, /, url: str, *, embed: bool = False) -> str:
    """Return hyperlinked text.

    Parameters:
        url (str):
            Link to mask.
        embed (bool):
            Whether the link should be embedded in the message.
    """
    if not embed:
        url = f'<{url}>'
    return f'[{s}]({url})'

def format_list(s: Any, /, ordered: bool = False, *,
                start: int = 1, indent: int = 0) -> str:
    """Return text formatted as a bulleted list.

    ## Strings:
        Each line is treated as an item in the list
        and indented based on the number of leading spaces.

    ## Iterables:
        Each element is treated as an item in the list
        and indented based on the iterable nesting level.
        WARNING: Nesting may not format as intended.

    ## Mappings:
        Each key-value pair is treated as an item in the list with
        the key being bolded and indented based on the nesting level.
        WARNING: Nesting may not format as intended.

    Parameters:
        ordered (bool):
            If `True`, will generate a numbered list.
            If `False`, will generate an unordered list. (Default)
        start (int):
            Starting value for numbered lists.
        indent (int):
            Starting indentation level.
    """
    def count_leading_spaces(line: str) -> int:
        return len(line) - len(line.lstrip(' '))
    def bullet(i: int) -> str:
        return f'{i}.' if ordered else '-'

    prefix = ' '*indent

    # String input
    if isinstance(s, (str, bytes)):
        lines = []
        for n, line in enumerate(s.splitlines(), start=start):
            if not line.strip():
                continue
            lines.append(
                f'{prefix}{' '*count_leading_spaces(line)}'
                f'{bullet(n)} {line.strip()}'
            )
        return '\n'.join(lines)

    # Mapping input
    if isinstance(s, Mapping):
        lines = []
        for n, (k, v) in enumerate(s.items(), start=start):
            head = f'{prefix}{bullet(n)} {bold(k)}'
            if (isinstance(v, (Mapping, Iterable)) and
                not isinstance(v, (str, bytes))):
                nested = format_list(
                    v, ordered=ordered, start=start, indent=indent+1
                )
                lines.append(f'{head}:\n{nested}')
            else:
                lines.append(f'{head}: {v}')
        return '\n'.join(lines)

    # Iterable input
    if isinstance(s, Iterable):
        lines = []
        for n, item in enumerate(s, start=start):
            if (isinstance(item, (Mapping, Iterable)) and
                not isinstance(item, (str, bytes))):
                nested = format_list(
                    item, ordered=ordered, start=1, indent=indent+1
                )
                lines.append(f'{prefix}{bullet(n)} {nested}')
            else:
                lines.append(f'{prefix}{bullet(n)} {item}')
        return '\n'.join(lines)

    # Fallback
    return f'{prefix}{bullet(start)} {s}'

def monospace(s: Any, /) -> str:
    """Return monospace text."""
    return f'`{s}`'

def code(s: Any, /, lang: str = '') -> str:
    """Return code block text.

    Parameters:
        lang (str, optional):
            Language to use for syntax highlighting.
    """
    return f'```{lang}\n{s}```'

def quote(s: Any, /) -> str:
    """Return quoted text."""
    return f'>>> {s}'

def spoiler(s: Any, /) -> str:
    """Return spoiler text."""
    return f'||{s}||'
