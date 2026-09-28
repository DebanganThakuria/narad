"""Signal Line build hook: word-boundary break points for codes in tables.

Reference tables keep every code on one line (see extra.css, section 7).
On phones the pinned first column lets its code wrap instead, so the
column cannot grow wider than the screen. A name like
NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION has no break point of
its own, so it would either run the pinned column off the screen or split
at an arbitrary letter. This adds <wbr> after each underscore, dot, slash
and hyphen in code at the start of a table cell, so it wraps at a word
boundary. <wbr> draws nothing, is not copied with the text, and does
nothing where the code is set nowrap. Code that starts a new line of a
cell (after a <br>, as in a Setting cell holding a key and then its
environment variable) gets the same break points.
"""

import re

_CELL_CODE = re.compile(r"((?:<t[dh][^>]*>|<br\s*/?>)\s*<code>)([^<]+)(</code>)")
_BREAK_AFTER = re.compile(r"([_./-])(?=[^_./-])")


def _add_breaks(match: "re.Match[str]") -> str:
    head, text, tail = match.groups()
    return head + _BREAK_AFTER.sub(r"\1<wbr>", text) + tail


def on_page_content(html: str, **kwargs) -> str:
    return _CELL_CODE.sub(_add_breaks, html)
