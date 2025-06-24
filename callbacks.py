import time
from typing import Callable, Optional, TypeAlias

CallbackId: TypeAlias = int

Callback: TypeAlias = tuple[float, float, Callable[[], None], bool]
"""A start time, a timeout, and a function to call when the timeout is hit"""

_curr_id = 0

_callbacks: dict[CallbackId, Callback] = {}


def register(
    callback: Callable[[], None], timeout: float, repeat: bool = False
) -> CallbackId:
    """Register a new callback

    # Parameters
    - `callback` - The function to call. If it returns True, the callback won't be removed
                   after it's run.
    - `timeout` - The timeout before triggering the callback, in seconds"""
    global _curr_id

    _curr_id += 1
    _callbacks[_curr_id] = (time.time(), timeout, callback, repeat)
    return _curr_id


def remove(id: CallbackId):
    del _callbacks[id]


def update():
    """Call and remove callbacks that have hit their timeout"""
    now = time.time()

    for id in list(_callbacks.keys()):
        start, timeout, callback, repeat = _callbacks[id]

        if now - start > timeout:
            # todo should we catch errors?
            if not repeat:
                del _callbacks[id]
            else:
                _callbacks[id] = now, timeout, callback, repeat
            callback()
