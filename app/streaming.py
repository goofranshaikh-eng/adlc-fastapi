"""SSE response that cancels the local subscription when the client disconnects.

Starlette buffers nothing, but a blocked agent subscription will not notice a
disconnect until the next event. Listening for ``http.disconnect`` cancels that
subscription so the handler's cleanup still runs.

Disconnect closes this process's subscription and drops the local registry
mapping. It does not cancel the agent run; the agent's background job
continues.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from functools import partial
from typing import Any

import anyio
from fastapi.responses import StreamingResponse

_StreamCall = Callable[[], Coroutine[Any, Any, None]]


@asynccontextmanager
async def _task_group():
    """Run sibling tasks and surface a single child error unchanged."""
    try:
        async with anyio.create_task_group() as task_group:
            yield task_group
    except BaseExceptionGroup as group:
        if len(group.exceptions) != 1:
            raise
        exc = group.exceptions[0]
        raise exc from exc.__cause__ or exc.__context__


async def _finish_then_cancel(task_group: anyio.abc.TaskGroup, func: _StreamCall) -> None:
    await func()
    task_group.cancel_scope.cancel()


async def _aclose(iterator: object) -> None:
    close = getattr(iterator, "aclose", None)
    if close is not None:
        await close()


class SSEResponse(StreamingResponse):
    """`text/event-stream` body that closes its generator on client disconnect.

    Cancellation stops the local agent subscription only. The agent run is
    left in place, and registry cleanup removes only the local id mapping.
    """

    def __init__(self, content: AsyncIterator[str]) -> None:
        super().__init__(
            content,
            status_code=200,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        # ``http.disconnect`` cancels the local subscription. Cleanup then
        # removes the registry entry. The agent run itself keeps running.
        try:
            async with _task_group() as task_group:
                task_group.start_soon(
                    _finish_then_cancel,
                    task_group,
                    partial(self.stream_response, send),
                )
                task_group.start_soon(
                    _finish_then_cancel,
                    task_group,
                    partial(self.listen_for_disconnect, receive),
                )
        finally:
            with anyio.CancelScope(shield=True):
                await _aclose(self.body_iterator)
            if self.background is not None:
                await self.background()
