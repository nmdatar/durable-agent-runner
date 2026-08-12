"""Failure categories understood by the durable runner."""


class RetryableStepError(RuntimeError):
    """A transient failure that may succeed on a later attempt."""


class TerminalStepError(RuntimeError):
    """A permanent failure that should fail the run immediately."""

