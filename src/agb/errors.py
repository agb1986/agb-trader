"""Errors shared across modules."""


class NotConfigured(ValueError):
    """A setting the work needs is missing or empty, such as an API key or a User-Agent that
    belongs in the service's env file. Nothing in the data is wrong, and the same work succeeds
    once the setting is supplied and the service restarted, so the pipeline holds a job that
    raises it rather than failing it (agb.services.pipeline)."""
