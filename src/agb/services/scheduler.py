"""The scheduler service. For now it only proves liveness by beating its heartbeat."""

import logging

from agb.heartbeat import interval_from_env, serve, stop_on_signals


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    serve("scheduler", interval_from_env(), stop_on_signals())


if __name__ == "__main__":
    main()
