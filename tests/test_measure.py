"""The memory sampler: criterion (a) of T32, the measurement the limits are derived from.

Docker is not available in these tests and the sampler is deliberately built to be asked
through one injected `run`, so the readings here are made up but their shape is docker's own:
tab-separated lines, binary units, a `used / allowed` pair per container.
"""

import pytest

from agb import measure
from agb.measure import MIB, MeasureError, Peaks, format_size, parse_size, suggested

PROJECT = "agb-trader"


class FakeDocker:
    """Docker answering `docker ps` and `docker stats` from a list of readings per call."""

    def __init__(self, containers, readings):
        self.containers = containers
        self.readings = list(readings)
        self.asked = []

    def __call__(self, argv):
        self.asked.append(list(argv))
        if argv[1] == "ps":
            return "".join(f"{name}\t{service}\n" for name, service in self.containers.items())
        wanted = argv[5:]
        current = self.readings.pop(0) if len(self.readings) > 1 else self.readings[0]
        return "".join(f"{name}\t{current[name]}\n" for name in wanted if name in current)


# Reading what docker prints.


@pytest.mark.parametrize(
    ("text", "bytes_"),
    [
        ("0B", 0),
        ("412.1MiB", 432118169),
        ("2GiB", 2 * 1024 * MIB),
        ("1.5GB", 1_500_000_000),  # docker prints decimal units outside memory
        (" 64MiB ", 64 * MIB),
        ("128", 128),
    ],
)
def test_a_docker_size_is_read_as_the_unit_it_says(text, bytes_):
    assert parse_size(text) == bytes_


@pytest.mark.parametrize("text", ["", "MiB", "a lot", "12 parsecs"])
def test_a_size_that_cannot_be_read_is_an_error_not_a_guess(text):
    with pytest.raises(MeasureError):
        parse_size(text)


# The limit a measurement argues for.


@pytest.mark.parametrize(
    ("peak", "limit"),
    [
        (0, "64m"),  # the floor alone
        (50 * MIB, "128m"),  # 50 + 64 = 114, up to the next 32 MiB step
        (300 * MIB, "480m"),  # headroom of half again, already on a step
        (2048 * MIB, "3g"),  # a whole number of GiB is written as GiB
    ],
)
def test_a_limit_is_the_peak_plus_headroom_rounded_up(peak, limit):
    assert format_size(suggested(peak)) == limit


def test_a_suggested_limit_is_never_below_the_peak_it_came_from():
    for peak in range(0, 4096 * MIB, 97 * MIB):
        assert suggested(peak) >= peak + measure.FLOOR


# Sampling.


def test_the_peak_is_the_highest_reading_not_the_last():
    docker = FakeDocker(
        {"agb-trader-pipeline-1": "pipeline"},
        [
            {"agb-trader-pipeline-1": "300MiB / 2GiB"},
            {"agb-trader-pipeline-1": "1.2GiB / 2GiB"},
            {"agb-trader-pipeline-1": "180MiB / 2GiB"},
        ],
    )
    peaks = Peaks()
    for _ in range(3):
        measure.sample(peaks, docker, PROJECT)

    [row] = peaks.rows()
    assert (row.service, row.container) == ("pipeline", "agb-trader-pipeline-1")
    assert (row.peak, row.limit, row.samples) == (int(1.2 * 1024 * MIB), 2048 * MIB, 3)


def test_each_service_is_tracked_apart_and_the_table_names_them_all():
    docker = FakeDocker(
        {"agb-trader-db-1": "db", "agb-trader-pipeline-1": "pipeline"},
        [{"agb-trader-db-1": "50MiB / 512MiB", "agb-trader-pipeline-1": "900MiB / 2GiB"}],
    )
    peaks = Peaks()
    measure.sample(peaks, docker, PROJECT)

    assert [row.service for row in peaks.rows()] == ["db", "pipeline"]
    table = peaks.table()
    assert "| db | agb-trader-db-1 | 1 | 50m | 512m | 128m |" in table
    assert "| pipeline | agb-trader-pipeline-1 | 1 | 900m | 2g | 1376m |" in table


def test_only_this_project_is_watched():
    docker = FakeDocker({"agb-trader-db-1": "db"}, [{"agb-trader-db-1": "50MiB / 512MiB"}])

    measure.sample(Peaks(), docker, PROJECT)

    ps, stats = docker.asked
    assert f"label=com.docker.compose.project={PROJECT}" in ps
    # The household stack's seventeen containers share the server; only ours are read.
    assert stats[-1:] == ["agb-trader-db-1"]


def test_a_container_with_no_compose_label_is_named_by_its_container_name():
    docker = FakeDocker({"stray": ""}, [{"stray": "10MiB / 0B"}])
    peaks = Peaks()

    measure.sample(peaks, docker, PROJECT)

    [row] = peaks.rows()
    assert (row.service, row.container) == ("stray", "stray")


def test_nothing_running_is_no_readings_and_no_crash():
    docker = FakeDocker({}, [{}])
    peaks = Peaks()

    assert measure.sample(peaks, docker, PROJECT) == {}
    assert peaks.rows() == []


def test_a_run_with_no_readings_reports_nothing_rather_than_an_empty_table(capsys, monkeypatch):
    monkeypatch.setattr(measure, "run_command", FakeDocker({}, [{}]))

    assert measure.main(["--samples", "1", "--interval", "0"]) == 1
    assert "no readings" in capsys.readouterr().err


def test_the_table_is_written_where_the_operator_asked_for_it(tmp_path, monkeypatch):
    docker = FakeDocker({"agb-trader-db-1": "db"}, [{"agb-trader-db-1": "50MiB / 512MiB"}])
    monkeypatch.setattr(measure, "run_command", docker)
    out = tmp_path / "peaks.md"

    assert measure.main(["--samples", "2", "--interval", "0", "--out", str(out)]) == 0

    written = out.read_text()
    assert written.startswith("| service | container | samples |")
    assert "| db | agb-trader-db-1 | 2 | 50m | 512m | 128m |" in written


def test_docker_failing_is_reported_and_not_sampled_over(capsys, monkeypatch):
    def broken(argv):
        raise MeasureError("docker is not on the path")

    monkeypatch.setattr(measure, "run_command", broken)

    assert measure.main(["--samples", "3", "--interval", "0"]) == 1
    assert "measure: docker is not on the path" in capsys.readouterr().err
