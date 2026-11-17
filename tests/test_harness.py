"""The harness's own housekeeping: which leftover Postgres containers a session may remove."""

import os

from conftest import alive, stale_containers


def test_a_container_whose_process_has_gone_is_stale():
    listing = "agb-test-w7-201-4242\t4242\n"
    assert stale_containers(listing, running=lambda pid: False) == ["agb-test-w7-201-4242"]


def test_a_container_whose_process_is_running_is_kept():
    listing = "agb-test-w7-201-4242\t4242\nagb-test-w5-211-99\t99\n"
    assert stale_containers(listing, running=lambda pid: pid == 99) == ["agb-test-w7-201-4242"]


def test_a_container_that_is_not_ours_is_never_listed():
    listing = "agb-trader-ui-1\t\nagb-trader-db-1\t\nsomeone-else-12\t12\n"
    assert stale_containers(listing, running=lambda pid: False) == []


def test_a_container_from_before_the_pid_label_is_judged_by_its_name():
    listing = "agb-test-w6-188-777\t\nagb-test-w6-188-notapid\t\n"
    assert stale_containers(listing, running=lambda pid: False) == ["agb-test-w6-188-777"]


def test_a_process_is_alive_until_it_exits():
    assert alive(os.getpid())
    assert not alive(2**22 + 1)
