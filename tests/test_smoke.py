from durable_agent_runner import __version__
from durable_agent_runner.cli import main


def test_package_version() -> None:
    assert __version__ == "0.1.0"


def test_demo_cli(capsys) -> None:
    assert main(["demo"]) == 0
    output = capsys.readouterr().out
    assert "COMPLETED publish" in output
    assert "PUBLISHED publication-1" in output
