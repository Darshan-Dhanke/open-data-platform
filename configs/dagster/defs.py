from dagster import asset, Definitions


@asset
def hello_platform() -> str:
    """A trivial asset proving the Dagster code location loads."""
    return "hello from the open data platform"


defs = Definitions(assets=[hello_platform])
