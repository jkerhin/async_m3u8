from pathlib import Path

__all__ = ["get_url_list", "log_fmt"]

log_fmt: str = "%(asctime)s [%(name)s]: [%(levelname)s]: {%(taskName)s}: %(message)s"


def parse_master_list():
    raise NotImplementedError


def get_url_list(fname: str | Path) -> list[str]:
    """Very janky, right now just looks for http(s) lines

    Haven't even looked at m3u8 file format/definition yet, revisit as needed
    """
    lines = Path(fname).read_text().splitlines()
    return [ln for ln in lines if ln.startswith("http")]
