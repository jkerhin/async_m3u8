from pathlib import Path

__all__ = ["get_url_list", "log_fmt"]

log_fmt: str = "%(asctime)s [%(name)s]: [%(levelname)s]: {%(taskName)s}: %(message)s"


def get_url_list(fname: str = "low.m3u8") -> list[str]:
    """Very janky"""
    lines = Path(fname).read_text().splitlines()
    return [ln for ln in lines if ln.startswith("http")]
