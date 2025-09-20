"""Baseline - synch code

On trial of 100 files
2025-09-20 11:57:15,765 [root]: [INFO]: {None}: Downloaded 37950432 bytes in 5.416216 seconds
"""  # noqa: E501

import io
import logging
import time

import httpx

from async_m3u8.common import get_url_list, log_fmt


def main():
    url_list = get_url_list("low.m3u8")

    # For testing purposes, truncate to first 100
    url_list = url_list[:100]

    client = httpx.Client(http2=True, follow_redirects=True)
    chunks: dict[int, bytes] = {}

    logging.info("Playlist contains %d urls", len(url_list))
    logging.info("Beginning download")
    t0 = time.monotonic()
    for counter, url in enumerate(url_list):
        r = client.get(url)
        r.raise_for_status()
        chunks[counter] = r.content
    t1 = time.monotonic()
    logging.info("Download complete")
    dt = t1 - t0

    fakefile = io.BytesIO()
    for key in sorted(chunks):
        fakefile.write(chunks[key])

    logging.info("Downloaded %d bytes in %f seconds", fakefile.tell(), dt)


if __name__ == "__main__":
    logging.basicConfig(format=log_fmt, level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    main()
