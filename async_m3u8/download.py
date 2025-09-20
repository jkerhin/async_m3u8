import asyncio
import logging
import time
from pathlib import Path

import aiofiles
import click
import httpx

from async_m3u8.common import get_url_list, log_fmt

N_WORKERS: int = 5
SLEEP_SECONDS_ON_429: float = 2.0
NOT_IN_COOLDOWN = asyncio.Event()
log = logging.getLogger(__name__)


class DownloadWorker:
    # TODO: Should COOLDOWN be defined here?

    def __init__(
        self,
        httpx_client: httpx.AsyncClient,
        url_queue: asyncio.Queue,
        result_queue: asyncio.Queue,
    ):
        self.httpx_client = httpx_client
        self.url_queue = url_queue
        self.result_queue = result_queue

    async def run(self):
        while not self.url_queue.empty():
            counter, url = await self.url_queue.get()
            content = await self._download(url=url)

            await self.result_queue.put((counter, content))

    async def _download(self, url) -> bytes:
        errors_seen = 0
        while True:
            await NOT_IN_COOLDOWN.wait()
            try:
                r = await self.httpx_client.get(url=url)
                r.raise_for_status()
                return r.content
            except httpx.HTTPStatusError as e:
                log.error("Status error attempting to fetch %s - %s", url, str(e))
                status_code: int = e.response.status_code
                if status_code == 429:
                    NOT_IN_COOLDOWN.clear()
                    log.debug(
                        "Sleeping for %0.1f seconds after 429 - %s",
                        SLEEP_SECONDS_ON_429,
                        r.url,
                    )
                    await asyncio.sleep(SLEEP_SECONDS_ON_429)
                    NOT_IN_COOLDOWN.set()
                elif status_code in (502, 503):
                    # Server errors need much longer wait than 429
                    NOT_IN_COOLDOWN.clear()
                    sleep_sec = SLEEP_SECONDS_ON_429 * 10
                    log.debug(
                        "Sleeping for %0.1f seconds after %d - %s",
                        sleep_sec,
                        status_code,
                        r.url,
                    )
                    await asyncio.sleep(sleep_sec)
                    NOT_IN_COOLDOWN.set()
                else:
                    # Any other handling goes here...
                    errors_seen += 1
            except Exception as e:
                log.error(
                    "Non-HTTP error attepting to fetch %s - %s",
                    url,
                    str(e),
                )
                errors_seen += 1
            finally:
                if errors_seen > 5:
                    raise RuntimeError("Too many errors trying to fetch %s", self.url)


async def reassemble(queue: asyncio.Queue, n_chunks: int, out_file: Path) -> int:
    async with aiofiles.open(out_file, "wb") as hdl:
        counter = 0
        local_chunks: dict[int, bytes] = {}

        while counter < n_chunks:
            item: tuple[int, bytes] = await queue.get()
            if item[0] == counter:
                await hdl.write(item[1])
                counter += 1
            else:
                local_chunks[item[0]] = item[1]

            for _ in range(len(local_chunks)):
                if counter in local_chunks:
                    chunk = local_chunks.pop(counter)
                    await hdl.write(chunk)
                    counter += 1

        bytes_written = await hdl.tell()
    return bytes_written


async def populate_queue(urls: list[str], url_queue: asyncio.Queue):
    for u in urls:
        await url_queue.put(u)


async def main(m3u8_file: Path, out_file: Path):
    url_list = get_url_list(m3u8_file)

    client = httpx.AsyncClient(http2=True, follow_redirects=True)
    url_queue = asyncio.Queue()
    result_queue = asyncio.Queue()
    NOT_IN_COOLDOWN.set()

    for counter, url in enumerate(url_list):
        await url_queue.put((counter, url))
    log.info("Populated URL queue with %d urls", len(url_list))

    log.info("Beginning download with %d workers", N_WORKERS)
    t0 = time.monotonic()
    async with asyncio.TaskGroup() as tg:
        for n in range(N_WORKERS):
            worker = DownloadWorker(
                httpx_client=client, url_queue=url_queue, result_queue=result_queue
            )
            tg.create_task(worker.run(), name=f"DownloadWorker {n}")

        t_reassemble = tg.create_task(
            reassemble(queue=result_queue, n_chunks=len(url_list), out_file=out_file),
            name="reassemble",
        )

    t1 = time.monotonic()
    dt = t1 - t0
    log.info("Download complete")
    bytes_written = t_reassemble.result()
    log.info("Dowloaded %d bytes in %f seconds", bytes_written, dt)


@click.command()
@click.option(
    "-o",
    "--out-file",
    type=click.Path(path_type=Path),
    help="Filname that output will be saved to",
)
@click.argument("m3u8_file", type=click.Path(exists=True, path_type=Path), nargs=1)
def cli(out_file: Path | None, m3u8_file: Path):
    logging.basicConfig(format=log_fmt, level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if not out_file:
        # FIXME: figure out how to name output file...
        out_name = m3u8_file.name.replace(m3u8_file.suffix, ".bin")
        out_file = m3u8_file.with_name(out_name)
    log.info("Will write to %s", str(out_file))
    asyncio.run(main(m3u8_file=m3u8_file, out_file=out_file))


if __name__ == "__main__":
    cli()
