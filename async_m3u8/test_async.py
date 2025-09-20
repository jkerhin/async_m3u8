"""Async

2025-09-20 12:57:04,337 [root]: [INFO]: {Task-1}: Populated URL queue with 100 urls
2025-09-20 12:57:04,337 [root]: [INFO]: {Task-1}: Beginning download with 5 workers
2025-09-20 12:57:06,399 [root]: [INFO]: {Task-1}: Download complete
2025-09-20 12:57:06,400 [root]: [INFO]: {Task-1}: Dowloaded 37950432 bytes in 2.062112 seconds

2025-09-20 12:58:01,752 [root]: [INFO]: {Task-1}: Populated URL queue with 100 urls
2025-09-20 12:58:01,752 [root]: [INFO]: {Task-1}: Beginning download with 10 workers
2025-09-20 12:58:03,466 [root]: [INFO]: {Task-1}: Download complete
2025-09-20 12:58:03,467 [root]: [INFO]: {Task-1}: Dowloaded 37950432 bytes in 1.714204 seconds
"""  # noqa: E501

import asyncio
import io
import logging
import time

import httpx

from async_m3u8.common import get_url_list, log_fmt

N_WORKERS: int = 10


class DownloadWorker:
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
            r = await self.httpx_client.get(url)
            # Error handling would go here
            r.raise_for_status()

            await self.result_queue.put((counter, r.content))


async def reassemble(queue: asyncio.Queue, n_chunks: int) -> io.BytesIO:
    fakefile = io.BytesIO()
    counter = 0
    local_chunks: dict[int, bytes] = {}

    while counter < n_chunks:
        item: tuple[int, bytes] = await queue.get()
        if item[0] == counter:
            fakefile.write(item[1])
            counter += 1
        else:
            local_chunks[item[0]] = item[1]

        for _ in range(len(local_chunks)):
            if counter in local_chunks:
                chunk = local_chunks.pop(counter)
                fakefile.write(chunk)
                counter += 1
    return fakefile


async def populate_queue(urls: list[str], url_queue: asyncio.Queue):
    for u in urls:
        await url_queue.put(u)


async def main():
    url_list = get_url_list()

    # For testing purposes, truncate to first 100
    url_list = url_list[:100]

    client = httpx.AsyncClient(http2=True, follow_redirects=True)
    url_queue = asyncio.Queue()
    result_queue = asyncio.Queue()

    for counter, url in enumerate(url_list):
        await url_queue.put((counter, url))
    logging.info("Populated URL queue with %d urls", len(url_list))

    logging.info("Beginning download with %d workers", N_WORKERS)
    t0 = time.monotonic()
    async with asyncio.TaskGroup() as tg:
        for n in range(N_WORKERS):
            worker = DownloadWorker(
                httpx_client=client, url_queue=url_queue, result_queue=result_queue
            )
            tg.create_task(worker.run(), name=f"DownloadWorker {n}")

        t_reassemble = tg.create_task(
            reassemble(queue=result_queue, n_chunks=len(url_list)), name="reassemble"
        )

    t1 = time.monotonic()
    dt = t1 - t0
    logging.info("Download complete")
    fakefile = t_reassemble.result()
    logging.info("Dowloaded %d bytes in %f seconds", fakefile.tell(), dt)


if __name__ == "__main__":
    logging.basicConfig(format=log_fmt, level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(main())
