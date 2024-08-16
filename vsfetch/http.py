import aiohttp
from typing import Dict, Any, Optional
from vsfetch.ctx import ctx


class HTTPRequestError(Exception):
    pass


async def get_json(url: str, *, timeout: Optional[int] = None) -> Dict[str, Any]:
    if timeout is None:
        timeout = ctx.cfg.external.timeout

    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, timeout=timeout) as resp:
            if resp.status >= 300:
                text = await resp.text()
                raise HTTPRequestError(f"unsuccessful status {resp.status} from {url}, response is {text}")
            return await resp.json(content_type=None)


async def delete_json(url: str, data: Optional[Dict[str, Any]] = None, *, timeout: Optional[int] = None) -> Dict[str, Any]:
    if timeout is None:
        timeout = ctx.cfg.external.timeout

    async with aiohttp.ClientSession() as sess:
        kwargs = {"timeout": timeout}
        if data is not None:
            kwargs["json"] = data

        async with sess.delete(url, **kwargs) as resp:
            if resp.status >= 300:
                text = await resp.text()
                raise HTTPRequestError(f"unsuccessful status {resp.status} from {url}, response is {text}")
            return await resp.json(content_type=None)


async def post_json(url: str, data: Dict[str, Any], *, timeout: Optional[int] = None) -> Dict[str, Any]:
    if timeout is None:
        timeout = ctx.cfg.external.timeout

    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, json=data, timeout=timeout) as resp:
            if resp.status >= 300:
                text = await resp.text()
                raise HTTPRequestError(f"unsuccessful status {resp.status} from {url}, response is {text}")
            return await resp.json(content_type=None)
