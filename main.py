import asyncio
from argparse import ArgumentParser
from vsfetch.dynamic import loop
from vsfetch.ctx import ctx
from mongey.context import ctx as mongey_ctx


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", default="/etc/vsfetch/vsfetch.toml", help="config file to use")
    args = parser.parse_args()
    ctx.set_config_filename(args.config)
    mongey_ctx.setup_db({"meta": ctx.cfg.database.model_dump(), "shards": {}})
    asyncio.run(loop())
