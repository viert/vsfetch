from argparse import ArgumentParser
from vsfetch.dynamic import loop
from vsfetch.ctx import ctx


def run():
    loop()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", default="/etc/vsfetch/vsfetch.toml", help="config file to use")
    args = parser.parse_args()
    ctx.set_config_filename(args.config)
    run()
