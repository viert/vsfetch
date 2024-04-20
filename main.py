from argparse import ArgumentParser
from vsfetch.dynamic import loop
from vsfetch.config import init_config


def run():
    loop()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", default="/etc/vsfetch/vsfetch.toml", help="config file to use")
    args = parser.parse_args()
    init_config(args.config)
    run()
