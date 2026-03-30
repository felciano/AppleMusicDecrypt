import argparse
import asyncio
import sys

from creart import add_creator

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from src.logger import LoggerCreator
add_creator(LoggerCreator)
from src.config import ConfigCreator
add_creator(ConfigCreator)
from src.api import APICreator
add_creator(APICreator)
from src.grpc.manager import WMCreator
add_creator(WMCreator)
from src.measurer import MeasurerCreator
add_creator(MeasurerCreator)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Apple Music Decryption Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (default when no URLs provided)
  python main.py

  # Download a single album
  python main.py https://music.apple.com/album/...

  # Download multiple URLs
  python main.py https://music.apple.com/album/1 https://music.apple.com/album/2

  # Download from a file containing URLs
  python main.py -i urls.txt

  # Download with specific codec
  python main.py -c aac https://music.apple.com/album/...

  # Force interactive mode
  python main.py --interactive
        """
    )

    parser.add_argument(
        'urls',
        nargs='*',
        help='Apple Music URLs to download'
    )
    parser.add_argument(
        '-i', '--input-file',
        help='File containing URLs (one per line, # for comments)'
    )
    parser.add_argument(
        '-c', '--codec',
        choices=['alac', 'ec3', 'aac', 'aac-binaural', 'aac-downmix', 'aac-legacy', 'ac3'],
        default='alac',
        help='Audio codec (default: alac)'
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Force overwrite existing files'
    )
    parser.add_argument(
        '-l', '--language',
        help='Metadata language (e.g., en-US, ja, zh-Hans-CN)'
    )
    parser.add_argument(
        '--include-participate-songs',
        action='store_true',
        dest='include_participate',
        help='Include songs the artist participated in (for artist URLs)'
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Force interactive mode even if URLs are provided'
    )
    parser.add_argument(
        '--queue-delay',
        type=float,
        default=None,
        dest='queue_delay',
        help='Delay in seconds between queuing URLs (default: from config, use 0 for no delay)'
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Determine mode: interactive vs batch
    has_urls = bool(args.urls) or bool(args.input_file)
    use_interactive = args.interactive or not has_urls

    if use_interactive:
        # Interactive mode
        from src.cmd import InteractiveShell
        cmd = InteractiveShell(loop)
        try:
            loop.run_until_complete(cmd.start())
        except KeyboardInterrupt:
            loop.stop()
    else:
        # Batch mode
        from src.cli import run_batch
        exit_code = loop.run_until_complete(
            run_batch(
                urls=args.urls,
                input_file=args.input_file,
                codec=args.codec,
                force=args.force,
                language=args.language,
                include_participate=args.include_participate,
                queue_delay=args.queue_delay,
            )
        )
        sys.exit(exit_code)


if __name__ == '__main__':
    main()
