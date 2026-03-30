"""
Non-interactive CLI mode for batch downloading.

Usage:
    uv run python main.py https://music.apple.com/album/...
    uv run python main.py -i urls.txt
    uv run python main.py -c aac https://music.apple.com/album/...
"""
import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.qemu import QemuInstance
    from src.rip import Ripper


@dataclass
class BatchStats:
    """Track statistics for batch downloads."""
    total_urls: int = 0
    songs_started: int = 0
    songs_success: int = 0
    songs_failed: int = 0
    songs_skipped: int = 0  # Already exists
    reconnect_count: int = 0
    failed_songs: list = field(default_factory=list)  # List of (name, error) tuples

    def print_summary(self):
        """Print a summary of the batch download results."""
        from creart import it
        from src.logger import GlobalLogger

        logger = it(GlobalLogger).logger

        logger.info("=" * 50)
        logger.info("BATCH DOWNLOAD SUMMARY")
        logger.info("=" * 50)
        logger.info(f"URLs processed:    {self.total_urls}")
        logger.info(f"Songs started:     {self.songs_started}")
        logger.info(f"Songs successful:  {self.songs_success}")
        logger.info(f"Songs skipped:     {self.songs_skipped} (already exist)")
        logger.info(f"Songs failed:      {self.songs_failed}")

        if self.reconnect_count > 0:
            logger.info(f"Stream reconnects: {self.reconnect_count}")

        if self.failed_songs:
            logger.info("-" * 50)
            logger.info("Failed songs:")
            for name, error in self.failed_songs[:20]:  # Limit to first 20
                logger.info(f"  - {name}: {error}")
            if len(self.failed_songs) > 20:
                logger.info(f"  ... and {len(self.failed_songs) - 20} more")

        logger.info("=" * 50)

        # Return success/failure for exit code
        return self.songs_failed == 0


class BatchDownloader:
    """Non-interactive batch downloader."""

    loop: asyncio.AbstractEventLoop
    ripper: "Ripper"
    local_instance: Optional["QemuInstance"] = None
    stats: BatchStats

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.stats = BatchStats()

        # Lazy import to avoid Creart initialization issues
        from src.rip import Ripper
        self.ripper = Ripper(
            on_start=self._on_song_start,
            on_success=self._on_song_success,
            on_failure=self._on_song_failure,
            on_skip=self._on_song_skip,
        )

    def _on_song_start(self, name: str):
        self.stats.songs_started += 1

    def _on_song_success(self, name: str):
        self.stats.songs_success += 1

    def _on_song_failure(self, name: str, error: str):
        self.stats.songs_failed += 1
        self.stats.failed_songs.append((name, error))

    def _on_song_skip(self, name: str):
        self.stats.songs_skipped += 1

    async def initialize(self) -> bool:
        """
        Initialize all required services for downloading.
        Returns True if initialization succeeded, False otherwise.
        """
        # Lazy imports
        import grpc.aio
        from creart import it
        from src.api import WebAPI
        from src.config import Config
        from src.grpc.manager import WrapperManager
        from src.logger import GlobalLogger
        from src.qemu import QemuInstance
        from src.utils import check_dep, run_sync, safely_create_task, config_outdated

        # Check dependencies
        dep_installed, missing_dep = check_dep()
        if not dep_installed:
            it(GlobalLogger).logger.error(f"Dependency {missing_dep} was not installed!")
            return False

        # Initialize WebAPI
        await run_sync(it(WebAPI).init)

        # Initialize wrapper-manager (local or remote)
        if it(Config).localInstance.enable:
            self.local_instance = QemuInstance()
            await self.local_instance.launch_instance(self.loop)
            it(Config).instance.url = "127.0.0.1:32767"
            it(Config).instance.secure = False
            await it(WrapperManager).init(it(Config).instance.url, it(Config).instance.secure)
            # Wait for wrapper-manager to be ready
            while True:
                it(WrapperManager).status.cache_invalidate()
                if (await it(WrapperManager).status()).ready:
                    break
                await asyncio.sleep(3)
        else:
            await it(WrapperManager).init(it(Config).instance.url, it(Config).instance.secure)

        # Start decrypt stream with reconnection support
        safely_create_task(it(WrapperManager).decrypt_init(
            on_success=self.ripper.on_decrypt_success,
            on_failure=self.ripper.on_decrypt_failed,
            max_reconnect_attempts=it(Config).download.maxReconnectAttempts,
            reconnect_delay=it(Config).download.reconnectDelay,
        ))

        # Verify connection
        try:
            it(WrapperManager).status.cache_invalidate()
            st_resp = await it(WrapperManager).status()
            if not st_resp.regions:
                it(GlobalLogger).logger.error(
                    "The wrapper-manager instance has no available account. "
                    "Please use interactive mode to login first."
                )
                return False
            it(GlobalLogger).logger.info(f"Regions available: {', '.join(st_resp.regions)}")
        except grpc.aio._call.AioRpcError:
            it(GlobalLogger).logger.error("Unable to connect to wrapper-manager")
            return False

        if config_outdated():
            it(GlobalLogger).logger.warning(
                "Config file is outdated. See config.example.toml for updates."
            )

        return True

    async def process_url(self, raw_url: str, codec: str, flags):
        """Process a single URL (song, album, artist, or playlist)."""
        from creart import it
        from src.api import WebAPI
        from src.logger import GlobalLogger
        from src.url import AppleMusicURL, URLType
        from src.utils import safely_create_task

        it(GlobalLogger).logger.debug(f"Processing URL: {raw_url}")
        url = AppleMusicURL.parse_url(raw_url)
        if not url:
            it(GlobalLogger).logger.debug(f"Direct parse failed, trying shortlink resolution: {raw_url}")
            # Try resolving shortlinks
            real_url = await it(WebAPI).get_real_url(raw_url)
            url = AppleMusicURL.parse_url(real_url)
            if not url:
                it(GlobalLogger).logger.error(f"Invalid URL: {raw_url}")
                return

        it(GlobalLogger).logger.debug(f"Parsed URL: type={url.type}, id={url.id}, storefront={url.storefront}")

        match url.type:
            case URLType.Song:
                safely_create_task(
                    self.ripper.rip_song(url, codec, flags)
                )
            case URLType.Album:
                safely_create_task(
                    self.ripper.rip_album(url, codec, flags)
                )
            case URLType.Artist:
                safely_create_task(
                    self.ripper.rip_artist(url, codec, flags)
                )
            case URLType.Playlist:
                safely_create_task(
                    self.ripper.rip_playlist(url, codec, flags)
                )
            case _:
                it(GlobalLogger).logger.error(f"Unsupported URL type: {raw_url}")

    async def process_urls(self, urls: list[str], codec: str, flags, queue_delay: float = 5.0):
        """Process multiple URLs with optional delay between queueing to avoid rate limits."""
        from creart import it
        from src.logger import GlobalLogger
        from src.utils import background_tasks

        processed = 0
        for url in urls:
            url = url.strip()
            if url and not url.startswith('#'):  # Skip empty lines and comments
                await self.process_url(url, codec, flags)
                processed += 1
                # Small delay to avoid overwhelming the server
                if queue_delay > 0:
                    await asyncio.sleep(queue_delay)

        it(GlobalLogger).logger.info(f"Queued {processed} URLs, {len(background_tasks)} background tasks created")

    async def wait_for_completion(self, poll_interval: float = 0.5, startup_timeout: float = 10.0):
        """Wait for all download tasks to complete."""
        from creart import it
        from src.grpc.manager import WrapperManager
        from src.measurer import Measurer
        from src.utils import background_tasks

        # Give event loop time to start tasks
        await asyncio.sleep(0.1)

        # Wait for tasks to start (with timeout)
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        while it(Measurer).tasks_count() == 0 and len(background_tasks) > 0:
            if loop.time() - start_time > startup_timeout:
                break
            await asyncio.sleep(0.1)

        # Now wait for all tasks to finish
        while it(Measurer).tasks_count() > 0 or len(background_tasks) > 0:
            await asyncio.sleep(poll_interval)

        # Capture reconnect count from manager
        self.stats.reconnect_count = it(WrapperManager).reconnect_count

    async def shutdown(self):
        """Clean up resources."""
        if self.local_instance:
            await self.local_instance.terminate()


async def run_batch(
    urls: list[str],
    input_file: Optional[str] = None,
    codec: str = "alac",
    force: bool = False,
    language: Optional[str] = None,
    include_participate: bool = False,
    queue_delay: Optional[float] = None,
) -> int:
    """
    Run batch download mode.

    Args:
        urls: List of URLs to download
        input_file: Path to file containing URLs (one per line)
        codec: Audio codec (alac, aac, ec3, ac3, etc.)
        force: Force overwrite existing files
        language: Metadata language override
        include_participate: Include songs artist participated in
        queue_delay: Delay between queuing URLs (None = use config default)

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Lazy imports
    from creart import it
    from src.config import Config
    from src.flags import Flags
    from src.logger import GlobalLogger

    loop = asyncio.get_running_loop()
    downloader = BatchDownloader(loop)

    # Use config default if not specified
    if queue_delay is None:
        queue_delay = it(Config).download.queueDelay

    # Initialize
    if not await downloader.initialize():
        return 1

    # Collect URLs
    all_urls = list(urls) if urls else []

    if input_file:
        input_path = Path(input_file)
        if not input_path.exists():
            it(GlobalLogger).logger.error(f"Input file not found: {input_file}")
            return 1
        with open(input_path, 'r', encoding='utf-8') as f:
            all_urls.extend(f.read().splitlines())

    if not all_urls:
        it(GlobalLogger).logger.error("No URLs provided")
        return 1

    # Filter empty lines and comments
    all_urls = [u.strip() for u in all_urls if u.strip() and not u.strip().startswith('#')]

    # Track total URLs
    downloader.stats.total_urls = len(all_urls)
    it(GlobalLogger).logger.info(f"Processing {len(all_urls)} URL(s) with codec: {codec}")

    # Build flags
    flags = Flags(
        force_save=force,
        language=language or it(Config).region.language,
        include_participate_in_works=include_participate,
    )

    try:
        # Process all URLs
        await downloader.process_urls(all_urls, codec, flags, queue_delay=queue_delay)

        # Wait for all tasks to complete
        it(GlobalLogger).logger.info("Waiting for downloads to complete...")
        await downloader.wait_for_completion()

        # Print summary
        success = downloader.stats.print_summary()
        return 0 if success else 1

    except KeyboardInterrupt:
        it(GlobalLogger).logger.info("Interrupted by user")
        downloader.stats.print_summary()
        return 130
    finally:
        await downloader.shutdown()
