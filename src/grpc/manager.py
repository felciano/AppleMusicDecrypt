import asyncio
import json
from typing import Awaitable, Callable, Type

from async_lru import alru_cache
from creart import AbstractCreator, CreateTargetInfo, exists_module, it
from grpc import ssl_channel_credentials
from grpc.aio import insecure_channel, Channel, secure_channel
from grpc.experimental import ChannelOptions
from tenacity import retry_if_exception_type, retry, wait_random_exponential, stop_after_attempt, \
    retry_if_not_exception_message, before_sleep_log

from src.grpc.manager_pb2 import *
from src.grpc.manager_pb2_grpc import WrapperManagerServiceStub, google_dot_protobuf_dot_empty__pb2
from src.logger import GlobalLogger
from src.config import Config
from src.utils import safely_create_task


class WrapperManagerException(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class WrapperManager:
    _channel: Channel
    _stub: WrapperManagerServiceStub
    _decrypt_queue: asyncio.Queue[DecryptRequest]
    _login_lock: asyncio.Lock
    _url: str
    _secure: bool
    _keepalive_task: asyncio.Task = None
    reconnect_count: int = 0  # Track total reconnections

    def __init__(self):
        self._login_lock = asyncio.Lock()
        self._decrypt_queue = asyncio.Queue()
        self.reconnect_count = 0

    async def init(self, url: str, secure: bool):
        self._url = url
        self._secure = secure
        await self._create_channel()
        return self

    async def _create_channel(self):
        """Create or recreate the gRPC channel."""
        service_config_json = json.dumps(
            {
                "methodConfig": [
                    {
                        "name": [{}],
                        "retryPolicy": {
                            "maxAttempts": 5,
                            "initialBackoff": "0.1s",
                            "maxBackoff": "1s",
                            "backoffMultiplier": 2,
                            "retryableStatusCodes": ["UNAVAILABLE", "INTERNAL"],
                        },
                    }
                ]
            }
        )
        options = ((ChannelOptions.SingleThreadedUnaryStream, 1), ("grpc.service_config", service_config_json))
        if self._secure:
            self._channel = secure_channel(self._url, credentials=ssl_channel_credentials(), options=options)
        else:
            self._channel = insecure_channel(self._url, options=options)
        self._stub = WrapperManagerServiceStub(self._channel)

    @alru_cache
    async def status(self) -> StatusData:
        resp: StatusReply = await self._stub.Status(google_dot_protobuf_dot_empty__pb2.Empty)
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data

    async def login(self, username: str, password: str, on_2fa: Callable[[str, str], Awaitable[str]]):
        await self._login_lock.acquire()

        login_queue = asyncio.Queue()

        async def request_stream():
            while True:
                item = await login_queue.get()
                if item is None:
                    break
                yield item

        stream = self._stub.Login(request_stream())

        await login_queue.put(LoginRequest(data=LoginData(username=username, password=password)))

        async for reply in stream:
            reply: LoginReply
            match reply.header.code:
                case -1:
                    self._login_lock.release()
                    await login_queue.put(None)
                    raise WrapperManagerException(reply.header.msg)
                case 0:
                    self._login_lock.release()
                    await login_queue.put(None)
                    return
                case 2:
                    two_step_code = await on_2fa(username, password)
                    await login_queue.put(LoginRequest(data=LoginData(
                        username=username,
                        password=password,
                        two_step_code=two_step_code)))

    async def decrypt(self, adam_id: str, key: str, sample: bytes, sample_index: int):
        await self._decrypt_queue.put(
            DecryptRequest(data=DecryptData(adam_id=adam_id, key=key, sample_index=sample_index,
                                            sample=sample)))

    async def _decrypt_request_generator(self):
        while True:
            yield await self._decrypt_queue.get()

    async def decrypt_init(self, on_success: Callable[[str, str, bytes, int], Awaitable[None]],
                           on_failure: Callable[[str, str, bytes, int], Awaitable[None]],
                           max_reconnect_attempts: int = 10,
                           reconnect_delay: float = 5.0):
        """
        Initialize the decrypt stream with automatic reconnection on failure.

        Args:
            on_success: Callback for successful decryption
            on_failure: Callback for failed decryption
            max_reconnect_attempts: Maximum number of reconnection attempts (0 = unlimited)
            reconnect_delay: Delay in seconds between reconnection attempts
        """
        reconnect_count = 0

        while max_reconnect_attempts == 0 or reconnect_count < max_reconnect_attempts:
            try:
                # Cancel existing keepalive task if any
                if self._keepalive_task and not self._keepalive_task.done():
                    self._keepalive_task.cancel()
                    try:
                        await self._keepalive_task
                    except asyncio.CancelledError:
                        pass

                # Start new stream and keepalive
                stream = self._stub.Decrypt(self._decrypt_request_generator())
                self._keepalive_task = asyncio.create_task(self._decrypt_keepalive())

                if reconnect_count > 0:
                    it(GlobalLogger).logger.info(f"Decrypt stream reconnected successfully (attempt {reconnect_count})")
                    reconnect_count = 0  # Reset on successful connection

                async for reply in stream:
                    reply: DecryptReply
                    if reply.data.adam_id == "KEEPALIVE":
                        continue
                    match reply.header.code:
                        case -1:
                            safely_create_task(
                                on_failure(reply.data.adam_id, reply.data.key, reply.data.sample, reply.data.sample_index))
                        case 0:
                            safely_create_task(
                                on_success(reply.data.adam_id, reply.data.key, reply.data.sample, reply.data.sample_index))

            except Exception as e:
                reconnect_count += 1
                self.reconnect_count += 1  # Track total reconnections
                it(GlobalLogger).logger.warning(
                    f"Decrypt stream disconnected: {e}. Reconnecting in {reconnect_delay}s (attempt {reconnect_count})..."
                )

                # Wait before reconnecting
                await asyncio.sleep(reconnect_delay)

                # Recreate the channel
                try:
                    await self._create_channel()
                except Exception as channel_err:
                    it(GlobalLogger).logger.error(f"Failed to recreate channel: {channel_err}")

        it(GlobalLogger).logger.error(f"Decrypt stream failed after {max_reconnect_attempts} reconnection attempts")

    async def _decrypt_keepalive(self):
        while True:
            await self._decrypt_queue.put(DecryptRequest(data=DecryptData(adam_id="KEEPALIVE")))
            await asyncio.sleep(15)

    @retry(retry=((retry_if_exception_type(WrapperManagerException)) & (
            retry_if_not_exception_message('no available instance'))),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def m3u8(self, adam_id: str) -> str:
        resp: M3U8Reply = await self._stub.M3U8(M3U8Request(data=M3U8DataRequest(adam_id=adam_id)))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data.m3u8

    @retry(retry=((retry_if_exception_type(WrapperManagerException)) & (
            retry_if_not_exception_message('no such account'))),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def logout(self, username: str):
        resp: LogoutReply = await self._stub.Logout(LogoutRequest(data=LogoutData(username=username)))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return

    @retry(retry=((retry_if_exception_type(WrapperManagerException)) & (
            retry_if_not_exception_message('no available instance'))),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def lyrics(self, adam_id: str, language: str, region: str) -> str:
        resp: LyricsReply = await self._stub.Lyrics(LyricsRequest(
            data=LyricsDataRequest(adam_id=adam_id, language=language, region=region)))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data.lyrics

    @retry(retry=((retry_if_exception_type(WrapperManagerException)) & (
            retry_if_not_exception_message('no available instance'))),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def webPlayback(self, adam_id: str) -> str:
        resp: WebPlaybackReply = await self._stub.WebPlayback(WebPlaybackRequest(
            data=WebPlaybackDataRequest(adam_id=adam_id)
        ))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data.m3u8

    @retry(retry=((retry_if_exception_type(WrapperManagerException)) & (
            retry_if_not_exception_message('no available instance'))),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def license(self, adam_id: str, challenge: str, kid: str) -> str:
        resp: LicenseReply = await self._stub.License(LicenseRequest(
            data=LicenseDataRequest(adam_id=adam_id, challenge=challenge, uri=kid)
        ))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data.license


class WMCreator(AbstractCreator):
    targets = (
        CreateTargetInfo("src.grpc.manager", "WrapperManager"),
    )

    @staticmethod
    def available() -> bool:
        return exists_module("src.grpc.manager")

    @staticmethod
    def create(create_type: Type[WrapperManager]) -> WrapperManager:
        return create_type()
