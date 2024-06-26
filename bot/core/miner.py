import time
from math import ceil
import asyncio
from urllib.parse import unquote
from typing import Any, Tuple, Optional, Dict, List

import aiohttp
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered
from pyrogram.raw.functions.messages import RequestWebView

from bot.utils import logger
from bot.exceptions import InvalidSession
from .headers import headers
from bot.config import settings


class Miner:
    def __init__(self, tg_client: Client):
        self.session_name = tg_client.name
        self.tg_client = tg_client
        self.authorized = False

        self.speed_levels = {
            '0': 0.01,
            '1': 0.015,
            '2': 0.02,
            '3': 0.025,
            '4': 0.03,
            '5': 0.05
        }
        self.speed_upgrades = {
            '1': 0.2,
            '2': 1.0,
            '3': 2.0,
            '4': 5.0,
            '5': 15.0
        }

        self.storage_levels = {
            '0': 2,
            '1': 3,
            '2': 4,
            '3': 6,
            '4': 12,
            '5': 24
        }
        self.storage_upgrades = {
            '1': 0.2,
            '2': 0.5,
            '3': 1.0,
            '4': 4.0,
            '5': 10.0
        }

    async def get_tg_web_data(self, proxy: str | None) -> str:
        try:
            if proxy:
                proxy = Proxy.from_str(proxy)
                proxy_dict = dict(
                    scheme=proxy.protocol,
                    hostname=proxy.host,
                    port=proxy.port,
                    username=proxy.login,
                    password=proxy.password
                )
            else:
                proxy_dict = None

            self.tg_client.proxy = proxy_dict

            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()
                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            web_view = await self.tg_client.invoke(RequestWebView(
                peer=await self.tg_client.resolve_peer('Newcoldwallet_bot'),
                bot=await self.tg_client.resolve_peer('Newcoldwallet_bot'),
                platform='android',
                from_bot_menu=False,
                url='https://app.coldwallet.cloud/'
            ))

            auth_url = web_view.url
            tg_web_data = unquote(
                string=unquote(
                    string=auth_url.split('tgWebAppData=', maxsplit=1)[1].split('&tgWebAppVersion', maxsplit=1)[0]))

            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return tg_web_data

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error during Authorization: {error}")
            await asyncio.sleep(delay=7)

    async def get_wallet(self, http_client: aiohttp.ClientSession, tg_web_data: str) -> dict[str]:
        try:
            http_client.headers["Telegram-Data"] = tg_web_data

            response = await http_client.get(
                url='https://app.coldwallet.cloud/api/wallet',
                json={})
            response.raise_for_status()

            response_json = await response.json()
            wallet_data = response_json

            return wallet_data
        except Exception as error:
            if 'unauthorized' in str(error).lower():
                self.authorized = False
            logger.error(f"{self.session_name} | Unknown error while getting Wallet Data: {error}")
            await asyncio.sleep(delay=7)

    async def get_account_tokens(self, http_client: aiohttp.ClientSession, address: str) -> dict[str]:
        try:
            response = await http_client.get(
                url=f'https://app.coldwallet.cloud/api/wallet/tokens?account_address={address}',
                json={})
            response.raise_for_status()

            response_json = await response.json()
            tokens_balance = response_json

            return tokens_balance
        except Exception as error:
            if 'unauthorized' in str(error).lower():
                self.authorized = False
            logger.error(f"{self.session_name} | Unknown error while getting account status: {error}")
            await asyncio.sleep(delay=7)

    async def get_account_mining_status(self, http_client: aiohttp.ClientSession, address: str, token_address: str) -> dict[str]:
        url=f'https://app.coldwallet.cloud/api/mining?account_address={address}&token_address={token_address}'
        try:
            response = await http_client.get(
                url=url,
                json={})
            response.raise_for_status()

            response_json = await response.json()
            mining_status = response_json

            return mining_status
        except Exception as error:
            if 'unauthorized' in str(error).lower():
                self.authorized = False
            logger.error(f"{self.session_name} | Unknown error while getting mining status: {error}")
            await asyncio.sleep(delay=7)

    async def claim(self, http_client: aiohttp.ClientSession, address: str, token_address: str) -> dict[str]:
        url=f'https://app.coldwallet.cloud/api/mining/claim?account_address={address}&token_address={token_address}'
        try:
            response = await http_client.post(
                url=url,
                json={})
            response.raise_for_status()

            response_json = await response.json()
            account_status = response_json

            return account_status
        except Exception as error:
            if 'unauthorized' in str(error).lower():
                self.authorized = False
            logger.error(f"{self.session_name} | Unknown error while claiming: {error}")
            await asyncio.sleep(delay=7)

    async def upgrade_speed(self, http_client: aiohttp.ClientSession, address: str, token_address: str, speed_level: int) -> Optional[Dict[str, Any]]:
        url=f'https://app.coldwallet.cloud/api/mining/speed?account_address={address}&token_address={token_address}'
        try:
            response = await http_client.post(
                url=url,
                json={'speed_level': speed_level})
            response.raise_for_status()

            response_json = await response.json()
            speed_status = response_json

            return speed_status
        except Exception as error:
            if 'unauthorized' in str(error).lower():
                self.authorized = False
            logger.error(f"{self.session_name} | Unknown error while upgrading speed: {error}")
            await asyncio.sleep(delay=7)

            return None

    async def upgrade_storage(self, http_client: aiohttp.ClientSession, address: str, token_address: str, storage_level: int) -> Optional[Dict[str, Any]]:
        url=f'https://app.coldwallet.cloud/api/mining/storage?account_address={address}&token_address={token_address}'
        try:
            response = await http_client.post(
                url=url,
                json={'storage_level': storage_level})
            response.raise_for_status()

            response_json = await response.json()
            storage_status = response_json

            return storage_status
        except Exception as error:
            if 'unauthorized' in str(error).lower():
                self.authorized = False
            logger.error(f"{self.session_name} | Unknown error while upgrading storage: {error}")
            await asyncio.sleep(delay=7)

            return None

    def is_claimable(self, account: Dict[str, Any]) -> Tuple[bool, int, float]:
        speed_level = account['speed_level']
        storage_level = account['storage_level']
        cycle_started_at = account['cycle_started_at']

        speed = self.speed_levels.get(str(speed_level))
        claim_period = self.storage_levels.get(str(storage_level)) * 3600
        cur_time = time.time()

        if cur_time - cycle_started_at >= claim_period:
            percent_elapsed = 100
            return True, percent_elapsed, speed

        percent_elapsed = ceil(100 * (cur_time - cycle_started_at) / (claim_period * 3600))
        if percent_elapsed >= settings.CLAIM_MIN_PERCENT:
            return True, percent_elapsed, speed
        return False, percent_elapsed, speed

    def get_next_claim_sleep_time(self, account: Dict[str, Any]) -> int:
        storage_level = account['storage_level']
        cycle_started_at = account['cycle_started_at']
        claim_period = self.storage_levels.get(str(storage_level)) * 3600

        next_time = cycle_started_at + claim_period
        cur_time = time.time()
        return int(ceil(next_time - cur_time))

    async def check_proxy(self, http_client: aiohttp.ClientSession, proxy: Proxy) -> None:
        try:
            response = await http_client.get(url='https://httpbin.org/ip', timeout=aiohttp.ClientTimeout(5))
            ip = (await response.json()).get('origin')
            logger.info(f"{self.session_name} | Proxy IP: {ip}")
        except Exception as error:
            logger.error(f"{self.session_name} | Proxy: {proxy} | Error: {error}")

    async def update_accounts(self, accounts: List[Dict[str, Any]], http_client: aiohttp.ClientSession):
        for account in accounts:
            address = account['address']
            account_balance = await self.get_account_tokens(http_client=http_client, address=address)

            balance = account_balance.get('data')[0].get('balance')
            token_address = account_balance.get('data')[0].get('address')

            # save fields
            account['balance'] = balance
            account['token_address'] = token_address

            # check mining status
            logger.info(f"{self.session_name} | balance is {account['balance']} COLD")
            mining_status = await self.get_account_mining_status(http_client=http_client, address=address, token_address=token_address)
            if not mining_status.get('data'):
                logger.info(f'Cannot get mining status for account address {address}')
                continue

            account['storage_level'] = mining_status['data']['storage_level']
            account['storage_next_level'] = mining_status['data']['storage_next_level']
            account['speed_level'] = mining_status['data']['speed_level']
            account['speed_next_level'] = mining_status['data']['speed_next_level']
            account['cycle_started_at'] = mining_status['data']['cycle_started_at']

    async def run(self, proxy: str | None) -> None:
        accounts = []
        sleep_time = 3600
        proxy_conn = ProxyConnector().from_url(proxy) if proxy else None

        async with (aiohttp.ClientSession(headers=headers, connector=proxy_conn) as http_client):
            if proxy:
                await self.check_proxy(http_client=http_client, proxy=proxy)

            while True:
                try:
                    if self.authorized is False:
                        tg_web_data = await self.get_tg_web_data(proxy=proxy)
                        wallet_data = await self.get_wallet(http_client=http_client, tg_web_data=tg_web_data)

                        if wallet_data.get('data') and wallet_data.get('data').get('accounts'):
                            accs = wallet_data.get('data').get('accounts')
                            for acc in accs:
                                address = acc.get('address')
                                accounts.append({'address': address})

                        await self.update_accounts(accounts, http_client=http_client)
                        if not accounts:
                            logger.error(f"{self.session_name} | Failed to update accounts status")
                            await asyncio.sleep(delay=5)
                            continue
                        self.authorized = True

                    sleeps = []
                    # process accounts - claim and upgrade:
                    for account in accounts:
                        claimable, percent, speed = self.is_claimable(account=account)

                        # claim if possible
                        logger.info(f'{self.session_name} | speed {speed} COLD/hour, storage fill {percent}%, claim possibility: {claimable}')
                        if claimable:
                            retry = 0
                            while retry < settings.CLAIM_RETRY_COUNT:
                                logger.info(f"{self.session_name} | Retry <y>{retry+1}</y> of <e>{settings.CLAIM_RETRY_COUNT}</e>")
                                account_status = await self.claim(http_client=http_client, address=account['address'], token_address=account['token_address'])
                                if account_status:
                                    balance = account_status['data']['balance']
                                    account['balance'] = balance
                                    logger.success(f'{self.session_name} | claimed successful, new balance is {balance} COLD')
                                    break

                                retry += 1

                        # upgrade speed if possible
                        if settings.UPGRADE_SPEED is True:
                            next_speed_level = account['speed_next_level']
                            if next_speed_level <= settings.SPEED_MAX_LEVEL:
                                if float(account['balance']) >= self.speed_upgrades[str(next_speed_level)]:
                                    logger.info(f"{self.session_name} | Sleep 5s before upgrade speed to {next_speed_level} lvl")
                                    await asyncio.sleep(delay=5)

                                    speed_status = await self.upgrade_speed(http_client=http_client,
                                                                            address=account['address'],
                                                                            token_address=account['token_address'],
                                                                            speed_level=next_speed_level)
                                    if speed_status:
                                        speed_level = speed_status.get('speed_level')
                                        logger.success(f"{self.session_name} | Speed upgraded to {speed_level} lvl")
                                        await self.update_accounts(accounts, http_client=http_client)

                        # upgrade storage if possible
                        if settings.UPGRADE_STORAGE is True:
                            next_storage_level = account['storage_next_level']
                            if next_storage_level <= settings.STORAGE_MAX_LEVEL:
                                if float(account['balance']) >= self.storage_upgrades[str(next_storage_level)]:
                                    logger.info(f"{self.session_name} | Sleep 5s before upgrade storage to {next_storage_level} lvl")
                                    await asyncio.sleep(delay=5)

                                    storage_status = await self.upgrade_storage(http_client=http_client,
                                                                                address=account['address'],
                                                                                token_address=account['token_address'],
                                                                                storage_level=next_storage_level)
                                    if storage_status:
                                        storage_level = speed_status.get('storage_level')
                                        logger.success(f"{self.session_name} | Storage upgraded to {storage_level} lvl")
                                        await self.update_accounts(accounts, http_client=http_client)

                        sleeps.append(self.get_next_claim_sleep_time(account=account))

                    sleep_time = min(sleeps)
                    if sleep_time < 0:
                        sleep_time = 3600

                except InvalidSession as error:
                    raise error

                except Exception as error:
                    logger.error(f"{self.session_name} | Unknown error: {error}")
                    await asyncio.sleep(delay=7)

                else:
                    logger.info(f"Sleeping for the next claim {sleep_time}s")
                    await asyncio.sleep(delay=sleep_time)


async def run_miner(tg_client: Client, proxy: str | None):
    try:
        await Miner(tg_client=tg_client).run(proxy=proxy)
    except InvalidSession:
        logger.error(f"{tg_client.name} | Invalid Session")