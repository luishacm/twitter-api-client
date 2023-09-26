import asyncio
import logging.config
import math
import platform
import random
import re
import time
from logging import Logger
from pathlib import Path
from typing import Any, Dict

import orjson
from httpx import AsyncClient, Client

from .constants import *
from .login import login
from .util import get_headers, find_key, build_params, read_account_json, save_account_json, AccountsEnded

reset = '\x1b[0m'
colors = [f'\x1b[{i}m' for i in range(31, 37)]

try:
    if get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
        import nest_asyncio

        nest_asyncio.apply()
except:
    ...

if platform.system() != 'Windows':
    try:
        import uvloop

        uvloop.install()
    except ImportError as e:
        ...


class Search:
    def __init__(self, accounts_json_path: str, **kwargs):
        self.save = kwargs.get('save', True)
        self.debug = kwargs.get('debug', 0)
        self.logger = self._init_logger(**kwargs)
        self.accounts_json_path = accounts_json_path
        self.accounts_json = read_account_json(accounts_json_path)
        self.session = None
        self.client = None
        self.accounts_ran_count = 0
        self.__new_session()

    def run(self, queries: list[dict], limit: int = math.inf, out: str = 'data/search_results', **kwargs):
        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        return asyncio.run(self.process(queries, limit, out, **kwargs))

    async def process(self, queries: list[dict], limit: int, out: Path, **kwargs) -> list:
        async with AsyncClient(headers=get_headers(self.session)) as s:
            self.client = s
            return await asyncio.gather(*(self.paginate(q, limit, out, **kwargs) for q in queries))

    async def paginate(self, query: dict, limit: int, out: Path, **kwargs) -> list[dict]:
        params = {
            'variables': {
                'count': 20,
                'querySource': 'typed_query',
                'rawQuery': query['query'],
                'product': query['category']
            },
            'features': Operation.default_features,
            'fieldToggles': {'withArticleRichContentState': False},
        }

        res = []
        cursor = ''
        total = set()
        while True:
            if cursor:
                params['variables']['cursor'] = cursor
            data, entries, cursor = await self.backoff(lambda: self.get(self.client, params), **kwargs)
            if data is None:
                print("Starting new session")
                self.__new_session()
                self.client = AsyncClient(headers=get_headers(self.session))
                continue

            res.extend(entries)
            if len(entries) <= 2 or len(total) >= limit:
                self.debug and self.logger.debug(
                    f'[{GREEN}success{RESET}] Returned {len(total)} search results for {query["query"]}')
                return res
            total |= set(find_key(entries, 'entryId'))
            self.debug and self.logger.debug(f'{query["query"]}')
            self.save and (out / f'{time.time_ns()}.json').write_bytes(orjson.dumps(entries))

    async def get(self, client: AsyncClient, params: dict) -> tuple:
        _, qid, name = Operation.SearchTimeline
        r = await client.get(f'https://twitter.com/i/api/graphql/{qid}/{name}', params=build_params(params))
        data = r.json()
        cursor = self.get_cursor(data)
        entries = [y for x in find_key(data, 'entries') for y in x if re.search(r'^(tweet|user)-', y['entryId'])]
        # add on query info
        for e in entries:
            e['query'] = params['variables']['rawQuery']
        return data, entries, cursor

    def get_cursor(self, data: list[dict]):
        for e in find_key(data, 'content'):
            if e.get('cursorType') == 'Bottom':
                return e['value']

    async def backoff(self, fn, **kwargs):
        retries = kwargs.get('retries', 3)
        for i in range(retries + 1):
            try:
                data, entries, cursor = await fn()
                if errors := data.get('errors'):
                    for e in errors:
                        self.logger.warning(f'{YELLOW}{e.get("message")}{RESET}')
                        return [], [], ''
                ids = set(find_key(data, 'entryId'))
                if len(ids) >= 2:
                    return data, entries, cursor
            except Exception as e:
                if i == retries:
                    self.logger.debug(f'Max retries exceeded\n{e}')
                    return None, None, None
                t = 2 ** i + random.random()
                self.logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                await asyncio.sleep(t)

    def __handle_cookies(self, client: Client, account: Dict[str, Any]) -> None:
        cookies_dict = dict(client.cookies)
        if cookies_dict != account["cookies"]:
            account["cookies"] = cookies_dict

    def __new_session(self, **kwargs) -> None:
        for i in range(self.accounts_ran_count, len(self.accounts_json["accounts"])): 
            try:
                account = self.accounts_json["accounts"][i]
                client = self._validate_session(account["email"], account["username"], account["password"], account["cookies"], **kwargs)
                self.__handle_cookies(client, account)
                self.session = client
                self.accounts_ran_count += 1
                save_account_json(self.accounts_json, self.accounts_json_path)
                return
            except Exception:
                self.accounts_ran_count += 1
        raise AccountsEnded

    def _init_logger(self, **kwargs) -> Logger:
        if kwargs.get('debug'):
            cfg = kwargs.get('log_config')
            logging.config.dictConfig(cfg or LOG_CONFIG)

            # only support one logger
            logger_name = list(LOG_CONFIG['loggers'].keys())[0]

            # set level of all other loggers to ERROR
            for name in logging.root.manager.loggerDict:
                if name != logger_name:
                    logging.getLogger(name).setLevel(logging.ERROR)

            return logging.getLogger(logger_name)

    @staticmethod
    def _validate_session(*args, **kwargs) -> Client:
        email, username, password, cookies = args

        try:
            if isinstance(cookies, dict) and all(cookies.get(c) for c in {'ct0', 'auth_token'}):
                _session = Client(cookies=cookies, follow_redirects=True)
                _session.headers.update(get_headers(_session))
                return _session
        except Exception:
            pass

        if cookies is None:
            cookies = kwargs.get('cookies')

        # try validating cookies from file
        if isinstance(cookies, str):
            _session = Client(cookies=orjson.loads(Path(cookies).read_bytes()), follow_redirects=True)
            _session.headers.update(get_headers(_session))
            return _session
        
        if all((email, username, password)):
            return login(email, username, password, **kwargs)

        raise Exception('Session not authenticated. '
                        'Please use an authenticated session or remove the `session` argument and try again.')

    @property
    def id(self) -> int:
        """ Get User ID """
        return int(re.findall('"u=(\d+)"', self.session.cookies.get('twid'))[0])

    def save_cookies(self, fname: str = None):
        """ Save cookies to file """
        cookies = self.session.cookies
        Path(f'{fname or cookies.get("username")}.cookies').write_bytes(orjson.dumps(dict(cookies)))
