import asyncio
import logging.config
import math
import platform
import random
import re
from logging import Logger
from pathlib import Path
from typing import Any, List, Dict, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
import orjson
from httpx import AsyncClient, Client, Proxy, Timeout
from .constants import *
from .login import login
from .util import get_headers, find_key, build_params, read_account_json, save_account_json, ensure_keys_exist

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
    keys_to_ensure = [
        "disabled",
        "last_collection_date",
        "last_collection_count",
        "blocked",
        "cookies",
        "proxy"
        ]
    
    def __init__(self,
                 twitter_accounts: Union[str, Dict],
                 collection_limit_per_account: int = 500,
                 hours_to_reset_collection: int = 12,
                 proxy_credentials: Dict = None,
                 **kwargs: dict) -> None:
        """
        Initializes the Search class.
        
        Parameters:
        - twitter_accounts (str or Dict): You can either pass the path to the accounts JSON or a 
          Dict {"s3": s3 client object initialized, "bucket_name": name of the s3 bucket, "file_name": str with the name of the file you want to save or read}
        - collection_limit_per_account (int): The maximum number of collections per account. Default is 500.
        - hours_to_reset_collection (int): The number of hours to reset the collection limit. Default is 12.
        - proxy_credentials (Dict): {"username": username, "password": password, "bearer_token": bearer_token} for IPRoyal account.
        - save (bool): Whether to save the data or not. Default is False.
        - **kwargs (dict): Additional keyword arguments.
        """
        
        self.twitter_accounts = twitter_accounts
        self.collection_limit_per_account = collection_limit_per_account
        self.hours_to_reset_collection = hours_to_reset_collection
        self.save = kwargs.get('save', False)
        self.logger = self._init_logger(**kwargs)
        self.session = None
        self.client = None
        self.current_account = None
        self.results = None
        self.total_collected_until_now = 0
        self.proxy_credentials = proxy_credentials
        self.__new_session(**kwargs)

    def get_accounts_json(self):
        accounts_json = read_account_json(self.twitter_accounts)
        accounts_list = random.sample(accounts_json["accounts"], len(accounts_json["accounts"]))
        accounts_json["accounts"] = accounts_list
        return accounts_json

    def run(self, queries: List[Dict], limit: int = math.inf, **kwargs: dict) -> List:
        """
        Executes the search based on the queries provided.
        
        Parameters:
        - queries (List[Dict]): A list of query dictionaries to be processed.
        - limit (int): The maximum number of results to retrieve. Default is infinity.
        - **kwargs (dict): Additional keyword arguments.
        
        Returns:
        List: The list containing the search results.
        """
        results = asyncio.run(self.process(queries, limit, **kwargs))
        self.results = results
        return results
    
    def get_tweets_dataframe(self) -> pd.DataFrame:
        """
        Converts the collected tweets into a DataFrame.
        
        Returns:
        pd.DataFrame: A DataFrame containing the tweets' information.
        """
        tweets = [y for x in self.results for y in x if not y.get('entryId').startswith('promoted')]

        tweets_list = []
        df = pd.DataFrame()
        for tweet in tweets:
            try:
                tweet_info = find_key(tweet, 'tweet_results')[0]["result"]["legacy"]
            except Exception:
                tweet_info = find_key(tweet, 'tweet_results')[0]["result"]["tweet"]["legacy"]
            user_info = find_key(tweet, 'user_results')[0]["result"]["legacy"]
            user_info = {f"user_{k}": v for k, v in user_info.items()}
            combined_dict = {**tweet_info, **user_info}
            try:
                combined_dict["post_source"] = find_key(tweet, 'tweet_results')[0]["result"]["source"]
            except Exception:
                combined_dict["post_source"] = None

            tweets_list.append(combined_dict)
        
        if len(tweets_list) > 0:
            df = pd.DataFrame(tweets_list)
            df["created_at"] = pd.to_datetime(df['created_at'], format="%a %b %d %H:%M:%S %z %Y")
            df.sort_values('created_at', ascending=False, inplace=True)
            df.dropna(subset='user_id_str', inplace=True)
            df.reset_index(drop=True, inplace=True)
            df = self.__organize_dataframe(df)
        return df

    async def process(self, queries: list[dict], limit: int, **kwargs) -> list:
        if self.session is None:
            print("There are no sessions available, check to see if your accounts are blocked")
            raise Exception ("No accounts to be used")
        if self.current_account["proxy"]:
            async with AsyncClient(headers=get_headers(self.session), proxies=Proxy(self.current_account["proxy"]), timeout=Timeout(timeout=15.0)) as s:
                self.client = s
                return await asyncio.gather(*(self.paginate(q, limit, **kwargs) for q in queries))
        else:
            async with AsyncClient(headers=get_headers(self.session), timeout=Timeout(timeout=15.0)) as s:
                self.client = s
                return await asyncio.gather(*(self.paginate(q, limit, **kwargs) for q in queries))

    async def paginate(self, query: dict, limit: int, **kwargs) -> list[dict]:
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
            if data is None or self.current_account["blocked"] is True:
                self.current_account["last_collection_count"] = self.current_account["last_collection_count"] + (len(total)- self.total_collected_until_now)
                self.current_account["last_collection_date"] = datetime.now().isoformat()
                if ((len(total) - self.total_collected_until_now) >= (self.collection_limit_per_account - self.current_account["last_collection_count"])):
                    self.current_account["blocked"] = True
                    print(f"{GREEN}The account is being blocked to avoid excess of collections{RESET}")
                self.__update_accounts_json()
                self.total_collected_until_now = len(total)
                opened_session = self.__new_session()
                if opened_session is True:
                    if self.current_account["proxy"]:
                        self.client = AsyncClient(headers=get_headers(self.session), proxies=Proxy(self.current_account["proxy"]), timeout=Timeout(timeout=15.0))
                    else:
                        self.client = AsyncClient(headers=get_headers(self.session), timeout=Timeout(timeout=15.0))
                    continue
                else:
                    return res
                    
            res.extend(entries)
            if len(entries) <= 2 or len(total) >= limit:
                print(f'[{GREEN}success{RESET}] Returned {len(total)} search results for {query["query"][:30]}...{query["query"][-30:]}')
                self.current_account["last_collection_count"] = self.current_account["last_collection_count"] + (len(total) - self.total_collected_until_now)
                self.current_account["last_collection_date"] = datetime.now().isoformat()
                self.__update_accounts_json()
                return res
            
            total |= set(find_key(entries, 'entryId'))
            print(f'{query["query"]}')

    async def get(self, client: AsyncClient, params: dict) -> tuple:
        _, qid, name = Operation.SearchTimeline
        r = await client.get(f'https://twitter.com/i/api/graphql/{qid}/{name}', params=build_params(params))
        data = r.json()
        cursor = self.get_cursor(data)
        if params.get("variables").get("product") == "Media":
            pattern = r'^.*?(tweet|user)-'
            entries = []
            entries_find = find_key(data, 'entries')
            items = find_key(entries_find, 'items')
            for entry_list in items:
                for y in entry_list:
                    if isinstance(y, dict) and 'entryId' in y:
                        if re.search(pattern, y['entryId']):
                            entries.append(y)
        else:
            entries = [y for x in find_key(data, 'entries') for y in x if re.search(r'^.*?(tweet|user)-', y['entryId'])]
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
                        message = e.get("message")
                        print(f'{YELLOW}{message}{RESET}')
                        if "Rate limit exceeded" in message:
                            self.current_account["cookies"] = None
                            self.current_account["blocked"] = True
                            self.__update_accounts_json()
                            print(f'{YELLOW}The account exceeded the rate limit{RESET}: {self.current_account["email"]}')
                            return None, False, None
                        
                        if "authenticate" in message:
                            self.current_account["cookies"] = None
                            self.__update_accounts_json()
                            print(f'{YELLOW}The cookies are no longer working, reseting them {RESET}: {self.current_account["email"]}')
                            return None, False, None
                        if "temporarily locked" in message:
                            self.current_account["cookies"] = None
                            self.current_account["blocked"] = True
                            self.__update_accounts_json()
                            print(f'{RED}The account was blocked {RESET}: {self.current_account["email"]}')
                            return None, False, None
                        return [], [], ''
                ids = set(find_key(data, 'entryId'))
                if len(ids) >= 2:
                    return data, entries, cursor
            except Exception as e:
                if i == retries:
                    print("Max retries exceeded")
                    return None, None, None
                t = 2 ** i + random.random()
                print(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                await asyncio.sleep(t)

    def __organize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Organize the DataFrame by renaming columns and extracting nested information

        Parameters:
        ----------
        df : pd.DataFrame
            The original DataFrame containing tweet information.

        Returns:
        -------
        pd.DataFrame
            The organized DataFrame with renamed columns and additional extracted information.
        """

        numeric = [
            'bookmark_count',
            'favorite_count',
            'quote_count',
            'reply_count',
            'retweet_count',
        ]
        df[numeric] = df[numeric].apply(pd.to_numeric, errors='coerce')
        df['created_at'] = df['created_at'].dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S 00:00")
        df.rename(columns={
            'user_id_str': 'author_id',
            'user_screen_name': 'author_username',
            'user_name': 'author_name',
            'user_description': 'author_description',
            'user_profile_image_url_https': 'author_profile_url',
            'user_followers_count': 'author_engagement_followers_count',
            'user_friends_count': 'author_engagement_friends_count',
            'user_favourites_count': 'author_engagement_likes_count',
            'user_listed_count': 'author_listed_count',
            'user_statuses_count': 'author_posts_count',
            'lang': 'author_language',
            'user_location': 'author_location',
            'user_verified': 'author_verified',
            'id_str': 'post_id',
            'full_text': 'post_text',
            'created_at': 'post_posted_at',
            'favorite_count': 'post_engagement_likes_count',
            'quote_count': 'post_engagement_quote_count',
            'reply_count': 'post_engagement_reply_count',
            'retweet_count': 'post_engagement_retweet_count',
            'retweeted': 'post_retweeted',
            'possibly_sensitive': 'post_possibly_sensitive',
            'in_reply_to_screen_name': 'post_in_reply_to_screen_name',
            'in_reply_to_status_id_str': 'post_in_reply_to_status_id',
            'in_reply_to_user_id_str': 'post_in_reply_to_user_id',
            'is_quote_status': 'post_is_quote_status',
            'quoted_status_id_str': 'post_quoted_status_id'
        }, inplace=True)
        
        if 'entities' in df.columns:
            df['post_user_mentions'] = df['entities'].apply(lambda x: [i.get('screen_name', None) for i in x.get('user_mentions', [])] if isinstance(x, dict) else None)
            df['post_hashtags'] = df['entities'].apply(lambda x: [i.get('text', None) for i in x.get('hashtags', [])] if isinstance(x, dict) else None)
            df['post_urls'] = df['entities'].apply(lambda x: [i.get('expanded_url', None) for i in x.get('urls', [])] if isinstance(x, dict) else None)
        else:
            df['post_user_mentions'] = None
            df['post_hashtags'] = None
            df['post_urls'] = None

        if 'place' in df.columns:
            df['post_location'] = df['place'].apply(lambda x: x.get('full_name', None) if isinstance(x, dict) else None)
        else:
            df['post_location'] = None

        if 'extended_entities' in df.columns:
            df['post_media'] = df['extended_entities'].apply(lambda x:
                                                             [{'display_url': media.get('media_url_https', ''),
                                                               'media_key': media.get('media_key', ''),
                                                               'type': media.get('type', '')}
                                                               for media in x.get('media', [])]
                                                               if isinstance(x, dict) else None
                                                               )
        else:
            df['post_media'] = None

        df["post_language"] = df["author_language"]

        column_order = [
            'author_id', 'author_username', 'author_name', 'author_description',
            'author_profile_url', 'author_engagement_followers_count',
            'author_engagement_friends_count', 'author_engagement_likes_count',
            'author_listed_count', 'author_posts_count', 'author_language',
            'author_location', 'author_timezone', 'author_verified',
            'post_id', 'post_text', 'post_posted_at', 'post_engagement_likes_count',
            'post_engagement_quote_count', 'post_engagement_reply_count',
            'post_engagement_retweet_count', 'post_retweeted',
            'post_possibly_sensitive', 'post_in_reply_to_screen_name',
            'post_in_reply_to_status_id', 'post_in_reply_to_user_id',
            'post_is_quote_status', 'post_quoted_status_id', 'post_user_mentions',
            'post_hashtags', 'post_media', 'post_language', 'post_location', 
            'post_source', 'post_urls', 'processing'
        ]

        for col in column_order:
            if col not in df.columns:
                df[col] = None
        
        df = df[column_order]
        df = df.apply(lambda col: col.map(lambda x: None if isinstance(x, list) and not x else x))
        return df

    def __handle_cookies(self, client: Client, account: Dict[str, Any]) -> None:
        cookies_dict = dict(client.cookies)
        if cookies_dict != account["cookies"]:
            account["cookies"] = cookies_dict
        return account

    def __update_accounts_json(self):
        if self.current_account is None:
            return
        
        accounts_json = self.get_accounts_json()
        for idx, account in enumerate(accounts_json["accounts"]):
            if account["email"] == self.current_account["email"]:
                accounts_json["accounts"][idx] = self.current_account
                save_account_json(accounts_json, self.twitter_accounts)
                return
            
    def __get_accounts_to_use(self) -> None:
        accounts_to_use = []
        accounts_json = self.get_accounts_json()
        for account in accounts_json["accounts"]:
            account = ensure_keys_exist(account, self.keys_to_ensure)
            
            if account["disabled"] is True:
                continue

            if account["last_collection_date"] is None:
                account["last_collection_count"] = 0
                account["blocked"] = False
                accounts_to_use.append(account)
                continue
            
            if account["blocked"] is True:
                last_collection_date = datetime.fromisoformat(account["last_collection_date"])
                if datetime.now() - last_collection_date >= timedelta(hours=self.hours_to_reset_collection):
                    account["last_collection_count"] = 0
                    account["blocked"] = False
                    accounts_to_use.append(account)
                    continue
                else:
                    continue

            if account["last_collection_count"] >= self.collection_limit_per_account:
                last_collection_date = datetime.fromisoformat(account["last_collection_date"])
                if datetime.now() - last_collection_date >= timedelta(hours=self.hours_to_reset_collection):
                    account["last_collection_count"] = 0
                    accounts_to_use.append(account)
                    continue
                else:
                    continue
            
            accounts_to_use.append(account)
        
        save_account_json(accounts_json, self.twitter_accounts)
        return accounts_to_use

    def __new_session(self, **kwargs) -> None:
        print("Starting new session")
        accounts_to_use = self.__get_accounts_to_use()
        for account in accounts_to_use:
            email = account['email']
            self.current_account = account
            try:
                account["proxy"] = self.__get_new_proxy()
                client = self._validate_session(account["email"], account["username"], account["password"], account["cookies"], account["proxy"], **kwargs)
                account = self.__handle_cookies(client, account)
                self.session = client
                self.current_account["blocked"] = False
                self.__update_accounts_json()
                print(f"Using the account: {email}")
                return True
            except Exception as error:        
                print(f"The account hasn't been able to login: {email} | {error}")
                self.session = None
                self.current_account["cookies"] = None
                self.current_account["last_collection_count"] = 0
                self.current_account["last_collection_date"] = datetime.now().isoformat()
                self.__update_accounts_json()
                continue
        print("No account to be used, all blocked")
        return False
    
    def __get_new_proxy(self) -> Optional[Dict]:
        proxy_string = self.__get_new_proxy_ip()
        if proxy_string is None:
            return None
        
        parts = proxy_string.split(":")
        host = parts[0]
        port = parts[1]
        credentials_and_params = ":".join(parts[2:])
        proxy_url = f"http://{credentials_and_params}@{host}:{port}"
        proxy= {
            "http://": proxy_url,
            "https://": proxy_url,
            }
        return proxy_url
                
    def __get_new_proxy_ip(self) -> Optional[str]:
        if self.proxy_credentials is None:
            return None
        
        url = "https://dashboard.iproyal.com/api/residential/royal/reseller/access/generate-proxy-list"
    
        headers = {
            "X-Access-Token": f"Bearer {self.proxy_credentials['bearer_token']}",
            "Content-Type": "application/json",
        }

        data = {
            "username": self.proxy_credentials["username"],
            "password": self.proxy_credentials["password"],
            "proxyCount": 1,
            "rotation": "sticky",
            "location": "_country-br_city-saopaulo",
            "lifetime": "5m",
            "highEndPool": True, 
            "skipIspStatic": True
        }

        with Client(timeout=Timeout(timeout=15.0)) as client:
            response = client.post(url, json=data, headers=headers)

        return response.json()[0]


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
        email, username, password, cookies, proxies = args

        try:
            if isinstance(cookies, dict) and all(cookies.get(c) for c in {'ct0', 'auth_token'}):
                _session = Client(cookies=cookies, max_redirects=100, proxies=Proxy(proxies), timeout=Timeout(timeout=15.0))
                _session.headers.update(get_headers(_session))
                # t = _session.get("https://api.ipify.org?format=json")
                # print(t.json())
                return _session
        except Exception:
            pass

        if isinstance(cookies, str):
            try:
                _session = Client(cookies=orjson.loads(Path(cookies).read_bytes(), proxies=Proxy(proxies)), max_redirects=100, timeout=Timeout(timeout=15.0))
                _session.headers.update(get_headers(_session))
                # t = _session.get("https://api.ipify.org?format=json")
                # print(t.json())
                return _session
            except Exception:
                pass
        
        if all((email, username, password)):
            _session = login(email, username, password, proxies, **kwargs)
            # t = _session.get("https://api.ipify.org?format=json")
            # print(t.json())
            return _session

        print(f"Could not authenticate the session, something went wrong with the account: {email}")
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
