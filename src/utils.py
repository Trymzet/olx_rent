import random
import logging
import sys
from time import gmtime
from config import config
import requests


def create_logger(name: str) -> logging.Logger:
    """
    Creates a logger with a `StreamHandler` that has level and formatting
    set from `squashalytics.config`.
    Args:
        - name (str): the suffix to add to the logger's name.
    Returns:
        - logging.Logger: a configured logging object
    """
    # logging.setLogRecordFactory(_log_record_context_injector)

    logger = logging.getLogger(name)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(config.logging.format)
    formatter.converter = gmtime  # type: ignore
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(config.logging.level)
    return logger


olx_rent_logger = create_logger(name="olx_rent")


def get_page(url: str, page: int = None) -> str:
    """Retrieve the specified page of a website at [url]
    
    Parameters
    ----------
    url : str
        Website address
    page : int, optional
        Page number
    
    Returns
    -------
    str
        Response string representing the website at [url]
    
    Raises
    ------
    ValueError
        If request fails
    """
    if page:
        url += f"&page={page}"
        # TODO: handle case when page > existing number of pages
        # in this case, OLX returns the last available page
    response = requests.get(url, headers={"User-Agent": get_random_user_agent()})
    if not response.ok:
        raise ValueError("Failed to retrieve response from {url}")
    return response.text


USER_AGENTS = [
    "Mozilla/5.0 (CrKey armv7l 1.5.16041) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
    "42.0.2311.135 Safari/537.36 Edge/12.246",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/"
    "9.0.2 Safari/601.3.9",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
]


def get_random_user_agent():
    """ Randoms user agent to prevent "python" user agent
    :return: Random user agent from USER_AGENTS
    :rtype: str
    """
    return random.choice(USER_AGENTS)
