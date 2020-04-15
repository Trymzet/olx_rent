from pathlib import Path
from typing import List
import dotenv
import requests
from bs4 import BeautifulSoup  # type: ignore
from utils import get_random_user_agent, olx_rent_logger

dotenv_dir = Path(__file__).parent.parent
dotenv_path = dotenv_dir.joinpath(".env")
dotenv.load_dotenv(dotenv_path)

logger = olx_rent_logger


class Search:
    def __init__(
        self,
        filters,
        category: str = "nieruchomosci",
        subcategory: str = "stancje-pokoje",
        city: str = "krakow",
        streets: List[str] = ["Kapelanka"],
        min_price: float = None,
        max_price: float = None,
    ):
        self.category = category
        self.subcategory = subcategory
        self.city = city
        self.streets = streets
        self.min_price = min_price
        self.max_price = max_price


def get_all_pages(url: str):
    pass


def find_offers(
    category: str = "nieruchomosci",
    subcategory: str = "stancje-pokoje",
    city: str = "krakow",
    streets: List[str] = ["Kapelanka"],
    min_price: float = None,
    max_price: float = None,
):
    responses = []
    for street in streets:
        url = f"https://www.olx.pl/{category}/{subcategory}/{city}/q-{street}"
        response = get_page(url)
    responses.append(response)
    return responses


responses = find_offers()
first_response = responses[0]
soup = BeautifulSoup(first_response, "html.parser")
print("a")
