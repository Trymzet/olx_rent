from pathlib import Path
from src import olx_rent
import logging

logging = logging.getLogger(__name__)


def test_dotenv():
    tests_dir = Path(__file__).parent
    assert olx_rent.dotenv_path == tests_dir.parent.joinpath(".env")


# def run():
#     test_dotenv()


# run()
