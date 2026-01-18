from datetime import datetime, date, timedelta
import platform
from pathlib import Path

YEAR = datetime.now().year
SCRAPE_DATE = date.today().strftime("%Y-%m-%d")
TODAY = datetime.now().date()
TOMORROW = TODAY + timedelta(days=1)
CURRENT_HOUR = datetime.now()

START_WEEK = TODAY - timedelta(days=TODAY.weekday())
YESTERDAY = datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d")

if platform.system() == "Windows":
    DOWNLOAD_DIRECTORY = Path("C:/Users/gabri/Downloads")
    CHROME_DOWNLOAD_DIRECTORY = Path("C:/Users/gabri/Downloads")
    CHROME_DRIVER_LOCATION = Path("C:/chromedriver.exe")
    DATA_DIRECTORY = Path("C:/GG/gglib/data")
else:
    DOWNLOAD_DIRECTORY = Path("/Users/gabe/Downloads")
    CHROME_DOWNLOAD_DIRECTORY = Path("/home/ggarrido/Downloads")
    CHROME_DRIVER_LOCATION = Path("/home/ggarrido/Downloads/chromedriver")
    DATA_DIRECTORY = Path("/Users/gabe/GG/gglib/data")

IMPORT_DIRECTORY = DATA_DIRECTORY / "import"
PROCESSED_PROJECTIONS_DIRECTORY = DATA_DIRECTORY / "processed"
PRELOCK_DIRECTORY = DATA_DIRECTORY / "prelock"
FAILED_PROJECTIONS_DIRECTORY = DATA_DIRECTORY / "failed"
OUTPUT_DIRECTORY = DATA_DIRECTORY / "output"
LINEUP_DIRECTORY = DATA_DIRECTORY / "lineups"

FILE_ADDRESES = {
    "download_directory": DOWNLOAD_DIRECTORY,
    "data_directory": DATA_DIRECTORY,
    "import_directory": IMPORT_DIRECTORY,
    "processed_directory": PROCESSED_PROJECTIONS_DIRECTORY,
    "prelock_directory": PRELOCK_DIRECTORY,
    "failed_directory": FAILED_PROJECTIONS_DIRECTORY,
    "output_directory": OUTPUT_DIRECTORY,
    "lineup_directory": LINEUP_DIRECTORY,
}
