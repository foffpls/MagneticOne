from dataclasses import dataclass
import os

from dotenv import load_dotenv


load_dotenv()


@dataclass
class BotConfig:
    token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    arcgis_item_id: str = os.getenv("ARCGIS_ITEM_ID", "")
    arcgis_portal_url: str = os.getenv("ARCGIS_PORTAL_URL", "https://www.arcgis.com")
    google_sheet_url: str = os.getenv("GOOGLE_SHEET_URL", "")

    @property
    def arcgis_item_url(self) -> str:
        if not self.arcgis_item_id:
            return ""
        return f"{self.arcgis_portal_url}/home/item.html?id={self.arcgis_item_id}"


def get_bot_config() -> BotConfig:
    return BotConfig()

