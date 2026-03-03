from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()

@dataclass
class ArcGISConfig:
    portal_url: str = os.getenv("ARCGIS_PORTAL_URL", "https://www.arcgis.com")
    item_id: str = os.getenv("ARCGIS_ITEM_ID", "")
    feature_layer_url: str = os.getenv("ARCGIS_FEATURE_LAYER_URL", "")


def get_arcgis_config() -> ArcGISConfig:
    return ArcGISConfig()

