from __future__ import annotations

from typing import Iterable, List

import logging

import pandas as pd
from arcgis.gis import GIS
from arcgis.features import FeatureLayer

from .config import ArcGISConfig, get_arcgis_config


logger = logging.getLogger(__name__)


FIELD_MAPPING = {
    "Дата": "date_1",
    "Область": "Область",
    "Місто": "city",
    "Значення 1": "value_1",
    "Значення 2": "value_2",
    "Значення 3": "value_3",
    "Значення 4": "value_4",
    "Значення 5": "value_5",
    "Значення 6": "value_6",
    "Значення 7": "value_7",
    "Значення 8": "value_8",
    "Значення 9": "value_9",
    "Значення 10": "value_10",
    "long": "long",
    "lat": "lat",
}


# Підключення до ArcGIS Online в анонімному режимі
def get_gis(arcgis_cfg: ArcGISConfig | None = None) -> GIS:
    cfg = arcgis_cfg or get_arcgis_config()
    return GIS(cfg.portal_url, anonymous=True)


def get_feature_layer(gis: GIS | None = None, item_id: str | None = None) -> FeatureLayer:
    """
    Повертає FeatureLayer: або за прямим URL (ARCGIS_FEATURE_LAYER_URL),
    або через Portal (item_id). Для публічних шарів краще вказати URL — уникаємо 403 з Portal.
    """
    cfg = get_arcgis_config()
    if cfg.feature_layer_url:
        return FeatureLayer(cfg.feature_layer_url)
    if gis is None:
        gis = get_gis(cfg)
    item = gis.content.get(item_id or cfg.item_id)
    return item.layers[0]


def df_to_features(df: pd.DataFrame) -> List[dict]:
    features: List[dict] = []
    for _, row in df.iterrows():
        # Нормалізація координат
        def _to_float(value) -> float:
            if isinstance(value, str):
                value = value.replace(",", ".")
            return float(value)

        x = _to_float(row["long"])
        y = _to_float(row["lat"])

        attributes: dict = {}
        for src, dst in FIELD_MAPPING.items():
            if src not in df.columns:
                continue
            if dst == "long":
                attributes[dst] = x
            elif dst == "lat":
                attributes[dst] = y
            else:
                attributes[dst] = row[src]
        feature = {
            "attributes": attributes,
            "geometry": {"x": x, "y": y, "spatialReference": {"wkid": 4326}},
        }
        features.append(feature)
    return features


def upload_dataframe(df: pd.DataFrame, clear_existing: bool = False) -> None:
    cfg = get_arcgis_config()
    gis = get_gis(cfg) if not cfg.feature_layer_url else None
    layer = get_feature_layer(gis, cfg.item_id)

    if clear_existing:
        logger.info("Видаляю існуючі об'єкти з шару: %s", layer.url)
        delete_result = layer.delete_features(where="1=1")
        logger.info("Результат видалення: %s", delete_result)

    features = df_to_features(df)
    if features:
        logger.info("Додаю %s об'єкт(ів) у шар: %s", len(features), layer.url)
        try:
            result = layer.edit_features(adds=features)
            logger.info("Результат edit_features: %s", result)

            add_results = result.get("addResults") if isinstance(result, dict) else None
            if add_results:
                errors = [r for r in add_results if not r.get("success")]
                if errors:
                    raise RuntimeError(f"Помилки при додаванні об'єктів: {errors}")
        except Exception as exc:
            logger.error("Помилка при виконанні edit_features: %s", exc, exc_info=True)
            raise
