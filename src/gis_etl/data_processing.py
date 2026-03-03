from __future__ import annotations

import pandas as pd


VALUE_COLUMNS = [f"Значення {i}" for i in range(1, 11)]
GEOM_COLUMNS = ["long", "lat"]
DIM_COLUMNS = ["Дата", "Область", "Місто"]


def expand_row(row: pd.Series) -> list[dict]:
    count = int(max(row[VALUE_COLUMNS])) if any(row[VALUE_COLUMNS] > 0) else 0
    if count == 0:
        return []

    result_rows: list[dict] = []
    for i in range(count):
        new_row = {col: row[col] for col in DIM_COLUMNS + GEOM_COLUMNS}
        for col in VALUE_COLUMNS:
            value = int(row[col]) if pd.notna(row[col]) else 0
            new_row[col] = 1 if value > i else 0
        result_rows.append(new_row)

    return result_rows


def transform_table(df: pd.DataFrame) -> pd.DataFrame:
    all_rows: list[dict] = []
    for _, row in df.iterrows():
        all_rows.extend(expand_row(row))
    return pd.DataFrame(all_rows, columns=DIM_COLUMNS + VALUE_COLUMNS + GEOM_COLUMNS)


def load_from_csv(path: str, delimiter: str = ",") -> pd.DataFrame:
    return pd.read_csv(path, delimiter=delimiter)


def save_to_csv(df: pd.DataFrame, path: str, index: bool = False) -> None:
    df.to_csv(path, index=index)


def process_csv(input_path: str, output_path: str, delimiter: str = ",") -> None:
    df = load_from_csv(input_path, delimiter=delimiter)
    transformed = transform_table(df)
    save_to_csv(transformed, output_path)

