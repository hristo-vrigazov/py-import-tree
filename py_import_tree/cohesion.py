import os
import sqlite3

from copy import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd


def get_package_dir_site_packages(path):
    if path is None or pd.isna(path):
        return Path('/')
    path = Path(path)
    tmp = copy(path)
    while tmp.parent.stem != 'site-packages' and len(tmp.parts) > 1:
        tmp = tmp.parent
    return tmp


def get_size_of_directory(start_path):
    start_path = Path(start_path)
    if start_path == Path('/'):
        return 0
    if os.path.isfile(start_path):
        return os.path.getsize(start_path)
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size


def compute_weight(sub_df):
    return sub_df[['root', 'package_weight']].drop_duplicates()['package_weight'].sum()


@dataclass
class CohesionData:
    cohesion_score: float
    full_df: pd.DataFrame
    definitions_df: pd.DataFrame


@dataclass
class ImportTree:
    imports: pd.DataFrame
    import_data: pd.DataFrame
    filenames: pd.DataFrame
    definitions: pd.DataFrame
    definitions_to_imports: pd.DataFrame
    filenames_to_imports: pd.DataFrame

    def compute_cohesion_data(self, weight_func=get_size_of_directory):
        def_with_imports = self.definitions.merge(self.definitions_to_imports,
                                                  left_on='id',
                                                  right_on='definition_id',
                                                  suffixes=('_definition', '_import_df'),
                                                  how='left')
        df = def_with_imports.merge(self.import_data, left_on='import_code_str', right_on='code_str', how='left')
        concrete_path_to_module_path = {path: str(get_package_dir_site_packages(path)) for path in df['path'].unique()}
        df['package_path'] = df['path'].map(concrete_path_to_module_path)
        unique_package_paths = list(set(list(concrete_path_to_module_path.values())))
        package_path_to_weight = {k: weight_func(k) for k in unique_package_paths}
        df['package_weight'] = df['package_path'].map(package_path_to_weight)
        ideal_weight_dict = df.groupby('id_definition').apply(compute_weight).to_dict()
        df['definition_ideal_weight'] = df['id_definition'].map(ideal_weight_dict)
        actual_weight_dict = df.groupby('filename_path').apply(compute_weight).to_dict()
        df['definition_actual_weight'] = df['filename_path'].map(actual_weight_dict)
        cols = ['id_definition', 'type', 'name', 'start_no', 'end_no', 'filename_path', 'definition_ideal_weight',
                'definition_actual_weight']
        definitions_df = df[cols].drop_duplicates()
        ideal = definitions_df['definition_ideal_weight']
        actual = definitions_df['definition_actual_weight']
        definitions_df['cohesion_score'] = ideal / actual
        definitions_df.loc[definitions_df['definition_actual_weight'] < 1e-4, 'cohesion_score'] = 1.
        return CohesionData(cohesion_score=definitions_df['cohesion_score'].mean(),
                            definitions_df=definitions_df,
                            full_df=df)

    @classmethod
    def from_dump(cls, output_directory: Union[str, Path]):
        output_directory = Path(output_directory)

        with sqlite3.connect(output_directory / 'modules.db') as conn:
            table_names = ['IMPORTS', 'IMPORT_DATA', 'FILENAMES', 'DEFINITIONS',
                           'DEFINITIONS_TO_IMPORTS', 'FILENAMES_TO_IMPORTS']
            res = {}
            for table_name in table_names:
                res[table_name.lower()] = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
            return cls(**res)
