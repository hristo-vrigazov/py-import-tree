import os
import pickle
import sqlite3
from collections import defaultdict
from copy import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import site


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
class Cohesion:
    score: float
    full: pd.DataFrame
    definitions: pd.DataFrame


def load_transitive_imports(output_directory):
    df_data = defaultdict(list)
    for child in output_directory.iterdir():
        code_str = child.stem
        with open(child, 'rb') as in_file:
            records = pickle.load(in_file)
            for root, module, path, version, node_identifier in records:
                df_data['root'].append(root)
                df_data['module'].append(module)
                df_data['path'].append(path)
                df_data['version'].append(version)
                df_data['code_str'].append(code_str)
    df = pd.DataFrame(df_data)
    df['id'] = np.arange(len(df)) + 1
    return df


def get_absolute_path_to_package_and_version_dict():
    print(f'Indexing site-packages files ...')
    package_name_resolver = {}
    site_packages = site.getsitepackages() + [site.getusersitepackages()]
    for site_packages_path in site_packages:
        site_packages_path = Path(site_packages_path)
        for child in site_packages_path.glob('*.dist-info'):
            package_name, version = child.stem.split('-', 2)
            records = pd.read_csv(child / 'RECORD', names=['filename', 'meta0', 'meta1'], header=None)
            with open(child / 'INSTALLER') as installer_file:
                installer = installer_file.read()
            for filename in records['filename']:
                file_path = str(site_packages_path / filename)
                package_name_resolver[file_path] = installer, package_name, version
    print(f'Done indexing site-packages files.')
    return package_name_resolver


@dataclass
class ImportTree:
    imports: pd.DataFrame
    import_data: pd.DataFrame
    filenames: pd.DataFrame
    definitions: pd.DataFrame
    definitions_to_imports: pd.DataFrame
    filenames_to_imports: pd.DataFrame

    def what_if_import_moves(self, from_file: str, import_code_str: str, to_file: str):
        fi = self.filenames_to_imports.copy()
        mask = (fi['filename_path'] == from_file) & (fi['import_code_str'] == import_code_str)
        fi.loc[mask, 'filename_path'] = to_file
        return ImportTree(
            imports=self.imports,
            import_data=self.import_data,
            filenames=self.filenames,
            filenames_to_imports=fi,
            definitions=self.definitions,
            definitions_to_imports=self.definitions_to_imports
        )

    def what_if_function_moves(self, from_file: str, function_name: str, to_file: str):
        return self.what_if_definition_moves('FunctionDef', from_file, function_name, to_file)

    def what_if_class_moves(self, from_file: str, class_name: str, to_file: str):
        return self.what_if_definition_moves('ClassDef', from_file, class_name, to_file)

    def what_if_definition_moves(self, def_type: str, from_file: str, function_name: str, to_file: str):
        type_m = self.definitions['type'] == def_type
        file_m = self.definitions['filename_path'] == from_file
        func_m = self.definitions['name'] == function_name
        mask = type_m & file_m & func_m
        definition_id = self.definitions[mask].iloc[0]['id']
        return self.what_if_definition_id_moves(definition_id, to_file)

    def what_if_definition_id_moves(self, definition_id: int, to_file: str):
        definitions = self.definitions.copy()
        definitions.loc[definitions['id'] == definition_id, 'filename_path'] = to_file
        return ImportTree(
            imports=self.imports,
            import_data=self.import_data,
            filenames=self.filenames,
            filenames_to_imports=self.filenames_to_imports,
            definitions=definitions,
            definitions_to_imports=self.definitions_to_imports
        )

    def compute_cohesion(self, weight_func=get_size_of_directory):
        df = self.get_full_df()
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
        return Cohesion(score=definitions_df['cohesion_score'].mean(),
                        definitions=definitions_df,
                        full=df)

    def get_full_df(self):
        def_with_imports = self.definitions.merge(self.definitions_to_imports,
                                                  left_on='id',
                                                  right_on='definition_id',
                                                  suffixes=('_definition', '_import_df'),
                                                  how='left')
        df = def_with_imports.merge(self.import_data, left_on='import_code_str', right_on='code_str', how='left')
        return df

    @classmethod
    def from_dump(cls, output_directory: Union[str, Path]):
        output_directory = Path(output_directory)

        with sqlite3.connect(output_directory / 'modules.db') as conn:
            table_names = ['IMPORTS', 'FILENAMES', 'DEFINITIONS',
                           'DEFINITIONS_TO_IMPORTS', 'FILENAMES_TO_IMPORTS']
            res = {}
            for table_name in table_names:
                res[table_name.lower()] = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
            res['import_data'] = load_transitive_imports(output_directory / 'transitive_imports')
            return cls(**res)
