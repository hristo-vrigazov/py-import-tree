import os
import pickle
import sqlite3
from collections import defaultdict
from copy import copy
from dataclasses import dataclass
from functools import partial
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
    return sub_df.drop_duplicates(subset='dependency')['dependency_weight'].sum()


@dataclass
class Cohesion:
    score: float
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


def read_child_files_of_package(child, package_name, version):
    return {str(module_path.resolve()): (package_name, version) for module_path in
            (child.parent / package_name).glob('**/*')}


def read_installed_files(child, package_name, version):
    res = {}
    path = child / 'installed-files.txt'
    if not path.exists():
        return res
    with open(path) as installed_file:
        for file in installed_file:
            file_path = str((child / file).resolve())
            res[file_path] = package_name, version
    return res


def get_dict_for_package_dist_info(child):
    res = {}
    site_packages_path = child.parent
    package_name, version = child.stem.split('-', 1)
    records = pd.read_csv(child / 'RECORD', names=['filename', 'meta0', 'meta1'], header=None)
    for filename in records['filename']:
        file_path = str((site_packages_path / filename).resolve())
        res[file_path] = package_name, version
    res.update(read_child_files_of_package(child, package_name, version))
    return res


def get_dict_for_package_egg_info(child):
    package_name, version = child.stem.split('-', 1)
    res = read_installed_files(child, package_name, version)
    res.update(read_child_files_of_package(child, package_name, version))
    return res


def get_package_weight(dct):
    total = 0
    for file in dct:
        if os.path.exists(file):
            total += os.path.getsize(file)
    return total


def get_absolute_path_to_package_and_version_dict():
    print(f'Indexing site-packages files ...')
    package_name_resolver = {}
    package_weight_dict = defaultdict(lambda : 0)
    site_packages = site.getsitepackages() + [site.getusersitepackages()]
    for site_packages_path in site_packages:
        site_packages_path = Path(site_packages_path)
        if not site_packages_path.exists():
            print(site_packages_path, 'does not exist')
            continue
        for child in site_packages_path.iterdir():
            if child.suffix not in {'.dist-info', '.egg-info'}:
                continue
            if child.suffix == '.dist-info':
                dct = get_dict_for_package_dist_info(child)
            elif child.suffix == '.egg-info':
                dct = get_dict_for_package_egg_info(child)
            else:
                dct = {}
            package_name_resolver.update(dct)
            package_name, version = child.stem.split('-', 1)
            package_weight_dict[f'{package_name}=={version}'] = get_package_weight(dct)
    print(f'Done indexing site-packages files.')
    return package_name_resolver, package_weight_dict


def get_dependency(path, absolute_path_to_package_and_version_dict):
    res = absolute_path_to_package_and_version_dict.get(path)
    if res is not None:
        dependency, version = res
        return f'{dependency}=={version}'
    return np.nan


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

    def cohesion(self):
        df = self.get_packages_df()
        return Cohesion(score=df.drop_duplicates(subset='definition')['cohesion_score'].mean(),
                        definitions=df)

    def get_full_df(self):
        def_with_imports = self.definitions.merge(self.definitions_to_imports,
                                                  left_on='id',
                                                  right_on='definition_id',
                                                  suffixes=('_definition', '_import_df'),
                                                  how='left')
        df = def_with_imports.merge(self.import_data, left_on='import_code_str', right_on='code_str', how='left')
        return df

    def get_packages_df(self):
        full = self.get_full_df()
        dct, package_weight = get_absolute_path_to_package_and_version_dict()
        full['dependency'] = full['path'].map(partial(get_dependency,
                                                      absolute_path_to_package_and_version_dict=dct))
        res = pd.DataFrame({
            'path': full['filename_path'],
            'definition': full['type'] + ':' + full['name'],
            'import': full['import_code_str'],
            'dependency': full['dependency'],
            'transitive_import_filename': full['path']
        })
        res['dependency_weight'] = res['dependency'].map(package_weight)
        ideal_weight_dict = res.groupby('definition').apply(compute_weight)
        res['definition_ideal_weight'] = res['definition'].map(ideal_weight_dict)
        actual_weight_dict = res.groupby('path').apply(compute_weight).to_dict()
        res['definition_actual_weight'] = res['path'].map(actual_weight_dict)
        ideal = res['definition_ideal_weight']
        actual = res['definition_actual_weight']
        res['cohesion_score'] = ideal / actual
        res.loc[res['definition_actual_weight'] < 1e-4, 'cohesion_score'] = 1.
        return res

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
