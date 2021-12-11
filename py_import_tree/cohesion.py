import os

from copy import copy
from pathlib import Path


def get_package_dir_site_packages(path):
    if path is None:
        return Path('/')
    path = Path(path)
    tmp = copy(path)
    while tmp.parent.stem != 'site-packages' and len(tmp.parts) > 1:
        tmp = tmp.parent
    return tmp


def get_size(start_path):
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


def cohesion_scores(df_dict):
    res = df_dict
    def_with_imports = res['DEFINITIONS'].merge(res['DEFINITIONS_TO_IMPORTS'],
                                                left_on='id',
                                                right_on='definition_id',
                                                suffixes=('_definition', '_import_df'),
                                                how='left')
    df = def_with_imports.merge(res['IMPORT_DATA'], left_on='import_code_str', right_on='code_str')
    concrete_path_to_module_path = {path: str(get_package_dir_site_packages(path)) for path in df['path'].unique()}
    df['package_path'] = df['path'].map(concrete_path_to_module_path)
    unique_package_paths = list(set(list(concrete_path_to_module_path.values())))
    package_path_to_weight = {k: get_size(k) for k in unique_package_paths}
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
    return {
        'full_df': df,
        'definitions_df': definitions_df,
        'cohesion_score': definitions_df['cohesion_score'].mean()
    }
