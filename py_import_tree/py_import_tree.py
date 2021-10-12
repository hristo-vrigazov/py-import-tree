import sys
from multiprocessing import Process

from stdlib_list import stdlib_list


def get_root_module(key):
    res = key.split('.')[0]
    return '-'.join(res.split('_'))


class ImportTracker:

    def __init__(self, blacklisting_function=None):
        self.stdlib_packages_set = set(stdlib_list())
        self.blacklisting_function = blacklisting_function

    def module_should_be_tracked(self, key):
        if key.startswith('_'):
            return False
        if key.startswith('ipywidgets'):
            return False
        if key in sys.builtin_module_names:
            return False
        if key in self.stdlib_packages_set:
            return False
        return True

    def should_be_tracked(self, key, module, modules_before):
        if key in modules_before:
            return False
        if not self.module_should_be_tracked(key):
            return False
        if self.blacklisting_function is not None and self.blacklisting_function(key, module, modules_before):
            return False
        return True

    def _get_packages_data(self, code_str, filename):
        #TODO: check if it is a relative import, and if it is, change the working directory
        modules_before = sys.modules.copy()
        a = exec(code_str)
        modules_after = sys.modules.copy()
        df_data = {
            'root_module': [],
            'import': [],
            'file': [],
            'version': [],
        }
        for key, module in modules_after.items():
            if not self.should_be_tracked(key, module, modules_before):
                continue
            df_data['root_module'].append(get_root_module(key))
            df_data['import'].append(key)
            try:
                df_data['file'].append(module.__file__)
            except:
                df_data['file'].append(None)

            try:
                df_data['version'].append(module.__version__)
            except:
                df_data['version'].append(None)
        import pandas as pd
        df = pd.DataFrame(df_data)
        df.to_csv(f'{filename}.csv')
        df = df.dropna()
        df.to_csv(f'{filename}.clean.csv')

    def dump_package_data(self, code_str, filename):
        p = Process(target=self._get_packages_data, args=(code_str, filename))
        p.start()
        p.join()
