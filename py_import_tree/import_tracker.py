import ast
import pickle
import sqlite3
import sys
import traceback
from copy import copy
from multiprocessing import Process
from pathlib import Path
from sqlite3 import IntegrityError
from typing import Union

import astunparse
from stdlib_list import stdlib_list


def get_root_module(key):
    res = key.split('.')[0]
    return '-'.join(res.split('_'))


class Wrapper:

    def get_statement(self):
        raise NotImplementedError()


class ImportWrapper(Wrapper):

    def __init__(self, import_stmt: ast.Import, name_idx: int):
        self.import_stmt = import_stmt
        self.name_idx = name_idx

    def get_statement(self):
        res = copy(self.import_stmt)
        res.names = [self.import_stmt.names[self.name_idx]]
        return res


class ImportFromWrapper(Wrapper):

    def __init__(self, import_from_stmt: ast.ImportFrom, name_idx: int):
        self.import_from_stmt = import_from_stmt
        self.name_idx = name_idx

    def get_statement(self):
        res = copy(self.import_from_stmt)
        res.names = [self.import_from_stmt.names[self.name_idx]]
        return res


def get_eff_name(alias):
    return alias.asname if alias.asname is not None else alias.name


class ImportsAndDefinitionsVisitor(ast.NodeVisitor):

    def __init__(self):
        self.import_wrappers = {}
        self.definitions = []

    def store_import(self, node, cls):
        for i, alias in enumerate(node.names):
            eff_name = get_eff_name(alias)
            self.import_wrappers[eff_name] = (cls(node, i))
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import):
        self.store_import(node, ImportWrapper)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        self.store_import(node, ImportFromWrapper)

    def visit_FunctionDef(self, node):
        self.definitions.append(node)

    def visit_ClassDef(self, node):
        self.definitions.append(node)


class RejectingVisitor(ast.NodeVisitor):

    def __init__(self, imported_names):
        self.imported_names = imported_names
        self.used = []

    def visit_Name(self, name):
        if name.id not in self.imported_names:
            self.generic_visit(name)
            return
        if name.lineno < self.imported_names[name.id].get_statement().lineno:
            self.generic_visit(name)
            return
        self.used.append(name)

    def get_unused_import_names(self):
        used_set = set(u.id for u in self.used)
        imported_set = set(self.imported_names.keys())
        return imported_set.difference(used_set)

    def get_used_import_names(self):
        return [self.imported_names[u.id] for u in self.used]


def read_source_file(path_to_module):
    try:
        with open(path_to_module) as in_file:
            return in_file.read()
    except UnicodeError:
        return None


def join_processes(processes):
    for process in processes:
        if process is None:
            continue
        process.join()
        process.close()


class ImportTracker:

    def __init__(self, output_directory: Union[str, Path],
                 blacklisting_function=None):
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(exist_ok=True)
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

    def dump_for_directory(self, directory: Union[str, Path], max_concurrent_processes=8):
        directory = Path(directory)
        filenames = list(directory.glob('**/*.py'))
        self.dump_for_filenames(filenames, max_concurrent_processes)

    def dump_for_filenames(self, filenames, max_concurrent_processes):
        already_traversed = set()
        processes = []
        for i, filename in enumerate(filenames):
            with open(filename) as in_file:
                print(f'[{i}/{len(filenames)}]: Dumping {filename}...')
                processes += self._dump_for_filename(str(filename),
                                                     in_file.read(),
                                                     already_traversed)
                if len(processes) > max_concurrent_processes:
                    join_processes(processes)
                    processes = []

    def _dump_for_filenames(self, filenames, code_strs, already_traversed):
        processes = []
        for i, filename in enumerate(filenames):
            print(f'[{i}/{len(filenames)}]: Dumping {filename}...')
            processes += self._dump_for_filename(filename, code_strs[i], already_traversed)
        join_processes(processes)

    def _dump_for_filename(self, filename, code_str, already_traversed):
        try:
            self._insert_filename(filename)
        except IntegrityError:
            print(f'Filename {filename} has already been traversed, skipping.')
            return []
        visitor = ImportsAndDefinitionsVisitor()
        visitor.visit(ast.parse(code_str))
        processes = []
        for key, wrapper in visitor.import_wrappers.items():
            code_str = astunparse.unparse(wrapper.get_statement()).strip()
            p = self._dump_external_dependencies_of_stmt(code_str, already_traversed)
            processes.append(p)
            self._store_arc('FILENAMES_TO_IMPORTS', 'filename_path', 'import_code_str', filename, code_str)
        for definition in visitor.definitions:
            definition_id = self._insert_definition(definition, filename)
            rejecting_vistor = RejectingVisitor(visitor.import_wrappers)
            rejecting_vistor.visit(definition)
            for wrapper in rejecting_vistor.get_used_import_names():
                code_str = astunparse.unparse(wrapper.get_statement()).strip()
                self._store_arc('DEFINITIONS_TO_IMPORTS', 'definition_id', 'import_code_str', definition_id, code_str)
        return processes

    def _get_packages_data_in_current_process(self, code_str, node_identifier):
        try:
            print(f'Collecting {node_identifier} "{code_str}"')
            modules_before = sys.modules.copy()
            a = exec(code_str)
            modules_after = sys.modules.copy()
            print(f'Collecting after {node_identifier} "{code_str}"')
        except Exception:
            print(traceback.format_exc())
            return
        records = []
        for key, module in modules_after.items():
            if not self.should_be_tracked(key, module, modules_before):
                if key in {'numpy', 'np'}:
                    print(module)
                continue
            record = [get_root_module(key), key]
            try:
                print(module)
                record.append(module.__file__)
            except:
                record.append(None)

            try:
                record.append(module.__version__)
            except:
                record.append(None)
            record.append(node_identifier)
            records.append(record)
        out_path = self.output_directory / f'transitive_imports'
        out_path.mkdir(exist_ok=True)
        with open(out_path / f'{code_str}.pkl', 'wb') as out_file:
            pickle.dump(records, out_file)
        print(f'Exiting {node_identifier} "{code_str}"')

    def _insert_code_str(self, code_str):
        return self._insert_unique('IMPORTS', 'code_str', code_str)

    def _insert_filename(self, filename):
        return self._insert_unique('FILENAMES', 'path', filename)

    def _insert_unique(self, table_name, col_name, value):
        with self._get_connection() as conn:
            query = f"""INSERT INTO {table_name}({col_name}) VALUES (?)"""
            c = conn.execute(query, [value])
            conn.commit()
            return c.lastrowid

    def _get_file_for_module_name(self, module_str):
        query = """
SELECT path
FROM IMPORT_DATA
WHERE module = :module"""
        with self._get_connection() as conn:
            c = conn.cursor()
            c.execute(query, {'module': module_str})
            row = c.fetchone()
            if row is None:
                return None
            return row[0]

    def _dump_external_dependencies_of_stmt(self, code_str, already_traversed):
        if code_str in already_traversed:
            print(f'Code string "{code_str}" has already been traversed, skipping.')
            return
        p = self._dump_package_data(code_str, code_str)
        already_traversed.add(code_str)
        return p

    def _store_arc(self, table_name, col0, col1, val0, val1):
        conn = self._get_connection()
        query = f"""INSERT INTO {table_name}({col0}, {col1}) VALUES (?,?)"""
        c = conn.cursor()
        c.execute(query, (val0, val1))
        conn.commit()

    def _insert_definition(self, definition, filename):
        with self._get_connection() as conn:
            query = f"""
INSERT INTO DEFINITIONS(type, name, start_no, end_no, filename_path) 
VALUES (?, ?, ?, ?, ?)
"""
            c = conn.cursor()
            c.execute(query, [str(type(definition).__name__),
                              definition.name,
                              definition.lineno,
                              definition.end_lineno,
                              filename])
            conn.commit()
            return c.lastrowid

    def _get_connection(self):
        db_path = self._get_db_path()
        should_init = not db_path.exists()
        conn = sqlite3.connect(self._get_db_path())
        if should_init:
            schema_path = Path(__file__).parent / 'schema.sql'
            with open(schema_path) as schema_file:
                conn.executescript(schema_file.read())
        return conn

    def _get_db_path(self):
        return self.output_directory / 'modules.db'

    def _dump_package_data(self, code_str, node_id):
        args = (code_str, node_id)
        p = Process(target=self._get_packages_data_in_current_process,
                    args=args,
                    )
        p.start()
        return p
