import ast
import os
import sqlite3
import sys
from copy import copy
from multiprocessing import Process
from pathlib import Path
from sqlite3 import IntegrityError
from typing import Union, List

import astunparse
from stdlib_list import stdlib_list


def get_root_module(key):
    res = key.split('.')[0]
    return '-'.join(res.split('_'))


class Wrapper:

    def get_root(self):
        raise NotImplementedError()

    def get_module(self):
        raise NotImplementedError()

    def get_statement(self):
        raise NotImplementedError()


class ImportWrapper(Wrapper):

    def __init__(self, import_stmt: ast.Import, name_idx: int):
        self.import_stmt = import_stmt
        self.name_idx = name_idx

    def get_root(self):
        pass

    def get_module(self):
        pass

    def get_statement(self):
        res = copy(self.import_stmt)
        res.names = [self.import_stmt.names[self.name_idx]]
        return res


class ImportFromWrapper(Wrapper):

    def __init__(self, import_from_stmt: ast.ImportFrom, name_idx: int):
        self.import_from_stmt = import_from_stmt
        self.name_idx = name_idx

    def get_root(self):
        pass

    def get_module(self):
        pass

    def get_statement(self):
        res = copy(self.import_from_stmt)
        res.names = [self.import_from_stmt.names[self.name_idx]]
        return res


class AstImportsVisitor(ast.NodeVisitor):

    def __init__(self):
        self.import_wrappers = []

    def visit_Import(self, node: ast.Import):
        for i in range(len(node.names)):
            self.import_wrappers.append(ImportWrapper(node, i))

    def visit_ImportFrom(self, node: ast.ImportFrom):
        for i in range(len(node.names)):
            self.import_wrappers.append(ImportFromWrapper(node, i))


def read_source_file(path_to_module):
    try:
        with open(path_to_module) as in_file:
            return in_file.read()
    except UnicodeError:
        return None


class ImportTracker:

    def __init__(self, packages_to_keep_traversing: List[str],
                 output_directory: Union[str, Path],
                 blacklisting_function=None):
        self.packages_to_keep_traversing = packages_to_keep_traversing
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

    def get_packages_data_in_current_process(self, code_str, node_identifier):
        print(f'Collecting {node_identifier} "{code_str}"')
        modules_before = sys.modules.copy()
        a = exec(code_str)
        modules_after = sys.modules.copy()
        print(f'Collecting after {node_identifier} "{code_str}"')
        conn = self.get_connection()
        for key, module in modules_after.items():
            if not self.should_be_tracked(key, module, modules_before):
                continue
            record = [get_root_module(key), key]
            try:
                record.append(module.__file__)
            except:
                record.append(None)

            try:
                record.append(module.__version__)
            except:
                record.append(None)
            record.append(node_identifier)
            query = """INSERT OR IGNORE INTO IMPORT_DATA(root, module, path, version, node_id) VALUES (?,?,?,?,?)"""
            c = conn.cursor()
            c.execute(query, record)
            conn.commit()
        print(f'Exiting {node_identifier} "{code_str}"')

    def get_connection(self):
        db_path = self.get_db_path()
        should_init = not db_path.exists()
        conn = sqlite3.connect(self.get_db_path())
        if should_init:
            schema_path = Path(__file__).parent / 'schema.sql'
            with open(schema_path) as schema_file:
                conn.executescript(schema_file.read())
        return conn

    def insert_code_str(self, code_str):
        try:
            with self.get_connection() as conn:
                c = conn.cursor()
                query = """INSERT INTO NODES(code_str) VALUES (?)"""
                c.execute(query, [code_str])
                conn.commit()
                return c.lastrowid
        except IntegrityError as e:
            return -1

    def get_file_for_module_name(self, module_str):
        query = """
SELECT path
FROM IMPORT_DATA
WHERE module = :module"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute(query, {'module': module_str})
            row = c.fetchone()
            if row is None:
                return None
            return row[0]

    def get_db_path(self):
        return self.output_directory / 'modules.db'

    def dump_package_data(self, code_str, node_id):
        args = (code_str, node_id)
        p = Process(target=self.get_packages_data_in_current_process, args=args)
        p.start()
        p.join()

    def dump_external_dependencies(self, code_str):
        visitor = AstImportsVisitor()
        visitor.visit(ast.parse(code_str))
        for wrapper in visitor.import_wrappers:
            self.dump_external_dependencies_of_stmt(wrapper)

    def dump_external_dependencies_of_stmt(self, import_wrapper: Wrapper):
        code_str = astunparse.unparse(import_wrapper.get_statement()).strip()
        node_id = self.insert_code_str(code_str)
        self.dump_package_data(code_str, node_id)

