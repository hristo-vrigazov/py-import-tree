import ast
import os
import sqlite3
import sys
from multiprocessing import Process
from pathlib import Path
from sqlite3 import IntegrityError
from typing import Union

import astunparse
from stdlib_list import stdlib_list


def get_root_module(key):
    res = key.split('.')[0]
    return '-'.join(res.split('_'))


class AstImportsVisitor(ast.NodeVisitor):

    def __init__(self):
        self.imports = []
        self.import_froms = []

    def visit_Import(self, node: ast.Import):
        self.imports.append(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        self.import_froms.append(node)


def get_number_of_relative_step_backs(raw_module_str):
    n = 0
    for i in range(len(raw_module_str)):
        if raw_module_str[i] != '.':
            return n
        n += 1
    return n


class ImportTracker:

    def __init__(self, output_directory: Union[str, Path], blacklisting_function=None):
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

    def read_source_file(self, path_to_module):
        try:
            with open(path_to_module) as in_file:
                return in_file.read()
        except UnicodeError:
            return None

    def dump_tree_import_froms_stmt(self, import_froms_str, parent_node_id=None):
        node_id = self.insert_code_str(import_froms_str)
        if node_id < 0:
            print(f'Already inserted {node_id} "{import_froms_str}"')
            return
        self.store_arc(parent_node_id, node_id)
        self.dump_package_data(import_froms_str, node_id)
        import_froms_stmt = ast.parse(import_froms_str).body[0]
        path_to_module = self.get_file_for_module_name(import_froms_stmt.module)
        if path_to_module is None:
            print(f'Built-in {node_id} "{import_froms_str}"')
            return
        path_to_module = Path(path_to_module)
        source = self.read_source_file(path_to_module)
        if source is None:
            print(f'Not Python {node_id} "{import_froms_str}"')
            return
        visitor = AstImportsVisitor()
        visitor.visit(ast.parse(source))
        for child_stmt in visitor.import_froms:
            child_import_str = astunparse.unparse(child_stmt).strip()
            raw_module_str = child_import_str.split()[1]
            n_dots = child_stmt.level
            is_relative_import = n_dots > 0
            if is_relative_import:
                parent_modules = import_froms_stmt.module.split('.')
                first_from_part = parent_modules[:len(parent_modules) - n_dots + 1]
                second_from_part = raw_module_str[n_dots:].split('.')
                from_part = '.'.join(filter(lambda x: len(x) > 0, first_from_part + second_from_part))
                print(f'Relative "{import_froms_str}" "{child_import_str}" | {from_part}')
                child_stmt.module = from_part
                child_stmt.level = 0
                child_import_str = astunparse.unparse(child_stmt).strip()
                print(f'Transformed: "{child_import_str}"')
                self.dump_tree_import_froms_stmt(child_import_str, node_id)
            else:
                print('Absolute!')
                self.dump_tree_import_froms_stmt(child_import_str, node_id)
        #TODO handle regular imports
        #TODO: store references to the code (file, line number)

    def store_arc(self, parent_node_id, child_node_id):
        if parent_node_id is None:
            return
        conn = self.get_connection()
        query = """INSERT INTO ADJACENCY_TABLE(parent_id, child_id) VALUES (?,?)"""
        c = conn.cursor()
        c.execute(query, (parent_node_id, child_node_id))
        conn.commit()
