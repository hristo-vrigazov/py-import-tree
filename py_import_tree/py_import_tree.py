import os
import sys
import ast
from collections import defaultdict
from dataclasses import dataclass, field

import astunparse

from multiprocessing import Process
from pathlib import Path
from typing import Union

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


@dataclass
class TraversalData:
    adjacency_list: defaultdict = field(default_factory=lambda: defaultdict(list))
    nodes: dict = field(default_factory=dict)
    node_name_to_code_str: dict = field(default_factory=dict)


def should_traverse(traversal_state):
    if traversal_state is None:
        return True
    if traversal_state == 'in_process':
        return False
    if traversal_state == 'processed':
        return False
    return True


def get_working_directory(import_from_stmt):
    pass


class ImportTracker:

    def __init__(self, output_directory: Union[str, Path], blacklisting_function=None, hash_function=None):
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(exist_ok=True)
        self.stdlib_packages_set = set(stdlib_list())
        self.blacklisting_function = blacklisting_function
        self.hash_function = hash if hash_function is None else hash_function

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

    def get_packages_data_in_current_process(self, code_str, work_directory=None):
        if work_directory is not None:
            os.chdir(work_directory)
        modules_before = sys.modules.copy()
        a = exec(code_str)
        modules_after = sys.modules.copy()
        records = []
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
            records.append(record)
        import sqlite3
        db_path = self.output_directory / 'modules.db'
        should_init = not db_path.exists()
        conn = sqlite3.connect(self.output_directory / 'modules.db')
        if should_init:
            schema_path = Path(__file__).parent / 'schema.sql'
            with open(schema_path) as schema_file:
                conn.executescript(schema_file.read())
        query = """INSERT INTO IMPORT_DATA(root, module, path, version) VALUES (?,?,?,?);"""
        c = conn.cursor()
        c.executemany(query, records)
        conn.commit()

    def dump_package_data(self, code_str, working_directory=None):
        args = (code_str, working_directory)
        p = Process(target=self.get_packages_data_in_current_process, args=args)
        p.start()
        p.join()

    def find_import_tree(self, code_str):
        root_hash = self.hash_function(code_str)
        hashes = [root_hash]
        code_strs = [code_str]
        self.dump_package_data(code_str, root_hash)
        traversal_data = TraversalData()
        traversal_data.node_name_to_code_str[root_hash] = code_str
        new_hashes, new_code_strs = self.traverse_ast_imports(code_str, root_hash, traversal_data)
        hashes += new_hashes
        code_strs += new_code_strs
        res = {
            'hashes': hashes,
            'code_strs': code_strs
        }
        import pandas as pd
        df = pd.DataFrame(res)
        df.to_csv(self.output_directory / '_nodes_mapping.csv')

    def traverse_ast_imports(self, code_str, code_hash, traversal_data):
        ast_module = ast.parse(code_str)
        visitor = AstImportsVisitor()
        visitor.visit(ast_module)
        for import_from_stmt in visitor.import_froms:
            child_str = astunparse.unparse(import_from_stmt).strip()
            child_hash = self.hash_function(child_str)
            traversal_data.adjacency_list[code_hash].append(child_hash)
            traversal_state = traversal_data.nodes.get(child_hash)
            if not should_traverse(traversal_state):
                continue
            traversal_data.nodes[child_hash] = 'in_process'
            working_directory = get_working_directory(import_from_stmt)
            self.dump_package_data(child_str, child_hash, working_directory)
            self.traverse_ast_imports(child_str, child_hash, traversal_data)
            traversal_data.nodes[child_hash] = 'processed'




