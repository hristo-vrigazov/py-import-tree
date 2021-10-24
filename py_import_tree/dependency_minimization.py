import ast


def get_eff_name(alias):
    return alias.asname if alias.asname is not None else alias.name


class DefinitionTracer(ast.NodeVisitor):

    def __init__(self):
        self.function_definitions = []
        self.class_definitions = []
        self.imported_names = {}

    def visit_FunctionDef(self, node):
        self.function_definitions.append(node)

    def visit_ImportFrom(self, node):
        for alias in node.names:
            eff_name = get_eff_name(alias)
            self.imported_names[eff_name] = node
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            eff_name = get_eff_name(alias)
            self.imported_names[eff_name] = node
        self.generic_visit(node)

    def visit_ClassDef(self, node):
        self.class_definitions.append(node)


class RejectingVisitor(ast.NodeVisitor):

    def __init__(self, imported_names):
        self.imported_names = imported_names
        self.used = []

    def visit_Name(self, name):
        if name.id not in self.imported_names:
            self.generic_visit(name)
            return
        if name.lineno < self.imported_names[name.id].lineno:
            self.generic_visit(name)
            return
        self.used.append(name)

    def get_unused_import_names(self):
        used_set = set(u.id for u in self.used)
        imported_set = set(self.imported_names.keys())
        return imported_set.difference(used_set)

    def get_used_import_names(self):
        return set(u.id for u in self.used)


def get_function_and_class_import_dependencies(source):
    module = ast.parse(source)
    tracer = DefinitionTracer()
    tracer.visit(module)
    res = {}
    for definitions in [tracer.function_definitions, tracer.class_definitions]:
        for definition in definitions:
            rejecting_vistor = RejectingVisitor(tracer.imported_names)
            rejecting_vistor.visit(definition)
            res[definition] = rejecting_vistor.get_used_import_names()
    return res, tracer.imported_names

