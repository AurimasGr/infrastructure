import inspect
import os

import cluster
from spark_livy_session import Session


class EmrSpark:

    def __init__(self):

        self.Cluster = getattr(cluster, os.getenv("EMR_CONFIG_VERSION"))

    @staticmethod
    def verbose_function(func, *args, print_func=True, dependencies=[], imports=[], **kwargs):
        def _rename_classes(data):
            if isinstance(data, dict):
                return inspect.getsource(data['class']).replace(data['class_name'], data['class_name_replacement'])
            else:
                return inspect.getsource(data)

        def _construct_modules(data):
            modules = []
            for class_holder in data:
                if isinstance(class_holder, dict):
                    modules.append(
                        f"\'{class_holder['class_name_replacement']}\': " + class_holder['class_name_replacement'])
            if len(modules) > 0:
                return ["modules = {" + ",\n".join(modules) + "}\n"]
            else:
                return []

        def _construct_imports(data):
            if len(data) > 0:
                return ["\n".join(data) + "\n"]
            else:
                return []

        func_dependencies = [f"{_rename_classes(data)}\n\n" for data in dependencies]
        func_decorator = inspect.getsource(func).split('\n')[0]
        func_code = inspect.getsource(func).split('\n')[1:]
        arguments_raw = [('', arg) for arg in args] + [(f'{key}=', value) for key, value in kwargs.items()]
        arguments = ', '.join(f'{k}\'{v}\'' if isinstance(v, str) else f'{k}{v}' for k, v in arguments_raw)
        func_call = f'{func.__name__}({arguments})'
        runnable_combination = '\n'.join(
            _construct_imports(imports) + func_dependencies + _construct_modules(dependencies) + func_code + [
                func_call])
        if print_func:
            print(runnable_combination)

        return runnable_combination

    def livy_spark(self, **kwargs_cluster):

        Cluster = self.Cluster

        def decorator(func):
            def wrapper(*args, dependencies=[], **kwargs):
                with Cluster(**kwargs_cluster) as emrcluster:
                    with Session(emrcluster.dns) as session:
                        session.submit(self.verbose_function(func, *args, dependencies=dependencies, **kwargs))
            return wrapper
        return decorator

    def livy_spark_dry(self, **kwargs_cluster):

        def decorator(func):
            def wrapper(*args, dependencies=[], **kwargs):
                print("______________________________________________________")
                print("EMR Cluster arguments:\n")
                for k in kwargs_cluster:
                    print(f"===================={k}====================\n\n{kwargs_cluster[k]} \n")
                print("______________________________________________________")
                print("Code that will be sent to Livy server:\n\n")
                print("<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>\n")
                print(self.verbose_function(func, *args, print_func=False, dependencies=dependencies, **kwargs))
                print("\n<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>")
            return wrapper
        return decorator
