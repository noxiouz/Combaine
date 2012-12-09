try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

setup(
        name = "Combaine",
        packages = [
        'combaine',
        'combaine.combainer',
        'combaine.combainer.Schedule',
        'combaine.combainer.Observer',
        'combaine.plugins.Aggregators',
        'combaine.plugins.DataGrid',
        'combaine.plugins.DistributedStorage',
        'combaine.plugins.DataFetcher',
        'combaine.plugins.StorageAPI',
        'combaine.plugins.LockServerAPI',
        ]
)
