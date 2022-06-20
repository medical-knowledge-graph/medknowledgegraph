from setuptools import setup

setup(
    name='pymedgraph',
    version='0.0.1',
    description='Build graph from med abstracts and more',
    packages=[
        'pymedgraph',
        'pymedgraph.input',
        'pymedgraph.dataextraction',
        'pymedgraph.graph'
    ],
    install_requires=[''],
    package_data={},
    include_package_data=True
)