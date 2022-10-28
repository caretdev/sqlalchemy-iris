from setuptools import setup
import os

thelibFolder = os.path.dirname(os.path.realpath(__file__))
requirementPath = thelibFolder + '/requirements.txt'

requirements, dependency_links = [], []
if os.path.isfile(requirementPath):
    with open('./requirements.txt') as f:
        for line in f.read().splitlines():
            if line.startswith('http'):
                dependency_links.append(line)
            else:
                requirements.append(line)

setup(
    install_requires=requirements,
    dependency_links=dependency_links,
    entry_points={
        "sqlalchemy.dialects": [
            "iris = sqlalchemy_iris.iris:IRISDialect_iris",
        ]
    },
)
