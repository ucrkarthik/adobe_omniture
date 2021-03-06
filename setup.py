from typing import List

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

from os import path, getcwd
from setuptools import setup, find_packages

package_name = 'adobe-omniture'

# Get the version value from the VERSION file
try:
    with open(path.join(getcwd(), 'VERSION')) as version_file:
        version = version_file.read().strip()
except IOError:
    raise

def parse_requirements(file: str)->List[str]:
    """
    Parse the requirements file and return a list of packages to install
    :param file: path to the requirements file.
    :return: list of
    """
    with open(file, "r") as fs:
        return [r for r in fs.read().splitlines() if
                (len(r.strip()) > 0 and not r.strip().startswith("#") and not r.strip().startswith("--"))]


requirements = parse_requirements('requirements.txt')

setup(name=package_name,
      version=version,
      description='Seach Enging Revenu',
      author='Karthik Venkatesan',
      author_email='ucrkarthik@gmail.com',
      url='https://github.com/ucrkarthik/adobe_omniture',
      packages=find_packages(exclude=['tests']),
      install_requires=requirements,
      include_package_data=True,
      zip_safe=False)
