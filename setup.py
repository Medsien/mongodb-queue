import os
from distutils.core import setup

try:
    from pypandoc import convert_file

    def get_long_description():
        return convert_file('README.md', 'rst')

except Exception:

    def get_long_description():
        pass


def get_package_data(package):
    start = len(package) + 1  # strip package name
    for path, dirs, files in os.walk(package):
        for file in files:
            if file.startswith('.') or file.endswith('.py') or file.endswith('.pyc'):
                continue
            yield os.path.join(path[start:], file)


setup(
    name='mongodb_queue',
    version='0.1.0',
    description='Mongo-based message queue',
    long_description=get_long_description(),
    author='Medsien, Inc.',
    author_email='hello@medsien.com',
    url='https://github.com/Medsien/mongodb-queue',
    packages=[
        'mongodb_queue',
    ],
    package_data={
        'mongodb_queue': list(get_package_data('mongodb_queue')),
    },
    keywords=['mongo', 'mongodb', 'pymongo', 'queue'],
)
