from setuptools import setup, find_packages

setup(
    name="hacom",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'flask',
        'flask-jwt-extended',
        'pymongo',
        'psycopg2-binary',
        'python-dotenv',
        'gunicorn',
        'flask-cors'
    ]
) 