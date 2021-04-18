from setuptools import setup, find_packages

setup(name="my_mess_server",
      version="0.0.1",
      description="mess_server",
      author="Tatiana Grishechkina",
      author_email="tatiangr@amdocs.com",
      packages=find_packages(),
      install_requires=['PyQt5', 'sqlalchemy', 'pycryptodome', 'pycryptodomex']
      )
