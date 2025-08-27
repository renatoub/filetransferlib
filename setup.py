import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="filetransferlib",
    version="0.1.0",
    author="Seu Nome",
    author_email="seu.email@example.com",
    description="Biblioteca para transferência de arquivos entre Azure Data Lake e pastas de rede Windows.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/renatoub/filetransferlib",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
        "Topic :: System :: Filesystems",
        "Topic :: Utilities"
    ],
    python_requires='>=3.8',
    install_requires=[
        "azure-storage-file-datalake>=12.3.0"
    ],
)
