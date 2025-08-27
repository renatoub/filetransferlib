# README.md

# filetransferlib

Biblioteca Python para transferência de arquivos entre Azure Data Lake Storage Gen2 e pastas de rede Windows.

## Instalação

```bash
pip install filetransferlib
```

## Uso

Exemplo básico:

```python
from filetransferlib import create_storage_client, FileTransferManager

# Criar clientes para fonte e destino
source = create_storage_client('azure_datalake', account_url='https://<account>.dfs.core.windows.net', credential='<key>')
dest = create_storage_client('windows_network', base_path='\\\server\share')

# Criar gerenciador de transferência
manager = FileTransferManager(source, dest)

# Transferir arquivos de um diretório
manager.transfer_files('filesystem/directory', 'destination_directory')
```

## Testes

Para rodar os testes:

```bash
pytest
```

## Licença

MIT

