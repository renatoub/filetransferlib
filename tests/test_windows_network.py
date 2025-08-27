import unittest
import os
from filetransferlib import WindowsNetworkFolderClient

class TestWindowsNetworkFolderClient(unittest.TestCase):
    def setUp(self):
        # Criar uma pasta temporária para testes
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)
        self.client = WindowsNetworkFolderClient(base_path=self.test_dir)

        # Criar um arquivo de teste
        self.test_file_path = os.path.join(self.test_dir, 'testfile.txt')
        with open(self.test_file_path, 'w') as f:
            f.write('conteúdo de teste')

    def tearDown(self):
        # Remover arquivo e pasta de teste
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)
        if os.path.exists(self.test_dir):
            os.rmdir(self.test_dir)

    def test_list_files(self):
        files = self.client.list_files('')
        self.assertIn('testfile.txt', files)

    def test_download_upload_file(self):
        # Testar download para um arquivo local
        local_path = os.path.join(self.test_dir, 'downloaded.txt')
        self.client.download_file('testfile.txt', local_path)
        self.assertTrue(os.path.exists(local_path))

        # Testar upload de volta
        upload_path = 'uploaded.txt'
        self.client.upload_file(local_path, upload_path)
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, upload_path)))

        # Limpar arquivos criados
        os.remove(local_path)
        os.remove(os.path.join(self.test_dir, upload_path))

if __name__ == '__main__':
    unittest.main()
