from azure.storage.blob import BlobServiceClient

#
class Storage:
    def __init__(self, account_name, account_key, container_name):
        service = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )
        self.container = service.get_container_client(container_name)

    def upload(self, path, content):
        """Guardar un archivo sin sobrescribir uno existente."""
        self.container.upload_blob(
            name=path,
            data=content,
            overwrite=False,
        )

    def download(self, path):
        """Leer el contenido de un archivo como bytes."""
        return self.container.download_blob(path).readall()
    
    def list_paths(self, prefix):
        """Listar rutas de archivos bajo un prefijo."""
        return [
            blob.name
            for blob in self.container.list_blobs(
                name_starts_with=prefix
            )
        ]