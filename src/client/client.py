import os
import uuid
from typing import List, Dict, Any
import grpc  # Supondo uso de gRPC para comunicação

# Importações hipotéticas - ajuste conforme sua implementação
from src.server.server_pb2 import UploadRequest, DownloadRequest, ListImagesRequest
from src.server.server_pb2_grpc import ServerStub

class Client:
    def __init__(self, server_address: str = 'localhost:50051'):
        """
        Inicializa o cliente com conexão ao servidor

        Args:
            server_address (str): Endereço do servidor gRPC
        """
        try:
            # Estabelece canal de comunicação gRPC
            self.channel = grpc.insecure_channel(server_address)
            self.stub = ServerStub(self.channel)
            
            # Diretório padrão para downloads
            self.download_path = os.path.join(
                os.path.expanduser('~'), 
                'Downloads', 
                'MyGeoEye'
            )
            os.makedirs(self.download_path, exist_ok=True)
        
        except Exception as e:
            print(f"Erro ao conectar ao servidor: {e}")
            raise
    
    def upload_image(self, image_path: str) -> str:
        """
        Faz upload de imagem de satélite

        Args:
            image_path (str): Caminho completo da imagem

        Returns:
            str: ID da imagem no sistema
        """
        try:
            # Verifica se arquivo existe
            if not os.path.exists(image_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {image_path}")
            
            # Lê conteúdo da imagem
            with open(image_path, 'rb') as image_file:
                image_data = image_file.read()
            
            # Gera ID único para imagem
            image_id = str(uuid.uuid4())
            
            # Prepara requisição de upload
            request = UploadRequest(
                image_id=image_id,
                image_data=image_data,
                filename=os.path.basename(image_path)
            )
            
            # Envia para o servidor
            response = self.stub.Upload(request)
            
            return image_id
        
        except Exception as e:
            print(f"Erro no upload da imagem: {e}")
            raise
    
    def download_image(self, image_id: str) -> str:
        """
        Baixa imagem do sistema

        Args:
            image_id (str): ID da imagem a ser baixada

        Returns:
            str: Caminho local do arquivo baixado
        """
        try:
            # Prepara requisição de download
            request = DownloadRequest(image_id=image_id)
            
            # Solicita download ao servidor
            response = self.stub.Download(request)
            
            # Caminho para salvar imagem
            local_path = os.path.join(
                self.download_path, 
                f"{image_id}_satellite.png"
            )
            
            # Salva imagem localmente
            with open(local_path, 'wb') as image_file:
                image_file.write(response.image_data)
            
            return local_path
        
        except Exception as e:
            print(f"Erro no download da imagem: {e}")
            raise
    
    def list_images(self) -> List[Dict[str, Any]]:
        """
        Lista imagens disponíveis no sistema

        Returns:
            List[Dict[str, Any]]: Lista de informações das imagens
        """
        try:
            # Prepara requisição de listagem
            request = ListImagesRequest()
            
            # Solicita lista de imagens ao servidor
            response = self.stub.ListImages(request)
            
            # Converte resposta para lista de dicionários
            images = [
                {
                    'image_id': img.image_id,
                    'filename': img.filename,
                    'upload_date': img.upload_date
                } for img in response.images
            ]
            
            return images
        
        except Exception as e:
            print(f"Erro ao listar imagens: {e}")
            raise
    
    def delete_image(self, image_id: str):
        """
        Deleta imagem do sistema

        Args:
            image_id (str): ID da imagem a ser deletada
        """
        try:
            # Prepara requisição de deleção
            request = DeleteRequest(image_id=image_id)
            
            # Solicita deleção ao servidor
            self.stub.Delete(request)
        
        except Exception as e:
            print(f"Erro ao deletar imagem: {e}")
            raise
    
    def close_connection(self):
        """
        Fecha canal de comunicação
        """
        self.channel.close()

# Exemplo de uso
if __name__ == "__main__":
    try:
        # Inicializa cliente
        client = Client()
        
        # Upload de imagem
        image_path = "/caminho/para/imagem.png"
        image_id = client.upload_image(image_path)
        print(f"Imagem uploadada com ID: {image_id}")
        
        # Lista imagens
        images = client.list_images()
        for img in images:
            print(f"Imagem: {img['filename']} - ID: {img['image_id']}")
        
        # Download de imagem
        download_path = client.download_image(image_id)
        print(f"Imagem baixada em: {download_path}")
        
        # Deleta imagem
        client.delete_image(image_id)
        print("Imagem deletada com sucesso")
    
    except Exception as e:
        print(f"Erro no cliente: {e}")
    finally:
        client.close_connection()
