import uuid
import time
import threading
from typing import List, Dict, Any, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

class clusterManager:
    def __init__(self, initial_nodes: List[Dict[str, Any]] = None):
        """
        Inicializa o gerenciador de cluster

        Args:
            initial_nodes (List[Dict[str, Any]], optional): Nós iniciais do cluster
        """
        # Configuração de logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        
        # Estrutura de nós do cluster
        self.nodes: List[Dict[str, Any]] = initial_nodes or []
        
        # Locks para operações thread-safe
        self._nodes_lock = threading.Lock()
        self._health_check_lock = threading.Lock()
        
        # Configurações de cluster
        self.node_timeout = 30  # segundos
        self.health_check_interval = 60  # segundos
        
        # Inicia thread de verificação de saúde
        self._start_health_monitoring()
    
    def add_node(self, node_info: Dict[str, Any]) -> str:
        """
        Adiciona um novo nó ao cluster

        Args:
            node_info (Dict[str, Any]): Informações do nó

        Returns:
            str: ID do nó adicionado
        """
        with self._nodes_lock:
            # Gera ID único para o nó
            node_id = str(uuid.uuid4())
            node_info['id'] = node_id
            node_info['last_heartbeat'] = time.time()
            node_info['status'] = 'active'
            
            self.nodes.append(node_info)
            
            logging.info(f"Nó adicionado ao cluster: {node_id}")
            return node_id
    
    def remove_node(self, node_id: str):
        """
        Remove um nó do cluster

        Args:
            node_id (str): ID do nó a ser removido
        """
        with self._nodes_lock:
            self.nodes = [
                node for node in self.nodes 
                if node['id'] != node_id
            ]
            
            logging.warning(f"Nó removido do cluster: {node_id}")
    
    def get_healthy_nodes(self, min_nodes: int = 1) -> List[Dict[str, Any]]:
        """
        Recupera nós saudáveis do cluster

        Args:
            min_nodes (int, optional): Número mínimo de nós saudáveis

        Returns:
            List[Dict[str, Any]]: Lista de nós saudáveis
        """
        current_time = time.time()
        
        # Filtra nós saudáveis
        healthy_nodes = [
            node for node in self.nodes
            if (node['status'] == 'active' and 
                current_time - node['last_heartbeat'] < self.node_timeout)
        ]
        
        if len(healthy_nodes) < min_nodes:
            logging.warning(f"Número de nós saudáveis abaixo do mínimo: {len(healthy_nodes)}")
            raise Exception(f"Cluster não possui {min_nodes} nós saudáveis")
        
        return healthy_nodes
    
    def select_node_for_operation(
        self, 
        operation_type: str = 'default'
    ) -> Dict[str, Any]:
        """
        Seleciona nó para operação usando estratégia Round-Robin

        Args:
            operation_type (str, optional): Tipo de operação

        Returns:
            Dict[str, Any]: Nó selecionado
        """
        with self._nodes_lock:
            # Recupera nós saudáveis
            healthy_nodes = self.get_healthy_nodes()
            
            # Round-Robin simples
            selected_node = healthy_nodes[0]
            
            # Move nó selecionado para o final da lista
            self.nodes.append(self.nodes.pop(0))
            
            logging.info(f"Nó selecionado para {operation_type}: {selected_node['id']}")
            return selected_node
    
    def _health_check(self):
        """
        Realiza verificação de saúde dos nós
        Método interno executado em thread separada
        """
        while True:
            try:
                with self._health_check_lock:
                    current_time = time.time()
                    
                    for node in self.nodes:
                        # Verifica tempo desde último heartbeat
                        if current_time - node['last_heartbeat'] > self.node_timeout:
                            node['status'] = 'inactive'
                            logging.warning(f"Nó inativo detectado: {node['id']}")
                
                # Intervalo entre verificações
                time.sleep(self.health_check_interval)
            
            except Exception as e:
                logging.error(f"Erro na verificação de saúde: {e}")
                time.sleep(self.health_check_interval)
    
    def _start_health_monitoring(self):
        """
        Inicia thread de monitoramento de saúde
        """
        health_thread = threading.Thread(
            target=self._health_check, 
            daemon=True
        )
        health_thread.start()
    
    def perform_distributed_operation(
        self, 
        operation: callable, 
        data: Any, 
        max_concurrent_nodes: int = 3
    ) -> List[Any]:
        """
        Executa operação distribuída em múltiplos nós

        Args:
            operation (callable): Função a ser executada
            data (Any): Dados para operação
            max_concurrent_nodes (int, optional): Máximo de nós simultâneos

        Returns:
            List[Any]: Resultados das operações
        """
        try:
            # Recupera nós saudáveis
            healthy_nodes = self.get_healthy_nodes()
            
            # Resultados das operações
            results = []
            
            # Execução em paralelo
            with ThreadPoolExecutor(max_workers=max_concurrent_nodes) as executor:
                # Mapeia futures para nós
                futures = {
                    executor.submit(operation, node, data): node 
                    for node in healthy_nodes[:max_concurrent_nodes]
                }
                
                # Processa resultados
                for future in as_completed(futures):
                    node = futures[future]
                    try:
                        result = future.result()
                        results.append({
                            'node_id': node['id'],
                            'result': result,
                            'status': 'success'
                        })
                    except Exception as e:
                        results.append({
                            'node_id': node['id'],
                            'error': str(e),
                            'status': 'failed'
                        })
            
            return results
        
        except Exception as e:
            logging.error(f"Erro em operação distribuída: {e}")
            raise

# Exemplo de uso
if __name__ == "__main__":
    # Exemplo de nós iniciais
    initial_nodes = [
        {
            'host': 'node1.example.com',
            'port': 8000,
            'storage_capacity': 1000  # GB
        },
        {
            'host': 'node2.example.com',
            'port': 8001,
            'storage_capacity': 1500  # GB
        }
    ]
    
    # Inicializa cluster manager
    cluster_manager = ClusterManager(initial_nodes)
    
    try:
        # Adiciona novo nó
        new_node_id = cluster_manager.add_node({
            'host': 'node3.example.com',
            'port': 8002,
            'storage_capacity': 2000  # GB
        })
        
        # Seleciona nó para operação
        selected_node = cluster_manager.select_node_for_operation('upload')
        print(f"Nó selecionado: {selected_node}")
        
        # Recupera nós saudáveis
        healthy_nodes = cluster_manager.get_healthy_nodes()
        print(f"Nós saudáveis: {len(healthy_nodes)}")
    
    except Exception as e:
        print(f"Erro no cluster manager: {e}")
