import time
import statistics
from typing import List, Dict, Any, Callable
import threading
import logging

class PerformanceMonitor:
    def __init__(self, log_file: str = 'performance_log.txt'):
        """
        Inicializa o monitor de desempenho

        Args:
            log_file (str, optional): Caminho para arquivo de log
        """
        # Configuração de logging
        logging.basicConfig(
            filename=log_file, 
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        
        # Estruturas para armazenar métricas
        self.upload_times: List[float] = []
        self.download_times: List[float] = []
        self.list_times: List[float] = []
        self.delete_times: List[float] = []
        
        # Lock para thread-safety
        self._lock = threading.Lock()
    
    def measure_execution_time(
        self, 
        operation: Callable, 
        *args, 
        operation_type: str = 'generic',
        **kwargs
    ) -> Any:
        """
        Mede tempo de execução de uma operação

        Args:
            operation (Callable): Função a ser medida
            operation_type (str): Tipo de operação para categorização
            *args, **kwargs: Argumentos para a operação

        Returns:
            Any: Resultado da operação
        """
        try:
            # Inicia contagem de tempo
            start_time = time.time()
            
            # Executa operação
            result = operation(*args, **kwargs)
            
            # Calcula tempo de execução
            execution_time = time.time() - start_time
            
            # Armazena tempo de acordo com tipo de operação
            with self._lock:
                if operation_type == 'upload':
                    self.upload_times.append(execution_time)
                elif operation_type == 'download':
                    self.download_times.append(execution_time)
                elif operation_type == 'list':
                    self.list_times.append(execution_time)
                elif operation_type == 'delete':
                    self.delete_times.append(execution_time)
            
            # Registra log
            logging.info(
                f"{operation_type.capitalize()} Operation: "
                f"Execution Time = {execution_time:.4f} seconds"
            )
            
            return result
        
        except Exception as e:
            logging.error(f"Error in {operation_type} operation: {e}")
            raise
    
    def get_performance_metrics(self) -> Dict[str, Dict[str, float]]:
        """
        Recupera métricas de desempenho

        Returns:
            Dict[str, Dict[str, float]]: Estatísticas de desempenho
        """
        def calculate_metrics(times: List[float]) -> Dict[str, float]:
            """Calcula métricas estatísticas"""
            if not times:
                return {
                    'count': 0,
                    'min': 0,
                    'max': 0,
                    'mean': 0,
                    'median': 0,
                    'std_dev': 0
                }
            
            return {
                'count': len(times),
                'min': min(times),
                'max': max(times),
                'mean': statistics.mean(times),
                'median': statistics.median(times),
                'std_dev': statistics.stdev(times) if len(times) > 1 else 0
            }
        
        return {
            'upload': calculate_metrics(self.upload_times),
            'download': calculate_metrics(self.download_times),
            'list': calculate_metrics(self.list_times),
            'delete': calculate_metrics(self.delete_times)
        }
    
    def benchmark_operations(
        self, 
        client, 
        num_iterations: int = 10
    ) -> Dict[str, Any]:
        """
        Realiza benchmark de operações do cliente

        Args:
            client: Cliente MyGeoEye
            num_iterations (int): Número de iterações para cada operação

        Returns:
            Dict[str, Any]: Resultados do benchmark
        """
        benchmark_results = {
            'upload': [],
            'download': [],
            'list': [],
            'delete': []
        }
        
        try:
            # Benchmark de upload
            for _ in range(num_iterations):
                upload_time = self.measure_execution_time(
                    client.upload_image, 
                    "/caminho/exemplo/imagem.png",
                    operation_type='upload'
                )
                benchmark_results['upload'].append(upload_time)
            
            # Benchmark de listagem
            for _ in range(num_iterations):
                self.measure_execution_time(
                    client.list_images,
                    operation_type='list'
                )
            
            # Benchmark de download
            for _ in range(num_iterations):
                self.measure_execution_time(
                    client.download_image, 
                    benchmark_results['upload'][0],
                    operation_type='download'
                )
            
            # Benchmark de deleção
            for _ in range(num_iterations):
                self.measure_execution_time(
                    client.delete_image, 
                    benchmark_results['upload'][0],
                    operation_type='delete'
                )
            
            return self.get_performance_metrics()
        
        except Exception as e:
            logging.error(f"Benchmark failed: {e}")
            raise
    
    def reset_metrics(self):
        """
        Reseta todas as métricas coletadas
        """
        with self._lock:
            self.upload_times.clear()
            self.download_times.clear()
            self.list_times.clear()
            self.delete_times.clear()
        
        logging.info("Performance metrics reset")

# Exemplo de uso
if __name__ == "__main__":
    # Importações para exemplo
    from src.client import Client
    
    # Inicializa monitor de desempenho
    performance_monitor = PerformanceMonitor()
    
    try:
        # Inicializa cliente
        client = Client()
        
        # Realiza benchmark
        benchmark_results = performance_monitor.benchmark_operations(
            client, 
            num_iterations=5
        )
        
        # Exibe resultados
        print("Métricas de Desempenho:")
        for operation, metrics in benchmark_results.items():
            print(f"\n{operation.capitalize()} Metrics:")
            for metric, value in metrics.items():
                print(f"{metric}: {value}")
    
    except Exception as e:
        print(f"Erro no benchmark: {e}")
