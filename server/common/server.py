import socket
import logging
import signal
from common.protocol import receive_message, send_bet_response, STATUS_OK, STATUS_ERROR
from common.utils import store_bets, Bet as UtilsBet


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self._client_socket = None
        
        signal.signal(signal.SIGTERM, self._signal_handler)

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        try:
            while self._running:
                try:
                    client_sock = self.__accept_new_connection()
                    if client_sock:
                        self._client_sockets = client_sock
                        self.__handle_client_connection(client_sock)
                        self._client_sockets = None
                except socket.error as e:
                    if self._running:
                        logging.error(f"action: accept_connection | result: fail | error: {e}")
                    break
        finally:
            self._cleanup()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket   
        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            bet_request = receive_message(client_sock)
            
            year = bet_request.nacimiento // 10000
            month = (bet_request.nacimiento // 100) % 100  
            day = bet_request.nacimiento % 100
            
            utils_bet = UtilsBet(
                agency=str(bet_request.agencia_id), 
                first_name=bet_request.nombre,
                last_name=bet_request.apellido,
                document=str(bet_request.documento),
                birthdate=f"{year:04d}-{month:02d}-{day:02d}",
                number=str(bet_request.numero)
            )
            

            store_bets([utils_bet])
            
            logging.info(f'action: apuesta_almacenada | result: success | dni: {bet_request.documento} | numero: {bet_request.numero}')
            
            send_bet_response(client_sock, STATUS_OK, "Apuesta registrada exitosamente")
                
        except (ConnectionError, socket.error) as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
            
        except ValueError as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
            send_bet_response(client_sock, STATUS_ERROR, "Datos de apuesta inválidos")
                
        except Exception as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
            send_bet_response(client_sock, STATUS_ERROR, "Error al procesar apuesta")  
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        try:
            logging.info('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except socket.error as e:
            if self._running:
                logging.error(f'action: accept_connections | result: fail | error: {e}')
            return None
    
    def _signal_handler(self, sig, frame):
        """Handles SIGTERM signal for graceful shutdown"""
        logging.info('action: signal_received | result: success | signal: SIGTERM')
        self._running = False
        
        if self._server_socket:
            self._server_socket.close()
        
        if self._client_socket:
            self._client_socket.shutdown(socket.SHUT_RDWR)
            self._client_socket.close()

    def _cleanup(self):
        """Cleans up resources during shutdown"""
        logging.info('action: shutdown_server | result: in_progress')
        if self._running and self._server_socket:
            self._server_socket.close()
            logging.info('action: close_server_socket | result: success')
        logging.info('action: shutdown_server | result: success')
