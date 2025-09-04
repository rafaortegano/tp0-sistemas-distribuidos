import socket
import logging
import signal
import os
import threading
from common.protocol import (
    send_bet_response, send_winners_response,
    recv_exactly, bytes_to_uint32_be, parse_batch_request, parse_query_winners_request,
    MSG_TYPE_BATCH, MSG_TYPE_QUERY_WINNERS,
    STATUS_OK, STATUS_ERROR
)
from common.utils import store_bets, load_bets, has_won, Bet as UtilsBet


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        
        self._expected_agencies = int(os.getenv('EXPECTED_AGENCIES', '5'))
        self._finished_agencies = set()
        self._sorteo_realizado = False
        self._winners_by_agency = {} 
        
        self._lock = threading.Lock()
        self._active_threads = {}
        
        signal.signal(signal.SIGTERM, self._signal_handler)

    def run(self):
        """
        Threaded Server loop

        Server that accepts new connections and creates a thread for each client.
        Each client connection is handled in a concurrent way.
        """

        try:
            while self._running:
                try:
                    client_sock = self.__accept_new_connection()
                    if client_sock:
                        client_thread = threading.Thread(
                            target=self.__handle_client_connection, 
                            args=(client_sock,)
                        )
                        client_thread.start()
                        
                        self._active_threads[client_thread] = client_sock
        
                except socket.error as e:
                    if self._running:
                        logging.error(f"action: accept_connection | result: fail | error: {e}")
                    break
        finally:
            self._cleanup()

    def _receive_message(self, client_sock):
        """Receive and return message type and complete message"""
        length_bytes = recv_exactly(client_sock, 4)
        if not length_bytes:
            return None, None
            
        length = bytes_to_uint32_be(length_bytes)
        payload = recv_exactly(client_sock, length)
        complete_message = length_bytes + payload
        
        if len(payload) < 1:
            raise ValueError("Empty message payload")
        
        msg_type = payload[0]
        return msg_type, complete_message

    def _handle_batch_message(self, client_sock, complete_message):
        """Handle batch message processing"""
        batch_request = parse_batch_request(complete_message)
        
        utils_bets = []
        for bet_request in batch_request.apuestas:
            year = bet_request.nacimiento // 10000
            month = (bet_request.nacimiento // 100) % 100  
            day = bet_request.nacimiento % 100
            
            utils_bet = UtilsBet(
                agency=str(batch_request.agencia_id), 
                first_name=bet_request.nombre,
                last_name=bet_request.apellido,
                document=str(bet_request.documento),
                birthdate=f"{year:04d}-{month:02d}-{day:02d}",
                number=str(bet_request.numero)
            )
            utils_bets.append(utils_bet)
        
        store_bets(utils_bets)
        logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(batch_request.apuestas)}')
        
        if batch_request.is_last_batch:
            self._handle_last_batch(batch_request.agencia_id)
        
        send_bet_response(client_sock, STATUS_OK, f"Batch de {len(batch_request.apuestas)} apuestas registrado exitosamente")

    def _handle_last_batch(self, agencia_id):
        """Handle when an agency finishes sending all batches"""
        with self._lock:
            self._finished_agencies.add(agencia_id)
            logging.info(f'action: agency_finished | result: success | agency_id: {agencia_id} | finished_count: {len(self._finished_agencies)}')
            
            if len(self._finished_agencies) == self._expected_agencies and not self._sorteo_realizado:
                self._perform_lottery_draw()

    def _handle_winners_query(self, client_sock, complete_message):
        """Handle winners query processing"""
        query_request = parse_query_winners_request(complete_message)
        
        with self._lock:
            if not self._sorteo_realizado:
                send_winners_response(client_sock, STATUS_OK, [])
                logging.info(f'action: respuesta_ganadores | result: success | agency_id: {query_request.agencia_id} | cant_ganadores: 0 | note: sorteo_pendiente')
            else:
                agency_winners = self._winners_by_agency.get(query_request.agencia_id, [])
                send_winners_response(client_sock, STATUS_OK, agency_winners)
                logging.info(f'action: respuesta_ganadores | result: success | agency_id: {query_request.agencia_id} | cant_ganadores: {len(agency_winners)}')

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket   
        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            while self._running:
                msg_type, complete_message = self._receive_message(client_sock)
                if msg_type is None:
                    break
                    
                if msg_type == MSG_TYPE_BATCH:
                    self._handle_batch_message(client_sock, complete_message)
                elif msg_type == MSG_TYPE_QUERY_WINNERS:
                    self._handle_winners_query(client_sock, complete_message)
                else:
                    raise ValueError(f"Unknown message type: {msg_type}")
                    
        except (ConnectionError, socket.error) as e:
            logging.error(f"action: handle_client | result: success | error: {e}") 
        except ValueError as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
            send_bet_response(client_sock, STATUS_ERROR, "Datos de apuesta inválidos")
                
        except Exception as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
            send_bet_response(client_sock, STATUS_ERROR, "Error al procesar mensaje")
        finally:
            client_sock.close()
    
    def _perform_lottery_draw(self):
        """
        Perform the lottery draw when all agencies have finished
        """
        try:
            logging.info('action: sorteo | result: in_progress')
            
            all_bets = load_bets()
            
            for bet in all_bets:
                agency_id = int(bet.agency)
                
                if agency_id not in self._winners_by_agency:
                    self._winners_by_agency[agency_id] = []
                
                if has_won(bet):
                    dni = int(bet.document)
                    self._winners_by_agency[agency_id].append(dni)
            
            self._sorteo_realizado = True
            
            total_winners = sum(len(winners) for winners in self._winners_by_agency.values())
            logging.info(f'action: sorteo | result: success | total_winners: {total_winners}')
            
            for agency_id, winners in self._winners_by_agency.items():
                logging.info(f'action: winners_calculated | result: success | agency_id: {agency_id} | winner_count: {len(winners)}')
                
        except Exception as e:
            logging.error(f'action: sorteo | result: fail | error: {e}')
            self._sorteo_realizado = True

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
        
        logging.info(f'action: shutdown_threads | result: in_progress | thread_count: {len(self._active_threads)}')
        
        for thread, client_sock in self._active_threads.items():
            if thread.is_alive():
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                    client_sock.close()
                    thread.join()
                except Exception as e:
                    logging.error(f'action: thread_shutdown | result: fail | error: {e}')

    
    def _cleanup(self):
        logging.info('action: shutdown_server | result: in_progress')

        if self._running and self._server_socket:
            try:
                self._server_socket.close()
                logging.info('action: close_server_socket | result: success')
            except Exception as e:
                logging.error(f'action: close_server_socket | result: fail | error: {e}')
        logging.info('action: shutdown_server | result: success')
