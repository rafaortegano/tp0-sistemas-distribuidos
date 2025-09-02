import socket
import logging
import signal
import os
from common.protocol import (
    receive_message, receive_batch_message, send_bet_response, 
    receive_query_winners_message, send_winners_response,
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
                        self.__handle_client_connection(client_sock)
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
            length_bytes = recv_exactly(client_sock, 4)
            length = bytes_to_uint32_be(length_bytes)
            payload = recv_exactly(client_sock, length)
            complete_message = length_bytes + payload
            
            if len(payload) < 1:
                raise ValueError("Empty message payload")
            
            msg_type = payload[0]
            
            if msg_type == MSG_TYPE_BATCH:
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
                    self._finished_agencies.add(batch_request.agencia_id)
                    logging.info(f'action: agency_finished | result: success | agency_id: {batch_request.agencia_id} | finished_count: {len(self._finished_agencies)}')
                    
                    if len(self._finished_agencies) == self._expected_agencies and not self._sorteo_realizado:
                        self._perform_lottery_draw()
                
                send_bet_response(client_sock, STATUS_OK, f"Batch de {len(batch_request.apuestas)} apuestas registrado exitosamente")
                
            elif msg_type == MSG_TYPE_QUERY_WINNERS:
                query_request = parse_query_winners_request(complete_message)
                
                if not self._sorteo_realizado:
                    send_winners_response(client_sock, STATUS_OK, [])
                    logging.info(f'action: consulta_ganadores | result: success | agency_id: {query_request.agencia_id} | cant_ganadores: 0 | note: sorteo_pendiente')
                else:
                    agency_winners = self._winners_by_agency.get(query_request.agencia_id, [])
                    send_winners_response(client_sock, STATUS_OK, agency_winners)
                    logging.info(f'action: consulta_ganadores | result: success | agency_id: {query_request.agencia_id} | cant_ganadores: {len(agency_winners)}')
            else:
                raise ValueError(f"Unknown message type: {msg_type}")
                
        except OSError as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
            try:
                send_bet_response(client_sock, STATUS_ERROR, "Error al procesar mensaje")
            except:
                pass
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
    
    def _cleanup(self):
        """Cleans up resources during shutdown"""
        logging.info('action: shutdown_server | result: in_progress')
        if hasattr(self, '_server_socket') and self._server_socket:
            try:
                self._server_socket.close()
                logging.info('action: close_server_socket | result: success')
            except:
                pass
        logging.info('action: shutdown_server | result: success')
