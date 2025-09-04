import logging

STATUS_OK = 0x00
STATUS_ERROR = 0x01


MSG_TYPE_BATCH = 0x01
MSG_TYPE_QUERY_WINNERS = 0x02

def uint32_to_bytes_be(value):
    """Convert uint32 to 4 bytes big-endian"""
    return bytes([
        (value >> 24) & 0xFF,
        (value >> 16) & 0xFF,
        (value >> 8) & 0xFF,
        value & 0xFF
    ])

def uint16_to_bytes_be(value):
    """Convert uint16 to 2 bytes big-endian"""
    return bytes([
        (value >> 8) & 0xFF,
        value & 0xFF
    ])

def bytes_to_uint32_be(data):
    """Convert 4 bytes big-endian to uint32"""
    return (data[0] << 24) | (data[1] << 16) | (data[2] << 8) | data[3]

def bytes_to_uint16_be(data):
    """Convert 2 bytes big-endian to uint16"""
    return (data[0] << 8) | data[1]


class BetRequest:
    def __init__(self, agencia_id, nombre, apellido, documento, nacimiento, numero):
        self.agencia_id = agencia_id
        self.nombre = nombre
        self.apellido = apellido
        self.documento = documento
        self.nacimiento = nacimiento
        self.numero = numero
    
    def __str__(self):
        return f"Bet({self.nombre} {self.apellido}, DOC:{self.documento}, NUM:{self.numero})"

class BatchRequest:
    def __init__(self, agencia_id, apuestas, is_last_batch=False):
        self.agencia_id = agencia_id
        self.apuestas = apuestas 
        self.is_last_batch = is_last_batch
    
    def __str__(self):
        return f"Batch(agency={self.agencia_id}, bets={len(self.apuestas)}, last={self.is_last_batch})"

class BetResponse:
    def __init__(self, status, message=""):
        self.status = status
        self.message = message
    
    def serialize(self):
        """Serialize response to binary format"""
        payload = bytearray()
        payload.append(self.status)
        
        message_bytes = self.message.encode('utf-8') if self.message else b''
        payload.append(len(message_bytes))
        payload.extend(message_bytes)
        
        length = len(payload)
        return uint32_to_bytes_be(length) + bytes(payload)

class QueryWinnersRequest:
    def __init__(self, agencia_id):
        self.agencia_id = agencia_id
    
    def __str__(self):
        return f"QueryWinners(agency={self.agencia_id})"

class WinnersResponse:
    def __init__(self, status, winner_dnis=None):
        self.status = status
        self.winner_dnis = winner_dnis or []
    
    def serialize(self):
        """Serialize winners response to binary format"""
        payload = bytearray()
        payload.append(self.status)
        
        winner_count = len(self.winner_dnis)
        payload.extend(uint16_to_bytes_be(winner_count))
        
        for dni in self.winner_dnis:
            payload.extend(uint32_to_bytes_be(dni))
        
        length = len(payload)
        return uint32_to_bytes_be(length) + bytes(payload)
    
    def __str__(self):
        return f"WinnersResponse(status={self.status}, winners={len(self.winner_dnis)})"

def recv_exactly(socket, length):
    """Receive exactly 'length' bytes from socket"""
    data = b''
    while len(data) < length:
        chunk = socket.recv(length - len(data))
        if not chunk:
            raise ConnectionError("Socket connection broken")
        data += chunk
    return data

def send_all(socket, data):
    """Send all data to socket"""
    total_sent = 0
    while total_sent < len(data):
        sent = socket.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection broken")
        total_sent += sent

def read_string(data, offset):
    """Read length-prefixed string from data at offset (1 byte length)"""
    if offset >= len(data):
        raise ValueError("Unexpected end of data while reading string length")
    
    length = data[offset]
    if offset + 1 + length > len(data):
        raise ValueError("Unexpected end of data while reading string")
    
    string_data = data[offset + 1:offset + 1 + length]
    return string_data.decode('utf-8'), offset + 1 + length

def parse_bet_request(data):
    """Parse binary data into BetRequest object"""
    if len(data) < 4:
        raise ValueError("Message too short")
    
    msg_length = bytes_to_uint32_be(data[:4])
    
    if len(data) != 4 + msg_length:
        raise ValueError(f"Message length mismatch: expected {4 + msg_length}, got {len(data)}")
    
 
    payload = data[4:]
    offset = 0
    
    if offset + 1 > len(payload):
        raise ValueError("Unexpected end of data while reading agencia_id")
    agencia_id = payload[offset]
    offset += 1
    
    nombre, offset = read_string(payload, offset)
    
    apellido, offset = read_string(payload, offset)
    
    if offset + 4 > len(payload):
        raise ValueError("Unexpected end of data while reading documento")
    documento = bytes_to_uint32_be(payload[offset:offset + 4])
    offset += 4
    
    if offset + 4 > len(payload):
        raise ValueError("Unexpected end of data while reading nacimiento")
    anio = bytes_to_uint16_be(payload[offset:offset + 2])
    mes = payload[offset + 2]
    dia = payload[offset + 3]
    offset += 4
    nacimiento = anio * 10000 + mes * 100 + dia  # Reconstruct YYYYMMDD
    
   
    if offset + 2 > len(payload):
        raise ValueError("Unexpected end of data while reading numero")
    numero = bytes_to_uint16_be(payload[offset:offset + 2])
    
    return BetRequest(agencia_id, nombre, apellido, documento, nacimiento, numero)

def parse_batch_request(data):
    """Parse binary data into a BatchRequest object"""
    if len(data) < 4:
        raise ValueError("Message too short for batch")
    
    msg_length = bytes_to_uint32_be(data[:4])
    
    if len(data) != 4 + msg_length:
        raise ValueError(f"Message length mismatch: expected {4 + msg_length}, got {len(data)}")
    
    payload = data[4:]
    offset = 0
    
    if offset + 1 > len(payload):
        raise ValueError("Unexpected end of data while reading message type")
    msg_type = payload[offset]
    if msg_type != MSG_TYPE_BATCH:
        raise ValueError(f"Expected batch message type, got {msg_type}")
    offset += 1

    if offset + 1 > len(payload):
        raise ValueError("Unexpected end of data while reading agencia_id")
    agencia_id = payload[offset]
    offset += 1
    
    if offset + 1 > len(payload):
        raise ValueError("Unexpected end of data while reading is_last_batch")
    is_last_batch = payload[offset] == 1
    offset += 1
    
    if offset + 2 > len(payload):
        raise ValueError("Unexpected end of data while reading bet count")
    bet_count = bytes_to_uint16_be(payload[offset:offset + 2])
    offset += 2
    
    apuestas = []
    

    for i in range(bet_count):
        
        nombre, offset = read_string(payload, offset)
       
        apellido, offset = read_string(payload, offset)
        
        if offset + 4 > len(payload):
            raise ValueError(f"Unexpected end of data while reading documento for bet {i}")
        documento = bytes_to_uint32_be(payload[offset:offset + 4])
        offset += 4
        
        if offset + 4 > len(payload):
            raise ValueError(f"Unexpected end of data while reading nacimiento for bet {i}")
        anio = bytes_to_uint16_be(payload[offset:offset + 2])
        mes = payload[offset + 2]
        dia = payload[offset + 3]
        offset += 4
        nacimiento = anio * 10000 + mes * 100 + dia
        
        if offset + 2 > len(payload):
            raise ValueError(f"Unexpected end of data while reading numero for bet {i}")
        numero = bytes_to_uint16_be(payload[offset:offset + 2])
        offset += 2
        
        bet = BetRequest(agencia_id, nombre, apellido, documento, nacimiento, numero)
        apuestas.append(bet)
    
    return BatchRequest(agencia_id, apuestas, is_last_batch)

def receive_message(socket):
    """Receive and parse a complete message from socket"""
    try:
       
        length_bytes = recv_exactly(socket, 4)
        length = bytes_to_uint32_be(length_bytes)
        
        payload = recv_exactly(socket, length)
        
        complete_message = length_bytes + payload
        
        return parse_bet_request(complete_message)
            
    except (ValueError, ConnectionError) as e:
        logging.error(f"Error while receiving message: {e}")
        raise

def receive_batch_message(socket):
    """Receive and parse a batch message from socket"""
    try:
        length_bytes = recv_exactly(socket, 4)
        length = bytes_to_uint32_be(length_bytes)
        
        payload = recv_exactly(socket, length)
        
        complete_message = length_bytes + payload
        
        return parse_batch_request(complete_message)
            
    except (ValueError, ConnectionError) as e:
        logging.error(f"Error while receiving batch message: {e}")
        raise

def send_bet_response(socket, status, message=""):
    """Send bet response to client"""
    response = BetResponse(status, message)
    data = response.serialize()
    send_all(socket, data)

def parse_query_winners_request(data):
    """Parse binary data into a QueryWinnersRequest object"""
    if len(data) < 4:
        raise ValueError("Message too short for query winners request")
    
    msg_length = bytes_to_uint32_be(data[:4])
    
    if len(data) != 4 + msg_length:
        raise ValueError(f"Message length mismatch: expected {4 + msg_length}, got {len(data)}")
    
    payload = data[4:]
    offset = 0
    
    if offset + 1 > len(payload):
        raise ValueError("Unexpected end of data while reading message type")
    msg_type = payload[offset]
    if msg_type != MSG_TYPE_QUERY_WINNERS:
        raise ValueError(f"Expected query winners message type, got {msg_type}")
    offset += 1
    
    if offset + 1 > len(payload):
        raise ValueError("Unexpected end of data while reading agencia_id")
    
    agencia_id = payload[offset]
    
    return QueryWinnersRequest(agencia_id)

def receive_query_winners_message(socket):
    """Receive and parse a query winners message from socket"""
    try:
        length_bytes = recv_exactly(socket, 4)
        length = bytes_to_uint32_be(length_bytes)
        
        payload = recv_exactly(socket, length)
        
        complete_message = length_bytes + payload
        
        return parse_query_winners_request(complete_message)
            
    except (ValueError, ConnectionError) as e:
        logging.error(f"Error while receiving query winners message: {e}")
        raise

def send_winners_response(socket, status, winner_dnis=None):
    """Send winners response to client"""
    response = WinnersResponse(status, winner_dnis)
    data = response.serialize()
    send_all(socket, data)
