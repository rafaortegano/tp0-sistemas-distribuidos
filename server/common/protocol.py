import struct
import logging

STATUS_OK = 0x00
STATUS_ERROR = 0x01


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
    def __init__(self, agencia_id, apuestas):
        self.agencia_id = agencia_id
        self.apuestas = apuestas 
    
    def __str__(self):
        return f"Batch(agency={self.agencia_id}, bets={len(self.apuestas)})"

class BetResponse:
    def __init__(self, status, message=""):
        self.status = status
        self.message = message
    
    def serialize(self):
        """Serialize response to binary format"""
        payload = struct.pack('B', self.status)
        
        message_bytes = self.message.encode('utf-8') if self.message else b''
        payload += struct.pack('B', len(message_bytes)) + message_bytes
        
        length = len(payload)
        return struct.pack('>I', length) + payload

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
    
    msg_length = struct.unpack('>I', data[:4])[0]
    
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
    documento = struct.unpack('>I', payload[offset:offset + 4])[0]
    offset += 4
    
    if offset + 4 > len(payload):
        raise ValueError("Unexpected end of data while reading nacimiento")
    anio = struct.unpack('>H', payload[offset:offset + 2])[0]
    mes = payload[offset + 2]
    dia = payload[offset + 3]
    offset += 4
    nacimiento = anio * 10000 + mes * 100 + dia  # Reconstruct YYYYMMDD
    
   
    if offset + 2 > len(payload):
        raise ValueError("Unexpected end of data while reading numero")
    numero = struct.unpack('>H', payload[offset:offset + 2])[0]
    
    return BetRequest(agencia_id, nombre, apellido, documento, nacimiento, numero)

def parse_batch_request(data):
    """Parse binary data into a BatchRequest object"""
    if len(data) < 4:
        raise ValueError("Message too short for batch")
    
    msg_length = struct.unpack('>I', data[:4])[0]
    
    if len(data) != 4 + msg_length:
        raise ValueError(f"Message length mismatch: expected {4 + msg_length}, got {len(data)}")
    
    payload = data[4:]
    offset = 0
    

    if offset + 1 > len(payload):
        raise ValueError("Unexpected end of data while reading agencia_id")
    agencia_id = payload[offset]
    offset += 1
    
    if offset + 2 > len(payload):
        raise ValueError("Unexpected end of data while reading bet count")
    bet_count = struct.unpack('>H', payload[offset:offset + 2])[0]
    offset += 2
    
    apuestas = []
    

    for i in range(bet_count):
        
        nombre, offset = read_string(payload, offset)
       
        apellido, offset = read_string(payload, offset)
        
        if offset + 4 > len(payload):
            raise ValueError(f"Unexpected end of data while reading documento for bet {i}")
        documento = struct.unpack('>I', payload[offset:offset + 4])[0]
        offset += 4
        
        if offset + 4 > len(payload):
            raise ValueError(f"Unexpected end of data while reading nacimiento for bet {i}")
        anio = struct.unpack('>H', payload[offset:offset + 2])[0]
        mes = payload[offset + 2]
        dia = payload[offset + 3]
        offset += 4
        nacimiento = anio * 10000 + mes * 100 + dia
        
        if offset + 2 > len(payload):
            raise ValueError(f"Unexpected end of data while reading numero for bet {i}")
        numero = struct.unpack('>H', payload[offset:offset + 2])[0]
        offset += 2
        
        bet = BetRequest(agencia_id, nombre, apellido, documento, nacimiento, numero)
        apuestas.append(bet)
    
    return BatchRequest(agencia_id, apuestas)

def receive_message(socket):
    """Receive and parse a complete message from socket"""
    try:
       
        length_bytes = recv_exactly(socket, 4)
        length = struct.unpack('>I', length_bytes)[0]
        
        payload = recv_exactly(socket, length)
        
        complete_message = length_bytes + payload
        
        return parse_bet_request(complete_message)
            
    except struct.error as e:
        raise ValueError(f"Failed to parse message: {e}")
    except (ValueError, ConnectionError) as e:
        logging.error(f"Error while receiving message: {e}")
        raise

def receive_batch_message(socket):
    """Receive and parse a batch message from socket"""
    try:
        length_bytes = recv_exactly(socket, 4)
        length = struct.unpack('>I', length_bytes)[0]
        
        payload = recv_exactly(socket, length)
        
        complete_message = length_bytes + payload
        
        return parse_batch_request(complete_message)
            
    except struct.error as e:
        raise ValueError(f"Failed to parse batch message: {e}")
    except (ValueError, ConnectionError) as e:
        logging.error(f"Error while receiving batch message: {e}")
        raise

def send_bet_response(socket, status, message=""):
    """Send bet response to client"""
    response = BetResponse(status, message)
    data = response.serialize()
    send_all(socket, data)
