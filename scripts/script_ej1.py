import sys 

if len(sys.argv) != 3:
    print("Número de argumentos inválido. Se esperan 2: archivo de salida y cantidad de cliente")
    sys.exit(1)

output_file = sys.argv[1]
clients_number = int(sys.argv[2])

with open(output_file, 'w') as f:
    f.write("""name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini
""")

    for i in range(1, clients_number + 1):
        f.write(f"""
  client{i}:
    container_name: client{i}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID={i}
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./client/config.yaml:/config.yaml
""")

    f.write("""
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
""")

