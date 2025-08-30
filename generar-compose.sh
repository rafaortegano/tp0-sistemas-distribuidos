#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Número de argumentos inválido. Se esperan 2: archivo de salida y cantidad de clientes"
  exit 1
fi

echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes solicitados: $2"
python3 scripts/script_ej1.py $1 $2
