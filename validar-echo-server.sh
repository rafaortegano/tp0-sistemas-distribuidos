#!/bin/bash

MESSAGE="Vamos River"
SERVICE="server"   
PORT=12345         
NETWORK="tp0_testing_net" 
MAX_SECONDS_TO_WAIT=3


RESPONSE=$(docker run --rm --network $NETWORK alpine sh -c \
  "apk add --no-cache netcat-openbsd >/dev/null && echo $MESSAGE | nc -w $MAX_SECONDS_TO_WAIT $SERVICE $PORT")


if [ "$RESPONSE" = "$MESSAGE" ]; then
  echo "action: test_echo_server | result: success"
  exit 0
  
else
  echo "action: test_echo_server | result: fail"
  exit 1
fi